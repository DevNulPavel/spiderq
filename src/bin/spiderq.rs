use std::{
    io::{
        self,
        Write,
    },
    env,
    process,
    convert::From,
    thread::{
        Builder,
        JoinHandle,
    },
    sync::mpsc::{
        channel,
        Sender,
        Receiver,
        TryRecvError,
    },
    num::ParseIntError,
};

use time::{
    Duration,
    SteadyTime,
};

use getopts::Options;

use simple_signal::Signal;

use spiderq::{db, pq};

use spiderq_proto::{
    Key,
    Value,
    LendMode,
    AddMode,
    ProtoError,
    GlobalReq,
    GlobalRep,
};

const MAX_POLL_TIMEOUT: i64 = 100;

#[derive(Debug)]
pub enum Error {
    Getopts(getopts::Fail),
    Db(db::Error),
    Zmq(ZmqError),
    InvalidFlushLimit(ParseIntError),
}

#[derive(Debug)]
pub enum ZmqError {
    Message(zmq::Error),
    Socket(zmq::Error),
    Connect(zmq::Error),
    Bind(zmq::Error),
    Recv(zmq::Error),
    Send(zmq::Error),
    Poll(zmq::Error),
    GetSockOpt(zmq::Error),
}

impl From<db::Error> for Error {
    fn from(err: db::Error) -> Error {
        Error::Db(err)
    }
}

/// Непосредственно запуск нашего приложения с параметрами
pub fn bootstrap(maybe_matches: getopts::Result) -> Result<(zmq::Context, JoinHandle<()>), Error> {
    // Разворачиваем параметры приложения
    let matches = maybe_matches.map_err(Error::Getopts)?;

    // Получаем путь к базе данных
    let database_dir = matches.opt_str("database").unwrap_or_else(|| "./spiderq".to_owned());
    // Получаем адрес ZeroMQ сокета
    let zmq_addr = matches.opt_str("zmq-addr").unwrap_or_else(|| "ipc://./spiderq.ipc".to_owned());
    // Получаем лимит элементов в очереди, при которых сбрасываем данные на диск
    let flush_limit: usize = matches.opt_str("flush-limit").unwrap_or_else(|| "131072".to_owned()).parse().map_err(Error::InvalidFlushLimit)?;
    
    // Фиксим путь к ZeroMQ очереди
    let zmq_addr_cloned = zmq_addr.replace("//*:", "//127.0.0.1:");

    // Назначаем обработчик различных системных прерываний
    simple_signal::set_handler(&[Signal::Hup, Signal::Int, Signal::Quit, Signal::Abrt, Signal::Term], move |signals| {
        // Создаем контекст ZMQ
        let zmq_ctx = zmq::Context::new();

        // Создаем сокет типа REQ для отправки в очередь команды
        let sock = zmq_ctx.socket(zmq::REQ).map_err(ZmqError::Socket).unwrap();
        
        // Коннектимся по адресу текущего процесса
        sock.connect(&zmq_addr_cloned).map_err(ZmqError::Connect).unwrap();

        println!(" ;; {:?} received, terminating server...", signals);

        {
            // Создаем ProtoBuf сообщение с запросом сброса всего на диск
            let packet = GlobalReq::Flush;
            let required = packet.encode_len();
            let mut msg = zmq::Message::with_capacity(required)
                .map_err(ZmqError::Message)
                .unwrap();
            packet.encode(&mut msg);

            // Отправляем в ZMQ сообщение завершения работы
            sock.send_msg(msg, 0).unwrap();

            // Затем ждем ответный статус
            let reply_msg = sock.recv_msg(0).map_err(ZmqError::Recv).unwrap();
            let (rep, _) = GlobalRep::decode(&reply_msg).unwrap();

            // Проверяем результат
            match rep {
                GlobalRep::Flushed => (),
                other => panic!("unexpected reply for flush: {:?}", other),
            }
        }

        {
            // Создаем ProtoBuf сообщение с запросом завершения работы
            let packet = GlobalReq::Terminate;
            let required = packet.encode_len();
            let mut msg = zmq::Message::with_capacity(required)
                .map_err(ZmqError::Message)
                .unwrap();
            packet.encode(&mut msg);

            // Отправляем в ZMQ сообщение завершения работы
            sock.send_msg(msg, 0).unwrap();

            // Затем ждем ответный статус
            let reply_msg = sock.recv_msg(0).map_err(ZmqError::Recv).unwrap();
            let (rep, _) = GlobalRep::decode(&reply_msg).unwrap();

            // Если успешно все завершилось - тогда все ок, если нет - паникуем
            match rep {
                GlobalRep::Terminated => (),
                other => panic!("unexpected reply for terminate: {:?}", other),
            }
        }
    });

    // Стартуем наш сервис с параметрами
    entrypoint(&zmq_addr, &database_dir, flush_limit)
}

/// Стартуем наш сервис с параметрами
pub fn entrypoint(zmq_addr: &str, database_dir: &str, flush_limit: usize) -> Result<(zmq::Context, JoinHandle<()>), Error> {
    // Открываем нашу базу данных по пути из параметров с лимитом на максимальное количество элементов перед сбросом на диск
    let db = db::Database::new(database_dir, flush_limit).map_err(Error::Db)?;

    // Создаем priority-очередь
    let mut pq = pq::PQueue::new();

    // Из базы данных заполняем данные снова в очередь
    for (k, _) in db.iter() {
        pq.add(k.clone(), AddMode::Tail)
    }

    // Создаем контекст для работы с ZeroMQ
    let ctx = zmq::Context::new();
    // Создаем сокет типа роутер
    let sock_master_ext = ctx.socket(zmq::ROUTER).map_err(ZmqError::Socket).map_err(Error::Zmq)?;
    // Сокеты для получения данных для базы данных + очереди
    let sock_master_db_rx = ctx.socket(zmq::PULL).map_err(ZmqError::Socket).map_err(Error::Zmq)?;
    let sock_master_pq_rx = ctx.socket(zmq::PULL).map_err(ZmqError::Socket).map_err(Error::Zmq)?;
    // Сокеты для отправки в базу + в очередь с приоритетами
    let sock_db_master_tx = ctx.socket(zmq::PUSH).map_err(ZmqError::Socket).map_err(Error::Zmq)?;
    let sock_pq_master_tx = ctx.socket(zmq::PUSH).map_err(ZmqError::Socket).map_err(Error::Zmq)?;

    // Роутеру назначаем переданный адрес
    sock_master_ext.bind(zmq_addr).map_err(ZmqError::Bind).map_err(Error::Zmq)?;
    // Всем остальным назначаем сокеты ВНУТРИпроцессного взаимодействия, работает вроде бы только на UNIX?
    sock_master_db_rx.bind("inproc://db_rxtx").map_err(ZmqError::Bind).map_err(Error::Zmq)?;
    sock_db_master_tx.connect("inproc://db_rxtx").map_err(ZmqError::Connect).map_err(Error::Zmq)?;
    sock_master_pq_rx.bind("inproc://pq_rxtx").map_err(ZmqError::Bind).map_err(Error::Zmq)?;
    sock_pq_master_tx.connect("inproc://pq_rxtx").map_err(ZmqError::Connect).map_err(Error::Zmq)?;

    // Создаем небуферизированные Rust-каналы для взаимодействия
    // TODO: может быть сделать каналы ограниченного размера
    let (chan_master_db_tx, chan_db_master_rx) = channel();
    let (chan_db_master_tx, chan_master_db_rx) = channel();
    let (chan_master_pq_tx, chan_pq_master_rx) = channel();
    let (chan_pq_master_tx, chan_master_pq_rx) = channel();

    // Создаем поток для работы в базой данных
    Builder::new().name("worker_db".to_owned())
        .spawn(move || {
            worker_db(sock_db_master_tx, chan_db_master_tx, chan_db_master_rx, db).unwrap()
        }).unwrap();

    // Создаем поток для работы с очередью
    Builder::new().name("worker_pq".to_owned())
        .spawn(move || {
            worker_pq(sock_pq_master_tx, chan_pq_master_tx, chan_pq_master_rx, pq).unwrap()
        }).unwrap();

    // Создаем мастер-поток контроля работы
    let master_thread = Builder::new().name("master".to_owned())
   	    .spawn(move || {
            master(sock_master_ext,
                   sock_master_db_rx,
                   sock_master_pq_rx,
                   chan_master_db_tx,
                   chan_master_db_rx,
                   chan_master_pq_tx,
                   chan_master_pq_rx).unwrap() 
        })
        .unwrap();
    Ok((ctx, master_thread))
}

#[derive(Debug, PartialEq)]
pub enum DbLocalReq {
    LoadLent(u64, Key, u64, SteadyTime, LendMode),
    RepayUpdate(Key, Value),
    Stop,
}

/// Запросы к очереди
#[derive(Debug, PartialEq)]
pub enum PqLocalReq {
    NextTrigger,
    /// Ставим в очередь новый элемент
    Enqueue(Key, AddMode),
    /// TODO: Арендуем в очереди место
    LendUntil(u64, SteadyTime, LendMode),
    RepayTimedOut,
    Heartbeat(u64, Key, SteadyTime),
    /// Удаление элемента из очереди
    Remove(Key),
    Stop,
}

#[derive(Debug, PartialEq)]
pub enum DbLocalRep {
    Added(Key, AddMode),
    Removed(Key),
    LentNotFound(Key, u64, SteadyTime, LendMode),
    Stopped,
}

#[derive(Debug, PartialEq)]
pub enum PqLocalRep {
    TriggerGot(Option<SteadyTime>),
    Lent(u64, Key, u64, SteadyTime, LendMode),
    EmptyQueueHit { timeout: u64, },
    Repaid(Key, Value),
    Stopped,
}

pub type Headers = Vec<zmq::Message>;

/// Тип запроса
#[derive(Debug, PartialEq)]
pub enum DbReq {
    /// Глобальный
    Global(GlobalReq),
    /// Локальный от самого сервиса
    Local(DbLocalReq),
}

#[derive(Debug, PartialEq)]
pub enum PqReq {
    Global(GlobalReq),
    Local(PqLocalReq),
}

#[derive(Debug, PartialEq)]
pub enum DbRep {
    Global(GlobalRep),
    Local(DbLocalRep),
}

#[derive(Debug, PartialEq)]
pub enum PqRep {
    Global(GlobalRep),
    Local(PqLocalRep),
}

/// Сообщение + заголовки с мета-информацией
pub struct Message<R> {
    /// Сервисные заголовки
    pub headers: Option<Headers>,
    /// Непосредственно сами данные
    pub load: R,
}

/// Вычитываем из сокета данные и сервисные заголовки если они там есть
fn rx_sock(sock: &mut zmq::Socket) -> Result<(Option<Headers>, Result<GlobalReq, ProtoError>), Error> {
    // Буффер
    let mut frames = Vec::new();
    loop {
        // Складываем получаемые сообщения в вектор
        frames.push(sock.recv_msg(0).map_err(ZmqError::Recv).map_err(Error::Zmq)?);
        // Если больше ничего нету - прерываем цикл
        if !sock.get_rcvmore().map_err(ZmqError::GetSockOpt).map_err(Error::Zmq)? {
            break
        }
    }

    // TODO: Но что, если у нас дальше будут прилетать сообщения, но уже не к этому запросу
    // Или это не важно из-за того, что у нас REQ/REP сокет?

    // В самом конце у нас идет непосредственно само сообщение, которое мы декодируем
    let load_msg = GlobalReq::decode(&frames.pop().unwrap()).map(|p| p.0);

    // Все остальное мы считаем заголовками
    Ok((Some(frames), load_msg))
}

/// Пишем ответное сообщение в сокет
fn tx_sock(packet: GlobalRep, maybe_headers: Option<Headers>, sock: &mut zmq::Socket) -> Result<(), Error> {
    // Размер наших данных
    let required = packet.encode_len();
    
    // Создаем сообщение нужного размера
    let mut load_msg = zmq::Message::with_capacity(required)
        .map_err(ZmqError::Message)
        .map_err(Error::Zmq)?;
    
    // Кодируем данные в нутрь сообщения
    packet.encode(&mut load_msg);

    // Если есть заголовки, то пушим их сначала
    if let Some(headers) = maybe_headers {
        for header in headers {
            // Режим zmq::SNDMORE значит, что данные будут сыпаться еще позднее и можно
            // просто их буфферизировать
            sock.send_msg(header, zmq::SNDMORE)
                .map_err(ZmqError::Send)
                .map_err(Error::Zmq)?;
        }
    }

    // В самом конце после всех заголовков пишем уже непосредственно наше сообщение
    sock.send_msg(load_msg, 0)
        .map_err(ZmqError::Send)
        .map_err(Error::Zmq)
}

/// Отправление сообщения в канал
pub fn tx_chan<R>(packet: R, maybe_headers: Option<Headers>, chan: &Sender<Message<R>>) {
    chan.send(Message { headers: maybe_headers, load: packet, }).unwrap()
}

/// Отправляем пустое сообщение в сокет
fn notify_sock(sock: &mut zmq::Socket) -> Result<(), Error> {
    let msg = zmq::Message::new()
        .map_err(ZmqError::Message)
        .map_err(Error::Zmq)?;
    sock.send_msg(msg, 0)
        .map_err(ZmqError::Send)
        .map_err(Error::Zmq)
}

/// Отправляем определенный пакет с заголовками в канал + оповещаем сокет путым пакетом
fn tx_chan_n<R>(packet: R, maybe_headers: Option<Headers>, chan: &Sender<Message<R>>, sock: &mut zmq::Socket) -> Result<(), Error> {
    tx_chan(packet, maybe_headers, chan);
    notify_sock(sock)
}

/// Функция-обработчик для работы в потоке с базой данных.
/// Своего рода - это актор по работе с базой данных.
fn worker_db(mut sock_tx: zmq::Socket,
             chan_tx: Sender<Message<DbRep>>,
             chan_rx: Receiver<Message<DbReq>>,
             mut db: db::Database) -> Result<(), Error>
{
    // Отправляем пустое сообщение в сокет
    notify_sock(&mut sock_tx)?;
    loop {
        // Прилетело какое-то сообщение в канал?
        let req = chan_rx.recv().unwrap();
        
        // Смотрим что за сообщение
        match req.load {
            // Добавляем в базу что-то новое
            DbReq::Global(GlobalReq::Add { key: k, value: v, mode: m, }) => {
                // Проверяем - есть ли что-то уже в базе данных с таким ключем?
                if db.lookup(&k).is_some() {
                    // Если есть, тогда пишем в ответный канал сообщение + оповещаем сокет о событии
                    tx_chan_n(DbRep::Global(GlobalRep::Kept), req.headers, &chan_tx, &mut sock_tx)?
                } else {
                    // Если не было, тогда пишем в базу 
                    db.insert(k.clone(), v);
                    // Отправляем в ответ сообщение про успешную запись в базу
                    tx_chan_n(DbRep::Local(DbLocalRep::Added(k, m)), req.headers, &chan_tx, &mut sock_tx)?
                }
            },
            // Обновляем значение в базе
            DbReq::Global(GlobalReq::Update(key, value)) => {
                // Ищем значение в базе данных
                if db.lookup(&key).is_some() {
                    // Если нашли, то просто добавляем новое под тем же ключем
                    db.insert(key.clone(), value);
                    // Пишем сообщение об обновлении в канал + дергаем сокет пустым сообщением
                    tx_chan_n(DbRep::Global(GlobalRep::Updated), req.headers, &chan_tx, &mut sock_tx)?
                } else {
                    // Такого поля нет - пишем в канал + сокет пустым сообещнием
                    tx_chan_n(DbRep::Global(GlobalRep::NotFound), req.headers, &chan_tx, &mut sock_tx)?
                }
            },
            // Ищем значение по ключу в базе
            DbReq::Global(GlobalReq::Lookup(key)) => {
                if let Some(value) = db.lookup(&key) {
                    // Нашли - отправляем значение в ответ + оповещаем сокет
                    tx_chan_n(DbRep::Global(GlobalRep::ValueFound(value.clone())), req.headers, &chan_tx, &mut sock_tx)?
                } else {
                    // Не нашли
                    tx_chan_n(DbRep::Global(GlobalRep::ValueNotFound), req.headers, &chan_tx, &mut sock_tx)?
                }
            },
            // Удаление значения из базы
            DbReq::Global(GlobalReq::Remove(key)) => {
                if db.lookup(&key).is_some() {
                    // Удаляем и оповещаем
                    db.remove(key.clone());
                    tx_chan_n(DbRep::Local(DbLocalRep::Removed(key)), req.headers, &chan_tx, &mut sock_tx)?
                } else {
                    // Оповещяем, что не нашли
                    tx_chan_n(DbRep::Global(GlobalRep::NotRemoved), req.headers, &chan_tx, &mut sock_tx)?
                }
            },
            // Прилетала команда на сброс данных
            DbReq::Global(GlobalReq::Flush) => {
                // Запрашиваем сброс на диск и обновление структуры снапшотов
                db.flush();
                // Оповещаем, что данные были успешно сброшены на диск
                tx_chan_n(DbRep::Global(GlobalRep::Flushed), req.headers, &chan_tx, &mut sock_tx)?
            },
            // Все остальные события не должны прилетать снаружи
            // TODO: Может быть не нужно паниковать и падать, а просто написать ошибку в логи?
            DbReq::Global(..) => 
                unreachable!(),
            // Локальное какое-то событие, которое прилетело нам
            DbReq::Local(DbLocalReq::LoadLent(lend_key, key, timeout, trigger_at, mode)) => {
                // Ищем определенное значение
                if let Some(value) = db.lookup(&key) {
                    // Отправляем уже глобальное событие 
                    tx_chan_n(DbRep::Global(GlobalRep::Lent { lend_key, key, value: value.clone(), }),
                              req.headers, &chan_tx, &mut sock_tx)?
                } else {
                    // Такого ключа нету
                    tx_chan_n(DbRep::Local(DbLocalRep::LentNotFound(key, timeout, trigger_at, mode)), req.headers, &chan_tx, &mut sock_tx)?
                }
            },
            // TODO: Обновление значения по ключу?
            DbReq::Local(DbLocalReq::RepayUpdate(key, value)) => {
                db.insert(key, value);
                tx_chan_n(DbRep::Global(GlobalRep::Repaid), req.headers, &chan_tx, &mut sock_tx)?
            },
            // Останавливаем работу текущего актора по работе с базой данных
            DbReq::Local(DbLocalReq::Stop) => {
                tx_chan_n(DbRep::Local(DbLocalRep::Stopped), req.headers, &chan_tx, &mut sock_tx)?;
                return Ok(())
            },
        }
    }
}

/// Актор по работе с очередью приоритетов
fn worker_pq(mut sock_tx: zmq::Socket,
             chan_tx: Sender<Message<PqRep>>,
             chan_rx: Receiver<Message<PqReq>>,
             mut pq: pq::PQueue) -> Result<(), Error>
{
    // Оповещаем сокет о получении?
    notify_sock(&mut sock_tx)?;
    loop {
        // Получаем команду по работе с очередью для актора
        let req = chan_rx.recv().unwrap();
        match req.load {
            // Получаем внешний запрос с информацией о количестве элементов в очереди
            PqReq::Global(GlobalReq::Count) => {
                // Кидаем в ответ сообщение с количеством элементов в очереди
                tx_chan_n(PqRep::Global(GlobalRep::Counted(pq.len())), req.headers, &chan_tx, &mut sock_tx)?;
            },
            // TODO: ???
            // Пишем в очередь значение, которое одолжили до этого с получением номера в очереди
            PqReq::Global(GlobalReq::Repay { lend_key: rlend_key, key: rkey, value: rvalue, status: rstatus, }) =>
                if pq.repay(rlend_key, rkey.clone(), rstatus) {
                    tx_chan_n(PqRep::Local(PqLocalRep::Repaid(rkey, rvalue)), req.headers, &chan_tx, &mut sock_tx)?
                } else {
                    tx_chan_n(PqRep::Global(GlobalRep::NotFound), req.headers, &chan_tx, &mut sock_tx)?
                },
            PqReq::Global(..) =>
                unreachable!(),
            // Добавляем в очередь новый элемент
            PqReq::Local(PqLocalReq::Enqueue(key, mode)) =>
                pq.add(key, mode),
            // Удаляем элемент из очереди
            PqReq::Local(PqLocalReq::Remove(key)) =>
                pq.remove(key),
            // Арендуем место в очереди на определенное время
            PqReq::Local(PqLocalReq::LendUntil(timeout, trigger_at, mode)) => {
                // Получаем текущее значение на верхушке очереди
                if let Some((key, serial)) = pq.top() {
                    // Отправляем статус, что мы арендовали значение
                    tx_chan_n(PqRep::Local(PqLocalRep::Lent(serial, key, timeout, trigger_at, mode)), req.headers, &chan_tx, &mut sock_tx)?;
                    // Извлекаем значение, которое получили при вызове pop
                    pq.lend(trigger_at);
                } else {
                    // Если у нас в очереди нет никакого значения, догда выдаем сообщение в ответ
                    match mode {
                        LendMode::Block =>
                            tx_chan_n(PqRep::Local(PqLocalRep::EmptyQueueHit { timeout }), req.headers, &chan_tx, &mut sock_tx)?,
                        LendMode::Poll =>
                            tx_chan_n(PqRep::Global(GlobalRep::QueueEmpty), req.headers, &chan_tx, &mut sock_tx)?,
                    }
                }
            },
            // Обновление времени аренды
            PqReq::Local(PqLocalReq::Heartbeat(lend_key, ref key, trigger_at)) => {
                if pq.heartbeat(lend_key, key, trigger_at) {
                    tx_chan_n(PqRep::Global(GlobalRep::Heartbeaten), req.headers, &chan_tx, &mut sock_tx)?
                } else {
                    tx_chan_n(PqRep::Global(GlobalRep::Skipped), req.headers, &chan_tx, &mut sock_tx)?
                }
            },
            // TODO: ???
            PqReq::Local(PqLocalReq::NextTrigger) =>
                tx_chan_n(PqRep::Local(PqLocalRep::TriggerGot(pq.next_timeout())), req.headers, &chan_tx, &mut sock_tx)?,
            PqReq::Local(PqLocalReq::RepayTimedOut) =>
                pq.repay_timed_out(),
            // Завершаем работу воркера работы с запросами
            PqReq::Local(PqLocalReq::Stop) => {
                tx_chan_n(PqRep::Local(PqLocalRep::Stopped), req.headers, &chan_tx, &mut sock_tx)?;
                return Ok(())
            },
        }
    }
}

/// Актор по координации работы всей системы
fn master(mut sock_ext: zmq::Socket,
          sock_db_rx: zmq::Socket,
          sock_pq_rx: zmq::Socket,
          chan_db_tx: Sender<Message<DbReq>>,
          chan_db_rx: Receiver<Message<DbRep>>,
          chan_pq_tx: Sender<Message<PqReq>>,
          chan_pq_rx: Receiver<Message<PqRep>>) -> Result<(), Error>
{
    // Состояние ожидания завершения работы приложения
    enum StopState {
        NotTriggered,
        WaitingPqAndDb(Option<Headers>),
        WaitingPq(Option<Headers>),
        WaitingDb(Option<Headers>),
        Finished(Option<Headers>),
    }

    let mut stats_ping = 0;
    let mut stats_count = 0;
    let mut stats_add = 0;
    let mut stats_update = 0;
    let mut stats_lookup = 0;
    let mut stats_remove = 0;
    let mut stats_lend = 0;
    let mut stats_repay = 0;
    let mut stats_heartbeat = 0;
    let mut stats_stats = 0;

    let (mut incoming_queue, mut pending_queue) = (Vec::new(), Vec::new());
    let mut next_timeout: Option<Option<SteadyTime>> = None;
    let mut stop_state = StopState::NotTriggered;

    // Дожидаемся старта каждого из воркеров с помощью 
    // получения сообщений оттуда с помощью сокета
    let _ = sock_db_rx.recv_msg(0).map_err(ZmqError::Recv).map_err(Error::Zmq)?;
    let _ = sock_pq_rx.recv_msg(0).map_err(ZmqError::Recv).map_err(Error::Zmq)?;

    loop {
        // Если мы в состоянии полного завершения работы - можем успешно выйти из цикла
        if let StopState::Finished(headers) = stop_state {
            // Отправив при этом во внешний сокет сообщение про успешное завершение работы
            tx_sock(GlobalRep::Terminated, headers, &mut sock_ext)?;
            return Ok(())
        }

        let mut pq_changed = false;
        let before_poll_ts = SteadyTime::now();

        // TODO: Считаем задержку перед очередной итерацией?
        let timeout = match next_timeout {
            None => {
                // TODO: ???
                // Пишем сообщение в канал работы с очередью для инициализации?
                tx_chan(PqReq::Local(PqLocalReq::NextTrigger), None, &chan_pq_tx);
                MAX_POLL_TIMEOUT
            },
            Some(None) => {
                MAX_POLL_TIMEOUT
            },
            Some(Some(next_trigger)) => {
                let interval = next_trigger - before_poll_ts;
                match interval.num_milliseconds() {
                    timeout if timeout <= 0 => {
                        tx_chan(PqReq::Local(PqLocalReq::RepayTimedOut), None, &chan_pq_tx);
                        tx_chan(PqReq::Local(PqLocalReq::NextTrigger), None, &chan_pq_tx);
                        next_timeout = None;
                        pq_changed = true;
                        MAX_POLL_TIMEOUT
                    },
                    timeout if timeout > MAX_POLL_TIMEOUT =>
                        MAX_POLL_TIMEOUT,
                    timeout =>
                        timeout,
                }
            },
        };

        // Создаем массив с информацией о данных в сокетах
        // TODO: Для наглядности лучше бы структурку сделать вместо массива
        let avail_socks = {
            // Список ZeroMQ сокетов, где нужно ждать события о доступности данных на чтение
            // .as_poll_item нужен для того, чтобы вытягивать новые события только какого-то одного типа.
            let mut pollitems = [sock_ext.as_poll_item(zmq::POLLIN),
                                 sock_db_rx.as_poll_item(zmq::POLLIN),
                                 sock_pq_rx.as_poll_item(zmq::POLLIN)];
            // Опрашиваем массив полл-сокетов с определенным таймаутом
            zmq::poll(&mut pollitems, timeout)
                .map_err(ZmqError::Poll)
                .map_err(Error::Zmq)?;
            // Выдаем массив наличия событий в каждом типе событий
            [pollitems[0].get_revents() == zmq::POLLIN,
             pollitems[1].get_revents() == zmq::POLLIN,
             pollitems[2].get_revents() == zmq::POLLIN]
        };

        let after_poll_ts = SteadyTime::now();

        // Есть что-то в сокете внешнем?
        if avail_socks[0] {
            // Получаем из внешнего сокета данные раз они там готовы
            match rx_sock(&mut sock_ext) {
                // Складываем в очередь полученных данных заголовки и сам запрос
                Ok((headers, Ok(req))) => incoming_queue.push((headers, req)),
                // Если не смогли получить, тогда выдаем в ответ на тот же сокет ошибку
                Ok((headers, Err(e))) => tx_sock(GlobalRep::Error(e), headers, &mut sock_ext)?,
                Err(e) => return Err(e),
            }
        }

        // Что-то есть в сокете работы с базой данных?
        if avail_socks[1] {
            // Обычно это просто сообщение пинга, чтоб оживить мастера и проверить работоспособночть
            let _ = sock_db_rx.recv_msg(0).map_err(ZmqError::Recv).map_err(Error::Zmq)?;
        }

        // Что-то есть в сокете работы с очередью
        if avail_socks[2] {
            // Обычно это просто сообщение пинга, чтоб оживить мастера и проверить работоспособночть
            let _ = sock_pq_rx.recv_msg(0).map_err(ZmqError::Recv).map_err(Error::Zmq)?;
        }

        loop {
            // process messages from db
            match stop_state {
                StopState::Finished(..) | StopState::WaitingPq(..) => break,
                _ => (),
            }

            match chan_db_rx.try_recv() {
                Ok(message) => match message.load {
                    DbRep::Global(rep @ GlobalRep::Kept) => {
                        stats_add += 1;
                        tx_sock(rep, message.headers, &mut sock_ext)?;
                    },
                    DbRep::Global(rep @ GlobalRep::Updated) => {
                        stats_update += 1;
                        tx_sock(rep, message.headers, &mut sock_ext)?;
                    },
                    DbRep::Global(rep @ GlobalRep::NotFound) => {
                        stats_update += 1;
                        tx_sock(rep, message.headers, &mut sock_ext)?;
                    },
                    DbRep::Global(rep @ GlobalRep::ValueFound(..)) => {
                        stats_lookup += 1;
                        tx_sock(rep, message.headers, &mut sock_ext)?;
                    },
                    DbRep::Global(rep @ GlobalRep::ValueNotFound) => {
                        stats_lookup += 1;
                        tx_sock(rep, message.headers, &mut sock_ext)?;
                    },
                    DbRep::Global(rep @ GlobalRep::NotRemoved) => {
                        stats_remove += 1;
                        tx_sock(rep, message.headers, &mut sock_ext)?;
                    },
                    DbRep::Global(rep @ GlobalRep::Lent { .. }) => {
                        stats_lend += 1;
                        tx_sock(rep, message.headers, &mut sock_ext)?;
                    },
                    DbRep::Global(rep @ GlobalRep::Repaid) => {
                        stats_repay += 1;
                        tx_sock(rep, message.headers, &mut sock_ext)?;
                    },
                    DbRep::Global(rep @ GlobalRep::Flushed) =>
                        tx_sock(rep, message.headers, &mut sock_ext)?,
                    DbRep::Global(rep @ GlobalRep::Error(..)) =>
                        tx_sock(rep, message.headers, &mut sock_ext)?,
                    DbRep::Global(GlobalRep::Pong) |
                    DbRep::Global(GlobalRep::Counted(..)) |
                    DbRep::Global(GlobalRep::Added) |
                    DbRep::Global(GlobalRep::Removed) |
                    DbRep::Global(GlobalRep::QueueEmpty) |
                    DbRep::Global(GlobalRep::Heartbeaten) |
                    DbRep::Global(GlobalRep::Skipped) |
                    DbRep::Global(GlobalRep::StatsGot { .. }) |
                    DbRep::Global(GlobalRep::Terminated) =>
                        unreachable!(),
                    DbRep::Local(DbLocalRep::Added(key, mode)) => {
                        tx_chan(PqReq::Local(PqLocalReq::Enqueue(key, mode)), None, &chan_pq_tx);
                        stats_add += 1;
                        pq_changed = true;
                        tx_sock(GlobalRep::Added, message.headers, &mut sock_ext)?;
                    },
                    DbRep::Local(DbLocalRep::Removed(key)) => {
                        tx_chan(PqReq::Local(PqLocalReq::Remove(key)), None, &chan_pq_tx);
                        stats_remove += 1;
                        pq_changed = true;
                        tx_sock(GlobalRep::Removed, message.headers, &mut sock_ext)?;
                    },
                    DbRep::Local(DbLocalRep::LentNotFound(key, timeout, trigger_at, mode)) => {
                        tx_chan(PqReq::Local(PqLocalReq::Remove(key.clone())), None, &chan_pq_tx);
                        tx_chan(PqReq::Local(PqLocalReq::LendUntil(timeout, trigger_at, mode)), message.headers, &chan_pq_tx);
                    },
                    DbRep::Local(DbLocalRep::Stopped) =>
                        stop_state = match stop_state {
                            StopState::NotTriggered | StopState::WaitingPq(..) | StopState::Finished(..) => unreachable!(),
                            StopState::WaitingPqAndDb(headers) => StopState::WaitingPq(headers),
                            StopState::WaitingDb(headers) => StopState::Finished(headers),
                        },
                },
                Err(TryRecvError::Empty) =>
                    break,
                Err(TryRecvError::Disconnected) =>
                    panic!("db worker thread is down"),
            }
        }

        loop {
            // process messages from pq
            match stop_state {
                StopState::Finished(..) | StopState::WaitingDb(..) => break,
                _ => (),
            }

            match chan_pq_rx.try_recv() {
                Ok(message) => match message.load {
                    PqRep::Global(rep @ GlobalRep::Counted(..)) => {
                        stats_count += 1;
                        tx_sock(rep, message.headers, &mut sock_ext)?;
                    },
                    PqRep::Global(rep @ GlobalRep::Heartbeaten) => {
                        stats_heartbeat += 1;
                        tx_sock(rep, message.headers, &mut sock_ext)?;
                        next_timeout = None;
                        pq_changed = true;
                    },
                    PqRep::Global(rep @ GlobalRep::Skipped) => {
                        stats_heartbeat += 1;
                        tx_sock(rep, message.headers, &mut sock_ext)?;
                    },
                    PqRep::Global(rep @ GlobalRep::NotFound) => {
                        stats_repay += 1;
                        tx_sock(rep, message.headers, &mut sock_ext)?;
                    },
                    PqRep::Global(rep @ GlobalRep::QueueEmpty) => {
                        stats_lend += 1;
                        tx_sock(rep, message.headers, &mut sock_ext)?;
                    },
                    PqRep::Global(GlobalRep::Pong) |
                    PqRep::Global(GlobalRep::Added) |
                    PqRep::Global(GlobalRep::Kept) |
                    PqRep::Global(GlobalRep::Updated) |
                    PqRep::Global(GlobalRep::ValueFound(..)) |
                    PqRep::Global(GlobalRep::ValueNotFound) |
                    PqRep::Global(GlobalRep::Removed) |
                    PqRep::Global(GlobalRep::NotRemoved) |
                    PqRep::Global(GlobalRep::Lent { .. }) |
                    PqRep::Global(GlobalRep::Repaid) |
                    PqRep::Global(GlobalRep::StatsGot { .. }) |
                    PqRep::Global(GlobalRep::Flushed) |
                    PqRep::Global(GlobalRep::Terminated) |
                    PqRep::Global(GlobalRep::Error(..)) =>
                        unreachable!(),
                    PqRep::Local(PqLocalRep::Lent(lend_key, key, timeout, trigger_at, mode)) => {
                        tx_chan(DbReq::Local(DbLocalReq::LoadLent(lend_key, key, timeout, trigger_at, mode)), message.headers, &chan_db_tx);
                        next_timeout = None;
                    },
                    PqRep::Local(PqLocalRep::EmptyQueueHit { timeout: t, }) =>
                        pending_queue.push((message.headers, GlobalReq::Lend { timeout: t, mode: LendMode::Block, })),
                    PqRep::Local(PqLocalRep::Repaid(key, value)) => {
                        tx_chan(DbReq::Local(DbLocalReq::RepayUpdate(key, value)), message.headers, &chan_db_tx);
                        next_timeout = None;
                        pq_changed = true;
                    }
                    PqRep::Local(PqLocalRep::TriggerGot(trigger_at)) =>
                        next_timeout = Some(trigger_at),
                    PqRep::Local(PqLocalRep::Stopped) =>
                        stop_state = match stop_state {
                            StopState::NotTriggered | StopState::WaitingDb(..) | StopState::Finished(..) => unreachable!(),
                            StopState::WaitingPqAndDb(headers) => StopState::WaitingDb(headers),
                            StopState::WaitingPq(headers) => StopState::Finished(headers),
                        },
                },
                Err(TryRecvError::Empty) =>
                    break,
                Err(TryRecvError::Disconnected) =>
                    panic!("pq worker thread is down"),
            }
        }

        // repeat pending requests if need to
        if pq_changed {
            incoming_queue.append(&mut pending_queue);
        }

        // process incoming messages
        for (headers, global_req) in incoming_queue.drain(..) {
            match global_req {
                GlobalReq::Ping => {
                    stats_ping += 1;
                    tx_sock(GlobalRep::Pong, headers, &mut sock_ext)?
                },
                req @ GlobalReq::Count =>
                    tx_chan(PqReq::Global(req), headers, &chan_pq_tx),
                req @ GlobalReq::Add { .. } =>
                    tx_chan(DbReq::Global(req), headers, &chan_db_tx),
                req @ GlobalReq::Update(..) =>
                    tx_chan(DbReq::Global(req), headers, &chan_db_tx),
                req @ GlobalReq::Lookup(..) =>
                    tx_chan(DbReq::Global(req), headers, &chan_db_tx),
                req @ GlobalReq::Remove(..) =>
                    tx_chan(DbReq::Global(req), headers, &chan_db_tx),
                req @ GlobalReq::Repay { .. } =>
                    tx_chan(PqReq::Global(req), headers, &chan_pq_tx),
                req @ GlobalReq::Flush =>
                    tx_chan(DbReq::Global(req), headers, &chan_db_tx),
                GlobalReq::Lend { timeout: t, mode: m, } => {
                    let trigger_at = after_poll_ts + Duration::milliseconds(t as i64);
                    tx_chan(PqReq::Local(PqLocalReq::LendUntil(t, trigger_at, m)), headers, &chan_pq_tx);
                },
                GlobalReq::Heartbeat { lend_key: l, key: k, timeout: t, } => {
                    let trigger_at = after_poll_ts + Duration::milliseconds(t as i64);
                    tx_chan(PqReq::Local(PqLocalReq::Heartbeat(l, k, trigger_at)), headers, &chan_pq_tx);
                },
                GlobalReq::Stats => {
                    stats_stats += 1;
                    tx_sock(GlobalRep::StatsGot {
                        ping: stats_ping,
                        count: stats_count,
                        add: stats_add,
                        update: stats_update,
                        lookup: stats_lookup,
                        remove: stats_remove,
                        lend: stats_lend,
                        repay: stats_repay,
                        heartbeat: stats_heartbeat,
                        stats: stats_stats,
                    }, headers, &mut sock_ext)?;
                },
                GlobalReq::Terminate => {
                    tx_chan(PqReq::Local(PqLocalReq::Stop), None, &chan_pq_tx);
                    tx_chan(DbReq::Local(DbLocalReq::Stop), None, &chan_db_tx);
                    stop_state = StopState::WaitingPqAndDb(headers);
                },
            }
        }
    }
}

fn main() {
    // Аргументы приложения
    let mut args = env::args();
    // Пропускаем первый параметр, так как это имя самого приложения
    let cmd_proc = args.next().unwrap();

    // Список опций приложения
    let mut opts = Options::new();
    // Где располагаем нашу базу данных?
    opts.optopt("d", "database", "database directory path (optional, default: ./spiderq)", "");
    // Размер элементов в очереди перед сбросом на диск
    opts.optopt("l", "flush-limit", "database disk sync threshold (items modified before flush) (optional, default: 131072)", "");
    // Адрес ZeroMQ очереди на которой будем сидеть
    opts.optopt("z", "zmq-addr", "zeromq interface listen address (optional, default: ipc://./spiderq.ipc)", "");

    // Стартуем наше приложение
    match bootstrap(opts.parse(args)) {
        // Все хорошо, можно прицепиться к потоку и ждать завершения
        Ok((_, master_thread)) => {
            master_thread.join().unwrap();
        },
        // Что-то пошло не так и все сфейлилось
        Err(cause) => {
            let _ = writeln!(&mut io::stderr(), "Error: {:?}", cause);
            let usage = format!("Usage: {}", cmd_proc);
            let _ = writeln!(&mut io::stderr(), "{}", opts.usage(&usage[..]));
            process::exit(1);
        }
    }
}

#[cfg(test)]
mod test {
    use std::fs;
    use std::sync::Arc;
    use std::fmt::Debug;
    use std::thread::spawn;
    use time::{SteadyTime, Duration};
    use rand::{thread_rng, distributions::Uniform, Rng};
    use std::sync::mpsc::{channel, Sender, Receiver};
    use spiderq_proto::{Key, Value, LendMode, AddMode, RepayStatus, GlobalReq, GlobalRep};
    use super::{db, pq, worker_db, worker_pq, tx_chan, entrypoint};
    use super::{Message, DbReq, DbRep, PqReq, PqRep, DbLocalReq, DbLocalRep, PqLocalReq, PqLocalRep};

    fn with_worker<WF, MF, Req, Rep>(base_addr: &str, worker_fn: WF, master_fn: MF) where
        WF: FnOnce(zmq::Socket, Sender<Message<Rep>>, Receiver<Message<Req>>) + Send + 'static,
        MF: FnOnce(zmq::Socket, Sender<Message<Req>>, Receiver<Message<Rep>>) + Send + 'static,
        Req: Send + 'static, Rep: Send + 'static
    {
        let ctx = zmq::Context::new();
        let sock_master_slave_rx = ctx.socket(zmq::PULL).unwrap();
        let sock_slave_master_tx = ctx.socket(zmq::PUSH).unwrap();

        let rx_addr = format!("inproc://{}_rxtx", base_addr);
        sock_master_slave_rx.bind(&rx_addr).unwrap();
        sock_slave_master_tx.connect(&rx_addr).unwrap();

        let (chan_master_slave_tx, chan_slave_master_rx) = channel();
        let (chan_slave_master_tx, chan_master_slave_rx) = channel();

        let worker = spawn(move || worker_fn(sock_slave_master_tx, chan_slave_master_tx, chan_slave_master_rx));
        assert_eq!(sock_master_slave_rx.recv_bytes(0).unwrap(), &[]); // sync
        master_fn(sock_master_slave_rx, chan_master_slave_tx, chan_master_slave_rx);
        worker.join().unwrap();
    }

    fn assert_worker_cmd<Req, Rep>(sock: &mut zmq::Socket, tx: &Sender<Message<Req>>, rx: &Receiver<Message<Rep>>, req: Req, rep: Rep)
        where Rep: PartialEq + Debug
    {
        tx_chan(req, None, tx);
        assert_eq!(sock.recv_bytes(0).unwrap(), &[]);
        assert_eq!(rx.recv().unwrap().load, rep);
    }

    fn rnd_kv() -> (Key, Value) {
        let (key_len, value_len) = {
            let mut rng = thread_rng();
            (rng.gen_range(1..64), rng.gen_range(1..64))
        };

        let distr = Uniform::new(0_u8, 255_u8);
        let key = thread_rng().sample_iter(distr).take(key_len).collect();
        let value = thread_rng().sample_iter(distr).take(value_len).collect();

        (Arc::new(key), Arc::new(value))
    }

    #[test]
    fn db_worker() {
        let path = "/tmp/spiderq_main";
        let _ = fs::remove_dir_all(path);
        let db = db::Database::new(path, 16).unwrap();
        with_worker(
            "db",
            move |sock_db_master_tx, chan_tx, chan_rx| worker_db(sock_db_master_tx, chan_tx, chan_rx, db).unwrap(),
            move |mut sock, tx, rx| {
                let ((key_a, value_a), (key_b, value_b), (key_c, value_c)) = (rnd_kv(), rnd_kv(), rnd_kv());
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Global(GlobalReq::Add { key: key_a.clone(), value: value_a.clone(), mode: AddMode::Tail, }),
                                  DbRep::Local(DbLocalRep::Added(key_a.clone(), AddMode::Tail)));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Global(GlobalReq::Add { key: key_a.clone(), value: value_b.clone(), mode: AddMode::Tail, }),
                                  DbRep::Global(GlobalRep::Kept));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Global(GlobalReq::Add { key: key_b.clone(), value: value_b.clone(), mode: AddMode::Head, }),
                                  DbRep::Local(DbLocalRep::Added(key_b.clone(), AddMode::Head)));
                let now = SteadyTime::now();
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Local(DbLocalReq::LoadLent(177, key_a.clone(), 1000, now, LendMode::Poll)),
                                  DbRep::Global(GlobalRep::Lent { lend_key: 177, key: key_a.clone(), value: value_a, }));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Local(DbLocalReq::LoadLent(277, key_b.clone(), 1000, now, LendMode::Poll)),
                                  DbRep::Global(GlobalRep::Lent { lend_key: 277, key: key_b.clone(), value: value_b, }));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Local(DbLocalReq::LoadLent(377, key_c.clone(), 1000, now, LendMode::Poll)),
                                  DbRep::Local(DbLocalRep::LentNotFound(key_c.clone(), 1000, now, LendMode::Poll)));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Local(DbLocalReq::RepayUpdate(key_a.clone(), value_c.clone())),
                                  DbRep::Global(GlobalRep::Repaid));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Local(DbLocalReq::LoadLent(177, key_a.clone(), 1000, now, LendMode::Poll)),
                                  DbRep::Global(GlobalRep::Lent { lend_key: 177, key: key_a, value: value_c.clone(), }));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Global(GlobalReq::Update(key_b.clone(), value_c.clone())),
                                  DbRep::Global(GlobalRep::Updated));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Local(DbLocalReq::LoadLent(277, key_b.clone(), 1000, now, LendMode::Poll)),
                                  DbRep::Global(GlobalRep::Lent { lend_key: 277, key: key_b, value: value_c.clone(), }));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Global(GlobalReq::Update(key_c, value_c)),
                                  DbRep::Global(GlobalRep::NotFound));
                assert_worker_cmd(&mut sock, &tx, &rx, DbReq::Local(DbLocalReq::Stop), DbRep::Local(DbLocalRep::Stopped));
            });
    }

    #[test]
    fn pq_worker() {
        let pq = pq::PQueue::new();
        with_worker(
            "pq",
            move |sock_pq_master_tx, chan_tx, chan_rx| worker_pq(sock_pq_master_tx, chan_tx, chan_rx, pq).unwrap(),
            move |mut sock, tx, rx| {
                let ((key_a, value_a), (key_b, value_b)) = (rnd_kv(), rnd_kv());
                let now = SteadyTime::now();
                assert_worker_cmd(&mut sock, &tx, &rx, PqReq::Global(GlobalReq::Count), PqRep::Global(GlobalRep::Counted(0)));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::LendUntil(1000, now + Duration::milliseconds(1000), LendMode::Poll)),
                                  PqRep::Global(GlobalRep::QueueEmpty));
                tx_chan(PqReq::Local(PqLocalReq::Enqueue(key_a.clone(), AddMode::Tail)), None, &tx);
                tx_chan(PqReq::Local(PqLocalReq::Enqueue(key_b.clone(), AddMode::Tail)), None, &tx);
                assert_worker_cmd(&mut sock, &tx, &rx, PqReq::Local(PqLocalReq::NextTrigger), PqRep::Local(PqLocalRep::TriggerGot(None)));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::LendUntil(1000, now + Duration::milliseconds(1000), LendMode::Block)),
                                  PqRep::Local(PqLocalRep::Lent(3, key_a.clone(), 1000, now + Duration::milliseconds(1000), LendMode::Block)));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::NextTrigger),
                                  PqRep::Local(PqLocalRep::TriggerGot(Some(now + Duration::milliseconds(1000)))));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::LendUntil(500, now + Duration::milliseconds(500), LendMode::Block)),
                                  PqRep::Local(PqLocalRep::Lent(3, key_b.clone(), 500, now + Duration::milliseconds(500), LendMode::Block)));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::NextTrigger),
                                  PqRep::Local(PqLocalRep::TriggerGot(Some(now + Duration::milliseconds(500)))));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::LendUntil(250, SteadyTime::now() + Duration::milliseconds(250), LendMode::Block)),
                                  PqRep::Local(PqLocalRep::EmptyQueueHit { timeout: 250 }));
                tx_chan(PqReq::Local(PqLocalReq::RepayTimedOut), None, &tx);
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::NextTrigger),
                                  PqRep::Local(PqLocalRep::TriggerGot(Some(now + Duration::milliseconds(1000)))));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::LendUntil(500, now + Duration::milliseconds(500), LendMode::Block)),
                                  PqRep::Local(PqLocalRep::Lent(4, key_b.clone(), 500, now + Duration::milliseconds(500), LendMode::Block)));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Global(GlobalReq::Repay { lend_key: 3,
                                                                   key: key_a.clone(),
                                                                   value: value_a.clone(),
                                                                   status: RepayStatus::Penalty }),
                                  PqRep::Local(PqLocalRep::Repaid(key_a.clone(), value_a)));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::NextTrigger),
                                  PqRep::Local(PqLocalRep::TriggerGot(Some(now + Duration::milliseconds(500)))));
                tx_chan(PqReq::Local(PqLocalReq::RepayTimedOut), None, &tx);
                assert_worker_cmd(&mut sock, &tx, &rx, PqReq::Global(GlobalReq::Count), PqRep::Global(GlobalRep::Counted(2)));
                assert_worker_cmd(&mut sock, &tx, &rx, PqReq::Local(PqLocalReq::NextTrigger), PqRep::Local(PqLocalRep::TriggerGot(None)));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::LendUntil(500, now + Duration::milliseconds(500), LendMode::Block)),
                                  PqRep::Local(PqLocalRep::Lent(6, key_b.clone(), 500, now + Duration::milliseconds(500), LendMode::Block)));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Global(GlobalReq::Repay { lend_key: 6,
                                                                   key: key_b.clone(),
                                                                   value: value_b.clone(),
                                                                   status: RepayStatus::Drop }),
                                  PqRep::Local(PqLocalRep::Repaid(key_b.clone(), value_b.clone())));
                assert_worker_cmd(&mut sock, &tx, &rx, PqReq::Global(GlobalReq::Count), PqRep::Global(GlobalRep::Counted(1)));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Global(GlobalReq::Repay { lend_key: 6,
                                                                   key: key_b.clone(),
                                                                   value: value_b,
                                                                   status: RepayStatus::Penalty }),
                                  PqRep::Global(GlobalRep::NotFound));
                assert_worker_cmd(&mut sock, &tx, &rx, PqReq::Local(PqLocalReq::NextTrigger), PqRep::Local(PqLocalRep::TriggerGot(None)));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::LendUntil(500, now + Duration::milliseconds(500), LendMode::Block)),
                                  PqRep::Local(PqLocalRep::Lent(6, key_a.clone(), 500, now + Duration::milliseconds(500), LendMode::Block)));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::NextTrigger),
                                  PqRep::Local(PqLocalRep::TriggerGot(Some(now + Duration::milliseconds(500)))));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::Heartbeat(6, key_a, now + Duration::milliseconds(1000))),
                                  PqRep::Global(GlobalRep::Heartbeaten));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::Heartbeat(6, key_b, now + Duration::milliseconds(1000))),
                                  PqRep::Global(GlobalRep::Skipped));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::NextTrigger),
                                  PqRep::Local(PqLocalRep::TriggerGot(Some(now + Duration::milliseconds(1000)))));

                assert_worker_cmd(&mut sock, &tx, &rx, PqReq::Local(PqLocalReq::Stop), PqRep::Local(PqLocalRep::Stopped));
            });
    }

    /// Отправляем данные в сокет
    fn tx_sock(packet: GlobalReq, sock: &mut zmq::Socket) {
        // Размер данных
        let required = packet.encode_len();
        // Создаем сообщение сразу нужной емкости
        let mut msg = zmq::Message::with_capacity(required).unwrap();
        // Кодируем данные в сообщение
        packet.encode(&mut msg);
        // Отсылаем
        sock.send_msg(msg, 0).unwrap();
    }

    /// Получение данных из сокета и парсинг
    fn rx_sock(sock: &mut zmq::Socket) -> GlobalRep {
        // Получаем новое сообщение
        let msg = sock.recv_msg(0).unwrap();
        // Проверяем, что больше нету никаких сообщений в очереди еще
        assert!(!sock.get_rcvmore().unwrap());
        // Парсим полученное сообщение
        let (rep, _) = GlobalRep::decode(&msg).unwrap();
        rep
    }

    #[test]
    fn server() {
        // TODO: Рабочая директория для сервера?
        let path = "/tmp/spiderq_server";
        let _ = fs::remove_dir_all(path);
        // Адрес нашего внешнего сокета только внутри процесса
        let zmq_addr = "inproc://server";
        // Стартуем сервер очередей
        let (ctx, master_thread) = entrypoint(zmq_addr, path, 16).unwrap();
        
        // Создаем сокет запросов к серверу
        let mut sock_ftd = ctx.socket(zmq::REQ).unwrap();
        // Подключаемся к серверу
        sock_ftd.connect(zmq_addr).unwrap();

        let sock = &mut sock_ftd;

        // Ping запрос делаем
        tx_sock(GlobalReq::Ping, sock); assert_eq!(rx_sock(sock), GlobalRep::Pong);

        // Генерируем рандомные Key/Value и добавляем в очередь
        let ((key_a, value_a), (key_b, value_b), (key_c, value_c)) = (rnd_kv(), rnd_kv(), rnd_kv());

        // Добавляем А
        tx_sock(GlobalReq::Add { key: key_a.clone(), value: value_a.clone(), mode: AddMode::Tail, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Added);

        // Проверяем наличие значений по ключу А
        tx_sock(GlobalReq::Lookup(key_a.clone()), sock); 
        assert_eq!(rx_sock(sock), GlobalRep::ValueFound(value_a.clone()));

        // По ключу B нету
        tx_sock(GlobalReq::Lookup(key_b.clone()), sock); 
        assert_eq!(rx_sock(sock), GlobalRep::ValueNotFound);

        // Добавляем С
        tx_sock(GlobalReq::Add { key: key_c.clone(), value: value_c.clone(), mode: AddMode::Tail, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Added);

        // При повторном добавлении возвращается Kept
        tx_sock(GlobalReq::Add { key: key_a.clone(), value: value_b.clone(), mode: AddMode::Tail, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Kept);

        // Добавляем значение по ключу B
        tx_sock(GlobalReq::Add { key: key_b.clone(), value: value_b.clone(), mode: AddMode::Tail, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Added);

        // Ищем по ключу B
        tx_sock(GlobalReq::Lookup(key_b.clone()), sock); 
        assert_eq!(rx_sock(sock), GlobalRep::ValueFound(value_b.clone()));
        
        // Ищем по ключу C
        tx_sock(GlobalReq::Lookup(key_c.clone()), sock); 
        assert_eq!(rx_sock(sock), GlobalRep::ValueFound(value_c));

        // Удаляем по ключу С
        tx_sock(GlobalReq::Remove(key_c.clone()), sock); 
        assert_eq!(rx_sock(sock), GlobalRep::Removed);

        // Повторная попытка удаления выдает NotRemoved
        tx_sock(GlobalReq::Remove(key_c.clone()), sock); 
        assert_eq!(rx_sock(sock), GlobalRep::NotRemoved);

        // Ищем значение по ключу C
        tx_sock(GlobalReq::Lookup(key_c), sock); 
        assert_eq!(rx_sock(sock), GlobalRep::ValueNotFound);

        // Арендуем слот с таймаутом 1000 милисекунд, получая A
        // При этом блокируемся пока не будет нового значения
        tx_sock(GlobalReq::Lend { timeout: 1000, mode: LendMode::Block, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Lent { lend_key: 4, key: key_a.clone(), value: value_a.clone(), });

        // Арендуем еще одно значение с таймаутом 500 миллисекунд, получая B
        tx_sock(GlobalReq::Lend { timeout: 500, mode: LendMode::Block, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Lent { lend_key: 4, key: key_b.clone(), value: value_b.clone(), });

        // В очереди при этом у нас не остается элементов
        tx_sock(GlobalReq::Count, sock); 
        assert_eq!(rx_sock(sock), GlobalRep::Counted(0));

        // При новой попытке аренды в режиме Poll нам скажут, что очередь пустая
        tx_sock(GlobalReq::Lend { timeout: 500, mode: LendMode::Poll, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::QueueEmpty);

        // Возвращаем значение по ключу B назад
        tx_sock(GlobalReq::Repay { lend_key: 4, key: key_b.clone(), value: value_a.clone(), status: RepayStatus::Penalty }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Repaid);

        // Возвращаем значение по ключу A назад
        tx_sock(GlobalReq::Repay { lend_key: 4, key: key_a.clone(), value: value_b.clone(), status: RepayStatus::Penalty, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Repaid);

        // В очереди снова 2 элемента
        tx_sock(GlobalReq::Count, sock); 
        assert_eq!(rx_sock(sock), GlobalRep::Counted(2));

        // Снова пробуем арендовать и получаем значение по ключу B
        tx_sock(GlobalReq::Lend { timeout: 10000, mode: LendMode::Block, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Lent { lend_key: 6, key: key_b, value: value_a.clone(), });

        // Снова пробуем арендовать и получаем значение по ключу A
        tx_sock(GlobalReq::Lend { timeout: 1000, mode: LendMode::Block, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Lent { lend_key: 6, key: key_a.clone(), value: value_b.clone(), });

        // Снова нет элементов в очереди
        tx_sock(GlobalReq::Count, sock); 
        assert_eq!(rx_sock(sock), GlobalRep::Counted(0));

        fn round_ms(t: SteadyTime, expected: i64, variance: i64) -> i64 {
            let value = (SteadyTime::now() - t).num_milliseconds();
            ((value + variance) / expected) * expected
        }

        let t_start_a = SteadyTime::now();

        // Пытаемся арендовать новое значение в блокирующем режиме, нам должно прилететь значение по ключу A
        // так как оно имеет меньший таймаут
        tx_sock(GlobalReq::Lend { timeout: 500, mode: LendMode::Block, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Lent { lend_key: 7, key: key_a.clone(), value: value_b, });

        // Ожидание аренды не должно занять больше 1000 milliSec
        assert_eq!(round_ms(t_start_a, 1000, 100), 1000);

        // Элементов нету в очереди
        tx_sock(GlobalReq::Count, sock); 
        assert_eq!(rx_sock(sock), GlobalRep::Counted(0));
        
        // Обновляем значение по ключу A
        tx_sock(GlobalReq::Update(key_a.clone(), value_a.clone()), sock); 
        assert_eq!(rx_sock(sock), GlobalRep::Updated);

        // Обновляем аренду по ключу A
        tx_sock(GlobalReq::Heartbeat { lend_key: 7, key: key_a.clone(), timeout: 1000, }, sock); 
        assert_eq!(rx_sock(sock), GlobalRep::Heartbeaten);

        let t_start_b = SteadyTime::now();

        // Арендуем значение и получаем A
        tx_sock(GlobalReq::Lend { timeout: 500, mode: LendMode::Block, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Lent { lend_key: 8, key: key_a.clone(), value: value_a.clone(), });

        // Время ожидания новой аренды не должно превышать снова 1000 milliSec
        assert_eq!(round_ms(t_start_b, 1000, 100), 1000);

        // Возвращаем арендованное значение в очереди вручную
        tx_sock(GlobalReq::Repay { lend_key: 8, key: key_a.clone(), value: value_a.clone(), status: RepayStatus::Penalty, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Repaid);

        // При повторной попытке возврата будет ошибка, что нечего возвращать
        tx_sock(GlobalReq::Repay { lend_key: 8, key: key_a, value: value_a, status: RepayStatus::Penalty, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::NotFound);

        // Получаем статистику по работе очереди
        tx_sock(GlobalReq::Stats, sock);
        assert_eq!(rx_sock(sock),
                   GlobalRep::StatsGot {
                       ping: 1,
                       count: 4,
                       add: 4,
                       update: 1,
                       lookup: 5,
                       remove: 2,
                       lend: 7,
                       repay: 4,
                       heartbeat: 1,
                       stats: 1, });

        // Отправляем событие завершения работы
        tx_sock(GlobalReq::Terminate, sock); assert_eq!(rx_sock(sock), GlobalRep::Terminated);

        // Ждем заверешения
        master_thread.join().unwrap();
    }
}
