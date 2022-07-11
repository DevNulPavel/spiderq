use std::{io, fs, mem};
use std::io::Read;
use std::path::{PathBuf, Path};
use std::sync::Arc;
use std::sync::mpsc::{sync_channel, Receiver, TryRecvError};
use std::thread::{spawn, JoinHandle};
use std::collections::HashMap;
use std::collections::hash_map;
use std::iter::Iterator;
use tempdir::TempDir;
use byteorder::{ReadBytesExt, WriteBytesExt, NativeEndian};
use super::proto::{Key, Value};

/// Тип значения в базе
#[derive(Clone)]
enum ValueSlot {
    /// Все нормально, значение хорошее
    Value(Value),
    /// Удаленное значение из базы
    Tombstone,
}

type Index = HashMap<Key, ValueSlot>;

// TODO: Добавить реализацию Error
/// Ошибки работы с базой данных
#[derive(Debug)]
pub enum Error {
    /// База данных должна размещаться в директории
    DatabaseIsNotADir(String),
    /// Ошибка при получении информации о директории базы данных
    DatabaseStat(io::Error),
    /// Не смогли создать директорю для базы данных
    DatabaseMkdir(io::Error),
    /// Проблемы с созданием временного файлика для снапшота
    DatabaseTmpFile(String, io::Error),
    /// Проблема с открытием файлика базы
    DatabaseFileOpen(String, io::Error),
    /// не смогли записать в файлик снапшот базы
    DatabaseWrite(io::Error),
    /// Проблемы с перемещением файлика из tmp в нормальное место
    DatabaseMove(String, String, io::Error),
    /// Проблемы при чтении
    DatabaseRead(io::Error),
    /// Что-то неизвестное
    DatabaseUnexpectedEof,
}

/// Снапшоты базы данных
enum Snapshot {
    /// Снапшот в оперативной памяти, [Index] - это просто [HashMap]<[Key], [ValueSlot]>
    Memory(Index),
    /// Снапшот все еще в оперативной памяти, но зафиксирован от изменения и готов к скидыванию на диск
    Frozen(Arc<Index>),
    /// Снапшот сохраняется на диск, 
    /// но дополнительно продублирован в оперативной памяти
    Persisting { 
        /// Наш снапшот в оперативной памяти
        index: Arc<Index>,
        /// Канал для получения подтверждения, что снапшот уже скинут на диск успешно
        chan: Receiver<Result<(), Error>>,
        /// Join хендл для проверки, что поток отработал
        slave: JoinHandle<()>, 
    },
    /// Все было успешно сохранено на диск + продублировано в оперативке
    Persisted(Arc<Index>),
    /// У нас был запущен процесс слияня данныъ
    Merging { 
        /// Данные, которые сейчас сливаются вместе.
        /// Поле нужно для того, чтобы мы продолжали работать еще еще.
        indices: Arc<Vec<Arc<Index>>>,
        /// Канал для ожидания смерженной общей хешмапы
        chan: Receiver<Index>,
        /// Join для поддержки ожидания завершения
        slave: JoinHandle<()>, 
    },
}

impl Snapshot {
    fn count(&self) -> usize {
        match self {
            Snapshot::Memory(ref idx) => idx.len(),
            Snapshot::Frozen(ref idx) => idx.len(),
            Snapshot::Persisting { index: ref idx, .. } => idx.len(),
            Snapshot::Persisted(ref idx) => idx.len(),
            Snapshot::Merging { indices: ref idxs, .. } => idxs.iter().fold(0, |total, idx| total + idx.len()),
        }
    }

    fn lookup(&self, key: &Key) -> Option<&ValueSlot> {
        match self {
            Snapshot::Memory(ref idx) => idx.get(key),
            Snapshot::Frozen(ref idx) => idx.get(key),
            Snapshot::Persisting { index: ref idx, .. } => idx.get(key),
            Snapshot::Persisted(ref idx) => idx.get(key),
            Snapshot::Merging { indices: ref idxs, .. } => {
                for idx in idxs.iter() {
                    if let Some(value) = idx.get(key) {
                        return Some(value)
                    }
                }

                None
            }
        }
    }

    /// Собираем в общее хранилище ссылку на текущую хешмапу
    fn indices_refs<'a, 'b>(&'a self, refs: &'b mut Vec<&'a Index>) {
        match self {
            Snapshot::Memory(ref idx) => refs.push(&*idx),
            Snapshot::Frozen(ref idx) => refs.push(&*idx),
            Snapshot::Persisting { index: ref idx, .. } => refs.push(&*idx),
            Snapshot::Persisted(ref idx) => refs.push(&*idx),
            Snapshot::Merging { indices: ref idxs, .. } => {
                for idx in idxs.iter() {
                    refs.push(&*idx)
                }
            }
        }
    }
}

pub struct Database {
    database_dir: Arc<PathBuf>,
    flush_limit: usize,
    snapshots: Vec<Snapshot>,
}

impl Database {
    /// Создаем нашу базу данных для хранения
    pub fn new(database_dir: &str, flush_limit: usize) -> Result<Database, Error> {
        // Сначала получаем мета-данные по директории с базой данных
        match fs::metadata(database_dir) {
            // Если это директория - все хорошо, идем дальше
            Ok(ref metadata) if metadata.is_dir() => (),
            // Если это не директория - выходим тогда с ошибкой
            Ok(_) => return Err(Error::DatabaseIsNotADir(database_dir.to_owned())),
            // Если в целом нет такой директории
            Err(ref e) if e.kind() == io::ErrorKind::NotFound =>{
                // Тогда пробуем ее создать
                fs::create_dir(database_dir).map_err(Error::DatabaseMkdir)?;
            },
            // Все остальные ошибки пробрасываем выше
            Err(e) => return Err(Error::DatabaseStat(e)),
        }

        // Создаем вектор со пустым снапшотом базы в оперативной памяти в виде HashMap
        let mut snapshots = vec![Snapshot::Memory(Index::new())];
        // Пробуем подтянуть сохраненные снапшоты из базы данных, то есть ключ/значения
        if let Some(persisted_index) = load_index(database_dir)? {
            snapshots.push(Snapshot::Persisted(Arc::new(persisted_index)));
        }

        // Возвращаем нашу базу
        Ok(Database {
            database_dir: Arc::new(PathBuf::from(database_dir)),
            flush_limit,
            snapshots,
        })
    }

    pub fn approx_count(&self) -> usize {
        self.snapshots.iter().fold(0, |total, snapshot| total + snapshot.count())
    }

    /// Делаем поиск нужного элемента в базе данных.
    /// Поиск происходит по всем снапшотам.
    pub fn lookup(&self, key: &Key) -> Option<&Value> {
        // Идем по каждому снапшоту итеративно и ищем подходящий элемент
        for snapshot in self.snapshots.iter() {
            match snapshot.lookup(key) {
                Some(&ValueSlot::Value(ref value)) =>
                    return Some(value),
                Some(&ValueSlot::Tombstone) =>
                    return None,
                None =>
                    (),
            }
        }

        None
    }

    /// Добавляем новй элемент к списку
    pub fn insert(&mut self, key: Key, value: Value) {
        self.insert_slot(key, ValueSlot::Value(value))
    }

    /// Удаляем элемент по ключу путем проставления для записи пустого значения
    pub fn remove(&mut self, key: Key) {
        self.insert_slot(key, ValueSlot::Tombstone)
    }

    /// Пишем в самый первый in-memory снапшот наши данные по ключу
    fn insert_slot(&mut self, key: Key, slot: ValueSlot) {
        if let Some(&mut Snapshot::Memory(ref mut idx)) = self.snapshots.first_mut() {
            idx.insert(key, slot);
        } else {
            panic!("unexpected snapshots layout");
        }

        // Обязательно вызываем попытку обновления списка снепшотов
        self.update_snapshots(false);
    }

    /// Занимается тем, что скидывает данные по базе данных на диск
    pub fn flush(&mut self) {
        self.update_snapshots(true);
    }

    /// Формируем снапшоты
    fn update_snapshots(&mut self, flush_mode: bool) {
        loop {
            // Первый снепшот у нас всегда является Memory типом обычно.
            // Поэтому проверяем - не было ли заполнено?
            if let Some(index_to_freeze) = match self.snapshots.first_mut() {
                // Делаем проверку, достигли мы лимита по записям в индексе оперативной памяти или нет?
                // Либо у нас форсированое скидывание данных на диск?
                Some(&mut Snapshot::Memory(ref mut idx)) if (idx.len() >= self.flush_limit) || (!idx.is_empty() && flush_mode) =>{
                    // Извлекаем все значения из базы в оперативной памяти, заменяя на пустой словарь
                    Some(mem::take(idx))
                },
                _ => None,
            } {
                // Если мы извлекли данные из оперативной памяти, тогда нам нужно зафиксировать данные в типе Frozen
                // Поэтому кладем на второе место в списке наш снапшот
                self.snapshots.insert(1, Snapshot::Frozen(Arc::new(index_to_freeze)));
                continue;
            }

            // Если последний снапшот в списке у нас типа Frozen.
            // Например, тот, что положили до этого
            if let Some(last_snapshot) = self.snapshots.last_mut() {
                if let Some(index_to_persist) = match last_snapshot {
                    &mut Snapshot::Frozen(ref idx) => Some(idx.clone()),
                    _ => None,
                } {
                    // Создаем небуфферизированный синхронный канал
                    let (tx, rx) = sync_channel(0);
                    // Директория, в которой хранится база
                    let slave_dir = self.database_dir.clone();
                    // Создаем клон Arc этого нашего Frozen снапшота
                    let slave_index = index_to_persist.clone();
                    // Создаем новый поток в котором сохраняем на диск снепшот + заменяем файлик снепшотов
                    let slave = spawn(move || tx.send(persist(slave_dir, slave_index)).unwrap());

                    // Теперь этот последний снапшот у нас типа "сохраняется на диск"
                    *last_snapshot = Snapshot::Persisting {
                        index: index_to_persist,
                        chan: rx,
                        slave,
                    };
                    continue;
                }
            }

            // Делаем проверку, вдруг нам нужно вмержишь некоторые снапшоты
            enum MergeLayout { FirstMemory, AtLeastOneFrozen, MaybeMoreFrozen, LastPersisted, }
            // Сначала определяем, что нам вообще надо делать обходя массив снапшотов
            let merge_decision =
                self.snapshots.iter().fold(Some(MergeLayout::FirstMemory), |state, snapshot| match (state, snapshot) {
                    (Some(MergeLayout::FirstMemory), &Snapshot::Memory(..)) => Some(MergeLayout::AtLeastOneFrozen),
                    (Some(MergeLayout::AtLeastOneFrozen), &Snapshot::Frozen(..)) => Some(MergeLayout::MaybeMoreFrozen),
                    (Some(MergeLayout::MaybeMoreFrozen), &Snapshot::Frozen(..)) => Some(MergeLayout::MaybeMoreFrozen),
                    (Some(MergeLayout::MaybeMoreFrozen), &Snapshot::Persisted(..)) => Some(MergeLayout::LastPersisted),
                    _ => None,
                });
            // Если у нас последний снапшот сохраненный на диск
            if let Some(MergeLayout::LastPersisted) = merge_decision {
                // Из снапшотов тянем все, кроме самого первого - выбирая лишь те, 
                // которые заморожены + сохранены на диск
                let indices: Vec<_> = self.snapshots.drain(1 ..)
                    .map(|snapshot| match snapshot {
                        Snapshot::Frozen(idx) => idx,
                        Snapshot::Persisted(idx) => idx,
                        _ => unreachable!(),
                    })
                    .collect();
                // Для продолжения работоспособности сохраняем несмерженные данные
                let master_indices = Arc::new(indices);
                // Создаем копию Arc для мержа
                let slave_indices = master_indices.clone();
                let (tx, rx) = sync_channel(0);
                // Запускаем в работу процесс слияния
                let slave = spawn(move || tx.send(merge(slave_indices)).unwrap());
                // В снапшоты сохряняем состояние мержа для отслеживания состояния
                self.snapshots.push(Snapshot::Merging {
                    indices: master_indices,
                    chan: rx,
                    slave,
                });

                continue;
            }

            // Находим те снапшоты, у которых состояние "сохранение на диск"
            if let Some(persisting_snapshot) = self.snapshots.iter_mut().find(|snapshot| matches!(snapshot, Snapshot::Persisting{..}) ) {
                // Получаем статус завершения из канала
                let done_index =
                    if let &mut Snapshot::Persisting { index: ref idx, chan: ref rx, .. } = persisting_snapshot {
                        // Если у нас не форсированный режим сброса
                        if !flush_mode {
                            // Тогда просто пробуем получить данные из канала без ожидания
                            match rx.try_recv() {
                                Ok(Ok(())) => Some(idx.clone()),
                                Ok(Err(e)) => panic!("persisting thread failed: {:?}", e),
                                Err(TryRecvError::Empty) => None,
                                Err(TryRecvError::Disconnected) => panic!("persisting thread is down"),
                            }
                        } else {
                            // Если у нас форсированный, тогда принудительно ждем из канала новые данные
                            match rx.recv().unwrap() {
                                Ok(()) => Some(idx.clone()),
                                Err(e) => panic!("persisting thread failed: {:?}", e),
                            }
                        }
                    } else {
                        unreachable!()
                    };

                // Если есть что-то, что завершилось успешно
                if let Some(persisted_index) = done_index {
                    // Тогда заменяем на тип Persisted + дожидаемся завершения работы потока
                    if let Snapshot::Persisting { slave: thread, .. } 
                        = mem::replace(persisting_snapshot, Snapshot::Persisted(persisted_index)) 
                    {
                        thread.join().unwrap();
                    } else {
                        unreachable!()
                    }

                    continue;
                }
            }

            // Находим снапшоты, которые завершились после мержа
            if let Some(merging_snapshot) = self.snapshots.iter_mut().find(|snapshot| matches!(snapshot, Snapshot::Merging{..})) {
                // Получаем завершенный снапшот после мержа если есть
                let done_index = if let &mut Snapshot::Merging { chan: ref rx, .. } = merging_snapshot {
                        // Если у нас обычный режим, то просто пробуем получить с помощью .try_recv данные
                        if !flush_mode {
                            match rx.try_recv() {
                                Ok(merged_index) => Some(merged_index),
                                Err(TryRecvError::Empty) => None,
                                Err(TryRecvError::Disconnected) => panic!("merging thread is down"),
                            }
                        } else {
                            // Если же у нас форсированный режим, значит нужно уже получать блокирующим образом
                            Some(rx.recv().unwrap())
                        }
                    } else {
                        unreachable!()
                    };
                
                // Если есть завершенный снапшот
                if let Some(merged_index) = done_index {
                    // Тогда заменяем на тип Frozen
                    if let Snapshot::Merging { slave: thread, .. } 
                        = mem::replace(merging_snapshot, Snapshot::Frozen(Arc::new(merged_index))) 
                    {
                        // И ждем завершения потока
                        thread.join().unwrap();
                    } else {
                        unreachable!()
                    }

                    continue;
                }
            }

            break;
        }
    }

    /// Создаем итератор по снепшотам базы данных.
    /// Сам итератор исключает дубликаты ключей. 
    /// Выдает толкьо новые ключи, которые еще не были.
    pub fn iter(&self) -> Iter {
        let mut indices = Vec::new();
        // Идем по каждому снапшоту
        for snapshot in self.snapshots.iter() {
            // Собираем ссылки на хешмапы индексов
            snapshot.indices_refs(&mut indices);
        }

        Iter {
            indices,
            index: 0,
            iter: None,
        }
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        // При уничтожении базы данных делаем принудительный сброс на диск
        self.flush();
    }
}

/// Итератор по хешмапам-индексам базы данных
pub struct Iter<'a> {
    // Ссылки на хеш-мапы индексов
    indices: Vec<&'a Index>,
    // Текущий индекс в массиве индексов
    index: usize,
    // Итератор по текущей хешмапе из массива индексов
    iter: Option<hash_map::Iter<'a, Key, ValueSlot>>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a Key, &'a Value);

    fn next(&mut self) -> Option<(&'a Key, &'a Value)> {
        // Если еще не дошли до конца списка индексов
        while self.index < self.indices.len() {
            // Проверяем - есть ли у нас уже итератор по хешмапе
            if let Some(ref mut map_iter) = self.iter {
                // Если итератор есть, итерируемся по нему
                for (key, slot) in map_iter.by_ref() {
                    // TODO: Оптимизировать?
                    // Делаем проверку по всем индексам в обратном порядке,
                    // вдруг уже в предыдущих итерациях мы выдавали такой ключ
                    if self.indices[0 .. self.index]
                        .iter()
                        .rev()
                        .any(|older_index| older_index.contains_key(key)) {
                            continue
                        }

                    match slot {
                        // Нашли значение - все ок
                        ValueSlot::Value(ref value) =>
                            return Some((key, value)),
                        // Удаленное значение - проходим мимо
                        ValueSlot::Tombstone =>
                            continue,
                    }
                }
            } else {
                // Если итератора еще нету - создаем его и идем на новую итерацию
                self.iter = Some(self.indices[self.index].iter());
                continue;
            }

            // Обработали все в текущем индексе - идем на следующий
            self.iter = None;
            self.index += 1;
        }

        None
    }
}

/// Просто конвертация пути в строку
fn filename_as_string(filename: &Path) -> String {
    filename.to_string_lossy().into_owned()
}

/// Из буфера для чтения вычитываем из самого начала какие-то данные
fn read_vec<R>(source: &mut R) -> Result<Vec<u8>, Error> where R: io::Read {
    // Вычитываем сначала размер данных
    let len = source.read_u32::<NativeEndian>().map_err(|e| Error::DatabaseRead(From::from(e)))? as usize;
    // Создаем буфер для хранения
    let mut buffer = Vec::with_capacity(len);
    let source_ref = source.by_ref();
    // Вычитываем конкретный размер данных из файлика после размера
    match source_ref.take(len as u64).read_to_end(&mut buffer) {
        Ok(bytes_read) if bytes_read == len => Ok(buffer),
        Ok(_) => Err(Error::DatabaseUnexpectedEof),
        Err(e) => Err(Error::DatabaseRead(e)),
    }
}

/// Попытка подтянуть снепшоты из базы данных в виде [Index], который по сути является HashMap
fn load_index(database_dir: &str) -> Result<Option<Index>, Error> {
    // К пути базы данных мы добавляем еще имя файлика
    let mut db_file = PathBuf::new();
    db_file.push(database_dir);
    db_file.push("snapshot");

    // Пробуем открыть наш файлик снепшотов
    match fs::File::open(&db_file) {
        // Если файлик есть
        Ok(file) => {
            // Создаем буферизированную обертку над ним
            let mut source = io::BufReader::new(file);
            // Читаем из самого начала байты количества элементов
            // TODO: Может быть заменить на конкретный размер - LittleEndian, 
            // вдруг переноситься будет база с экзотического одного компьютера, на другой
            let total = source.read_u64::<NativeEndian>().map_err(|e| Error::DatabaseRead(From::from(e)))? as usize;
            // Создаем хешмапу для хранения
            let mut index = Index::with_capacity(total);
            // Вычитываем из файлика снепшота ключ и значение и собираем в наш индекс
            for _ in 0 .. total {
                let (key, value) = (read_vec(&mut source)?, read_vec(&mut source)?);
                index.insert(Arc::new(key), ValueSlot::Value(Arc::new(value)));
            }
            Ok(Some(index))
        },
        // Нету файлика - все ок
        Err(ref e) if e.kind() == io::ErrorKind::NotFound =>
            Ok(None),
        // Не смогли открыть базу - кидаем ошибку
        Err(e) =>
            Err(Error::DatabaseFileOpen(filename_as_string(&db_file), e))
    }
}

/// Пишем в писателя какое-то значение
fn write_vec<W>(value: &Arc<Vec<u8>>, target: &mut W) -> Result<(), Error> where W: io::Write {
    // Сначала длину данных
    target.write_u32::<NativeEndian>(value.len() as u32).map_err(|e| Error::DatabaseWrite(From::from(e)))?;
    // Затем сами данные
    target.write_all(&value[..]).map_err(Error::DatabaseWrite)?;
    Ok(())
}

/// Выполнение сохранения на диск базы данных из оперативной памяти
fn persist(dir: Arc<PathBuf>, index: Arc<Index>) -> Result<(), Error> {
    // Имя файлика будет снапшот
    let db_filename = "snapshot";
    // Создаем во временной директории `/tmp` файлик с путем базы данных
    let tmp_dir = TempDir::new_in(&*dir, "snapshot").map_err(Error::DatabaseMkdir)?;
    let mut tmp_db_file = PathBuf::new();
    tmp_db_file.push(tmp_dir.path());
    tmp_db_file.push(db_filename);

    let approx_len = index.len();
    let mut actual_len = 0;

    {
        // Открываем файлик на запись во временной директории
        let mut file = io::BufWriter::new(
            fs::File::create(&tmp_db_file).map_err(|e| Error::DatabaseTmpFile(filename_as_string(&tmp_db_file), e))?);
        // Пишем в начало длину элементов
        file.write_u64::<NativeEndian>(approx_len as u64).map_err(|e| Error::DatabaseWrite(From::from(e)))?;
        // Затем пишем в файлик key/value поочередно
        for (key, slot) in &*index {
            // Но дополнительно сверяем, что это нормальное значение, а не удаленное
            if let &ValueSlot::Value(ref value) = slot {
                write_vec(key, &mut file)?;
                write_vec(value, &mut file)?;
                actual_len += 1;
            }
        }
    }

    // Если мы записали не все данные в файлик, 
    // тогда нам надо файлик заново открыть и подправить в самом 
    // начале файлика размер на актуальный
    if actual_len != approx_len {
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&tmp_db_file)
            .map_err(|e| Error::DatabaseTmpFile(filename_as_string(&tmp_db_file), e))?;
        file.write_u64::<NativeEndian>(actual_len as u64).map_err(|e| Error::DatabaseWrite(From::from(e)))?;
    }

    // Уже нормальный путь для файлика куда нам надо положить его
    let mut db_file = PathBuf::new();
    db_file.push(&*dir);
    db_file.push(db_filename);

    // Затем файлик перемещаем в нужное место
    // Таким образом у нас достигается некая транзакционность
    fs::rename(&tmp_db_file, &db_file).map_err(|e| Error::DatabaseMove(filename_as_string(&tmp_db_file), filename_as_string(&db_file), e))
}

/// Выполнение слияния массивов индексов в один общий
fn merge(indices: Arc<Vec<Arc<Index>>>) -> Index {
    let mut base_index: Option<Index> = None;
    for index in indices.iter().rev() {
        if let Some(ref mut base) = base_index {
            for (key, slot) in &**index {
                base.insert(key.clone(), slot.clone());
            }
        } else {
            base_index = Some((**index).clone());
        }
    }

    base_index.take().unwrap()
}

#[cfg(test)]
mod test {
    use std::fs;
    use std::sync::Arc;
    use std::collections::HashMap;
    use rand::{thread_rng, distributions::Uniform, Rng};
    use super::{Database};
    use super::super::proto::{Key, Value};

    fn mkdb(path: &str, flush_limit: usize) -> Database {
        let _ = fs::remove_dir_all(path);
        Database::new(path, flush_limit).unwrap()
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


    fn rnd_fill_check(db: &mut Database, check_table: &mut HashMap<Key, Value>, count: usize) {
        let mut to_remove = Vec::new();
        let remove_count = count / 2;
        for _ in 0 .. count + remove_count {
            let (k, v) = rnd_kv();
            check_table.insert(k.clone(), v.clone());
            db.insert(k.clone(), v.clone());
            if to_remove.len() < remove_count {
                to_remove.push(k.clone());
            }
        }

        for k in to_remove {
            check_table.remove(&k);
            db.remove(k);
        }

        check_against(db, check_table);
    }

    fn check_against(db: &Database, check_table: &HashMap<Key, Value>) {
        if db.approx_count() < check_table.len() {
            panic!("db.approx_count() == {} < check_table.len() == {}", db.approx_count(), check_table.len());
        }

        for (k, v) in check_table {
            assert_eq!(db.lookup(k), Some(v));
        }
    }

    #[test]
    fn make() {
        let db = mkdb("/tmp/spiderq_a", 10);
        assert_eq!(db.approx_count(), 0);
    }

    #[test]
    fn insert_lookup() {
        let mut db = mkdb("/tmp/spiderq_b", 16);
        assert_eq!(db.approx_count(), 0);
        let mut check_table = HashMap::new();
        rnd_fill_check(&mut db, &mut check_table, 10);
    }

    #[test]
    fn save_load() {
        let mut check_table = HashMap::new();
        {
            let mut db = mkdb("/tmp/spiderq_c", 16);
            assert_eq!(db.approx_count(), 0);
            rnd_fill_check(&mut db, &mut check_table, 10);
        }
        {
            let db = Database::new("/tmp/spiderq_c", 16).unwrap();
            assert_eq!(db.approx_count(), 10);
            check_against(&db, &check_table);
        }
    }

    #[test]
    fn stress() {
        let mut check_table = HashMap::new();
        {
            let mut db = mkdb("/tmp/spiderq_d", 160);
            rnd_fill_check(&mut db, &mut check_table, 2560);
        }
        {
            let db = Database::new("/tmp/spiderq_d", 160).unwrap();
            assert!(db.approx_count() <= 2560);
            check_against(&db, &check_table);
        }
    }

    #[test]
    fn iter() {
        let mut check_table = HashMap::new();
        {
            let mut db = mkdb("/tmp/spiderq_e", 64);
            rnd_fill_check(&mut db, &mut check_table, 1024);
            for (k, v) in db.iter() {
                assert_eq!(check_table.get(k), Some(v));
            }
        }
        {
            let db = Database::new("/tmp/spiderq_e", 64).unwrap();
            for (k, v) in db.iter() {
                assert_eq!(check_table.get(k), Some(v));
            }
        }
    }
}
