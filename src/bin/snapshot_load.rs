use std::{io, env, process, fs::File};
use std::io::{Write, BufRead};
use getopts::Options;

use spiderq::{db, pq};
use spiderq_proto::{Key, Value, AddMode};

#[derive(Debug)]
pub enum Error {
    Getopts(getopts::Fail),
    Db(db::Error),
    Io(io::Error),
    Pq(pq::Error)
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

pub fn entrypoint(maybe_matches: getopts::Result) -> Result<(), Error> {
    env_logger::init();

    let matches = maybe_matches.map_err(Error::Getopts)?;
    let skip_db = matches.opt_present("skip-db");

    let database_dir = matches.opt_str("database").unwrap();
    let lines_count: Option<usize> = matches.opt_get("number").unwrap();
    let skip_lines_count: Option<usize> = matches.opt_get("drop").unwrap();
    let snapshot_path = matches.opt_str("snapshot").unwrap();

    fn maybe_take<B>(
        lines: io::Lines<B>,
        count: Option<usize>,
        skip: Option<usize>
    ) -> Box<dyn Iterator<Item = io::Result<String>>>
        where
            B: BufRead + 'static
    {
        match (count, skip) {
            (Some(t), Some(s)) => Box::new(lines.skip(s).take(t)),
            (Some(t), None) => Box::new(lines.take(t)),
            (None, Some(s)) => Box::new(lines.skip(s)),
            (None, None) => Box::new(lines)
        }
    }

    if !skip_db {
        let mut db = db::Database::new(&database_dir).map_err(Error::Db)?;
        eprintln!("done db load");

        let file = File::open(snapshot_path.clone())?;
        let lines = io::BufReader::new(file).lines();

        let (db_tx, db_rx) = std::sync::mpsc::sync_channel(10000);

        let db_thread = std::thread::spawn(move || {
            for r in db_rx {
                match r {
                    Ok((k, v)) => {
                        db.insert(k, v).unwrap();
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        });

        for line in maybe_take(lines, lines_count, skip_lines_count) {
            if let Ok(line) = line {
                let mut split_iter = line.splitn(2, '\t');

                let key = split_iter.next().unwrap().to_owned();
                let key = key.into_boxed_str();
                println!("{}", key);
                let key: Key = key.into_boxed_bytes().into();

                let value = split_iter.next().unwrap().to_owned();
                let value = value.into_boxed_str();
                let value: Value = value.into_boxed_bytes().into();

                db_tx.send(Ok((key, value))).unwrap();
            }
        }

        db_tx.send(Err(())).unwrap();
        db_thread.join().unwrap();
        eprintln!("done db writes");
    }

    let mut queue = pq::PQueue::new(&database_dir).map_err(Error::Pq)?;
eprintln!("done queue load");

    let (pq_tx, pq_rx) = std::sync::mpsc::sync_channel(10000);

    let pq_thread = std::thread::spawn(move || {
        for r in pq_rx {
            match r {
                Ok(key) => {
                    queue.add(key, AddMode::Tail, false);
                }
                Err(_) => {
                    break;
                }
            }
        }
    });

    let file = File::open(snapshot_path)?;
    let lines = io::BufReader::new(file).lines();

    for line in maybe_take(lines, lines_count, skip_lines_count) {
        if let Ok(line) = line {
            let mut split_iter = line.splitn(2, '\t');

            let key = split_iter.next().unwrap().to_owned();
            let key = key.into_boxed_str();
            println!("{}", key);
            let key: Key = key.into_boxed_bytes().into();

            pq_tx.send(Ok(key)).unwrap();
        }
    }

    pq_tx.send(Err(())).unwrap();
    pq_thread.join().unwrap();
    eprintln!("done pq write");

    Ok(())
}

fn main() {
    let mut args = env::args();
    let cmd_proc = args.next().unwrap();
    let mut opts = Options::new();

    opts.reqopt("s", "snapshot", "snapshot file path", "snapshot.dump");
    opts.reqopt("", "database", "database dir", "");
    opts.optopt("n", "number", "load this number of rows only (optional)", "");
    opts.optopt("d", "drop", "drop this number of rows and import the rest (or n if specified)", "");
    opts.optflag("", "skip-db", "skip all db ops");

    match entrypoint(opts.parse(args)) {
        Ok(()) =>
            (),
        Err(cause) => {
            let _ = writeln!(&mut io::stderr(), "Error: {:?}", cause);
            let usage = format!("Usage: {}", cmd_proc);
            let _ = writeln!(&mut io::stderr(), "{}", opts.usage(&usage[..]));
            process::exit(1);
        }
    }
}
