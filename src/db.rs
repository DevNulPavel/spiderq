use std::io;
use std::iter::Iterator;
use super::proto::{Key, Value};
use crate::system::{SledTree, System};

#[derive(Debug)]
pub enum Error {
    DatabaseIsNotADir(String),
    DatabaseStat(io::Error),
    DatabaseMkdir(io::Error),
    DatabaseDriverError(sled::Error),
}

impl From<sled::Error> for Error {
    fn from(err: sled::Error) -> Self {
        Error::DatabaseDriverError(err)
    }
}

pub struct Database {
    db: sled::Db,
    system: System,
}

impl Database {
    pub fn new(database_dir: &str) -> Result<Database, Error> {
        let mut db_path = std::path::PathBuf::from(database_dir);
        db_path.push("store");

        let db_cfg = sled::Config::new()
            .path(db_path)
            .mode(sled::Mode::HighThroughput)
            .flush_every_ms(Some(10 * 60 * 1000));

        let start = std::time::Instant::now();
        let db = db_cfg.open().map_err(|e| Error::DatabaseDriverError(e))?;
        let end = std::time::Instant::now();
        metrics::timing!("db.init_time", start, end);

        let system = db.open_tree("system")?;
        let system = System::new(system, SledTree::Db(db.clone()), false);

        Ok(Database {
            db,
            system
        })
    }

    pub fn approx_count(&mut self) -> usize {
        self.system.count()
    }

    pub fn count(&self) -> usize {
        self.db.len()
    }

    pub fn lookup(&self, key: &Key) -> Option<Value> {
        let start = std::time::Instant::now();

        let r = match self.db.get(key) {
            Ok(value) => value.map(|v| v.into()),
            Err(_) => None
        };

        let end = std::time::Instant::now();
        metrics::timing!("db.lookup_time", start, end);
        metrics::counter!("db.lookup", 1);

        r
    }

    pub fn insert(&mut self, key: Key, value: Value) -> Result<(), Error> {
        let start = std::time::Instant::now();

        match self.db.insert(key, value)? {
            None => {
                self.system.incr();
            }
            Some(_) => {}
        }

        let end = std::time::Instant::now();
        metrics::timing!("db.insert_time", start, end);
        metrics::counter!("db.insert", 1);

        Ok(())
    }

    pub fn remove(&mut self, key: Key) -> Result<(), Error> {
        let start = std::time::Instant::now();

        match self.db.remove(key.as_ref())? {
            None => {}
            Some(_) => {
                self.system.decr();
            }
        }

        let end = std::time::Instant::now();
        metrics::timing!("db.remove_time", start, end);
        metrics::counter!("db.remove", 1);

        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), Error> {
        let start = std::time::Instant::now();

        self.db.flush()?;

        let end = std::time::Instant::now();
        metrics::timing!("db.flush_time", start, end);
        metrics::counter!("db.flush", 1);

        Ok(())
    }

    pub fn iter(&self) -> Iter {
        let iter = self.db.iter();

        Iter {
            iter
        }
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        self.flush().unwrap();
    }
}

pub struct Iter {
    iter: sled::Iter
}

impl Iterator for Iter {
    type Item = (Key, Value);

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            None => None,
            Some(r) => match r {
                Ok((k, v)) => Some((k.into(), v.into())),
                Err(_) => self.next()
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::fs;
    use std::collections::HashMap;
    use rand::{thread_rng, Rng, distributions::Uniform};
    use super::{Database};
    use super::super::proto::{Key, Value};

    fn mkdb(path: &str) -> Database {
        let _ = fs::remove_dir_all(path);
        Database::new(path).unwrap()
    }

    fn rnd_kv() -> (Key, Value) {
        let mut rng = thread_rng();
        let byte_range = Uniform::from(0..255);

        let key: Key = rng
            .sample_iter(byte_range)
            .take(rng.gen_range(1, 64))
            .collect();

        let value: Value = rng
            .sample_iter(byte_range)
            .take(rng.gen_range(1, 64))
            .collect();

        (key, value)
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

    fn check_against(db: &mut Database, check_table: &HashMap<Key, Value>) {
        if db.count() < check_table.len() {
            panic!("db.approx_count() == {} < check_table.len() == {}", db.approx_count(), check_table.len());
        }

        for (k, v) in check_table {
            assert_eq!(db.lookup(k).as_ref(), Some(v));
        }
    }

    #[test]
    fn make() {
        let mut db = mkdb("/tmp/spiderq_a");
        assert_eq!(db.count(), 0);
    }

    #[test]
    fn insert_lookup() {
        let mut db = mkdb("/tmp/spiderq_b");
        assert_eq!(db.count(), 0);
        let mut check_table = HashMap::new();
        rnd_fill_check(&mut db, &mut check_table, 10);
    }

    #[test]
    fn save_load() {
        let mut check_table = HashMap::new();
        {
            let mut db = mkdb("/tmp/spiderq_c");
            assert_eq!(db.count(), 0);
            rnd_fill_check(&mut db, &mut check_table, 10);
        }
        {
            let mut db = Database::new("/tmp/spiderq_c").unwrap();
            assert_eq!(db.count(), 10);
            check_against(&mut db, &check_table);
        }
    }

    #[test]
    fn stress() {
        let mut check_table = HashMap::new();
        {
            let mut db = mkdb("/tmp/spiderq_d");
            rnd_fill_check(&mut db, &mut check_table, 2560);
        }
        {
            let mut db = Database::new("/tmp/spiderq_d").unwrap();
            assert!(db.approx_count() <= 2560);
            check_against(&mut db, &check_table);
        }
    }

    #[test]
    fn iter() {
        let mut check_table = HashMap::new();
        {
            let mut db = mkdb("/tmp/spiderq_e");
            rnd_fill_check(&mut db, &mut check_table, 1024);
            for (k, v) in db.iter() {
                assert_eq!(check_table.get(&k), Some(v).as_ref());
            }
        }
        {
            let db = Database::new("/tmp/spiderq_e").unwrap();
            for (k, v) in db.iter() {
                assert_eq!(check_table.get(&k), Some(v).as_ref());
            }
        }
    }
}
