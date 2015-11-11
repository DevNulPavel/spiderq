use std::{io, fs, mem};
use std::slice::bytes;
use std::ops::DerefMut;
use std::io::{Seek, SeekFrom, Read, Write};
use std::convert::From;
use std::path::PathBuf;
use byteorder::{ReadBytesExt, WriteBytesExt, NativeEndian};

#[derive(Debug)]
pub enum Error {
    DatabaseIsNotADir(String),
    DatabaseStat(io::Error),
    DatabaseMkdir(io::Error),
    DatabaseFile(String, String, io::Error),
    Metadata(String, io::Error),
    Seek(String, io::Error),
    Read(String, io::Error),
    Write(String, io::Error),
    IndexIsTooBig { given: u32, total: u32, },
    EofReadingData { index: u32, len: usize, read: usize },
}

pub struct Database {
    filename_idx: PathBuf,
    filename_db: PathBuf,
    fd_idx: fs::File,
    fd_db: fs::File,
    cache_idx: Option<Vec<u8>>,
    cache_db: Option<Vec<u8>>,
}

fn open_rw(database_dir: &str, filename: &str) -> Result<(PathBuf, fs::File), Error> {
    let mut full_filename = PathBuf::new();
    full_filename.push(database_dir);
    full_filename.push(filename);
    let file = 
        try!(fs::OpenOptions::new()
             .read(true)
             .write(true)
             .append(true)
             .create(true)
             .open(&full_filename)
             .map_err(|e| Error::DatabaseFile(database_dir.to_owned(), filename.to_owned(), e)));
    Ok((full_filename, file))
}

fn filename_as_string(filename: &PathBuf) -> String {
    filename.to_string_lossy().into_owned()
}

fn file_size(fd: &fs::File, filename: &PathBuf) -> Result<u64, Error> {
    let md = try!(fd.metadata().map_err(|e| Error::Metadata(filename_as_string(filename), e)));
    Ok(md.len())
}

#[derive(Debug)]
pub enum LoadError<E> {
    Db(Error),
    Load(E),
}

impl<E> From<Error> for LoadError<E> {
    fn from(err: Error) -> LoadError<E> {
        LoadError::Db(err)
    }
}

pub trait Loader {
    type Error;

    fn set_len(&mut self, len: usize) -> Result<(), Self::Error>;
    fn contents(&mut self) -> &mut [u8];
}

impl Loader for Vec<u8> {
    type Error = ();

    fn set_len(&mut self, len: usize) -> Result<(), ()> {
        self.resize(len, 0);
        Ok(())
    }

    fn contents(&mut self) -> &mut [u8] {
        self.deref_mut()
    }
}

impl Database {
    pub fn new(database_dir: &str, use_mem_cache: bool) -> Result<Database, Error> {
        match fs::metadata(database_dir) {
            Ok(ref metadata) if metadata.is_dir() => (),
            Ok(_) => return Err(Error::DatabaseIsNotADir(database_dir.to_owned())),
            Err(ref e) if e.kind() == io::ErrorKind::NotFound =>
                try!(fs::create_dir(database_dir).map_err(|e| Error::DatabaseMkdir(e))),
            Err(e) => return Err(Error::DatabaseStat(e)),
        }

        let (filename_idx, mut fd_idx) = try!(open_rw(database_dir, "spiderq.idx"));
        let (filename_db, mut fd_db) = try!(open_rw(database_dir, "spiderq.db"));
        let cache_idx = if use_mem_cache {
            let mut cache = Vec::with_capacity(try!(file_size(&fd_idx, &filename_idx)) as usize);
            try!(fd_idx.read_to_end(&mut cache).map_err(|e| Error::Read(filename_as_string(&filename_idx), e)));
            Some(cache)
        } else {
            None
        };
        let cache_db = if use_mem_cache {
            let mut cache = Vec::with_capacity(try!(file_size(&fd_db, &filename_db)) as usize);
            try!(fd_db.read_to_end(&mut cache).map_err(|e| Error::Read(filename_as_string(&filename_db), e)));
            Some(cache)
        } else {
            None
        };

        Ok(Database {
            filename_idx: filename_idx,
            filename_db: filename_db,
            fd_idx: fd_idx,
            fd_db: fd_db,
            cache_idx: cache_idx,
            cache_db: cache_db,
        })
    }

    pub fn count(&self) -> Result<usize, Error> {
        Ok(if let Some(ref cache) = self.cache_idx {
            cache.len()
        } else {
            try!(file_size(&self.fd_idx, &self.filename_idx)) as usize
        } / mem::size_of::<u64>())
    }

    pub fn add(&mut self, data: &[u8]) -> Result<u32, Error> {
        let last_offset = try!(file_size(&self.fd_db, &self.filename_db));
        try!(self.fd_idx.seek(SeekFrom::End(0)).map_err(|e| Error::Seek(filename_as_string(&self.filename_idx), e)));
        try!(self.fd_idx.write_u64::<NativeEndian>(last_offset)
             .map_err(|e| Error::Write(filename_as_string(&self.filename_idx), From::from(e))));
        
        try!(self.fd_db.seek(SeekFrom::End(0)).map_err(|e| Error::Seek(filename_as_string(&self.filename_db), e)));
        try!(self.fd_db.write_u32::<NativeEndian>(data.len() as u32)
             .map_err(|e| Error::Write(filename_as_string(&self.filename_db), From::from(e))));
        try!(self.fd_db.write(data).map_err(|e| Error::Write(filename_as_string(&self.filename_db), e)));

        if let (Some(cache_idx), Some(cache_db)) = (self.cache_idx.as_mut(), self.cache_db.as_mut()) {
            let last_offset = cache_db.len() as u64;
            cache_idx.write_u64::<NativeEndian>(last_offset).unwrap();
            cache_db.write_u32::<NativeEndian>(data.len() as u32).unwrap();
            cache_db.write(data).unwrap();
        }

        Ok(try!(self.count()) as u32 - 1)
    }

    pub fn load<'a, 'b, E, L>(&'a mut self, index: u32, loader: &'b mut L) -> Result<(), LoadError<E>> where L: Loader<Error = E> {
        let total = try!(self.count()) as u32;
        if index >= total {
            return Err(LoadError::Db(Error::IndexIsTooBig { given: index, total: total, }))
        }

        if let (Some(cache_idx), Some(cache_db)) = (self.cache_idx.as_ref(), self.cache_db.as_ref()) {
            let offset = (&cache_idx[(index as usize * mem::size_of::<u64>()) ..]).read_u64::<NativeEndian>().unwrap() as usize;
            let data_len = (&cache_db[offset ..]).read_u32::<NativeEndian>().unwrap() as usize;
            try!(loader.set_len(data_len).map_err(|e| LoadError::Load(e)));
            let mut target = loader.contents();
            let start = offset + mem::size_of::<u32>();
            let end = start + data_len;
            bytes::copy_memory(&cache_db[start .. end], target);
        } else {
            try!(self.fd_idx.seek(SeekFrom::Start((index as usize * mem::size_of::<u64>()) as u64))
                 .map_err(|e| Error::Seek(filename_as_string(&self.filename_idx), e)));
            let offset = try!(self.fd_idx.read_u64::<NativeEndian>()
                              .map_err(|e| Error::Read(filename_as_string(&self.filename_idx), From::from(e))));
            try!(self.fd_db.seek(SeekFrom::Start(offset))
                 .map_err(|e| Error::Seek(filename_as_string(&self.filename_db), e)));
            let data_len = try!(self.fd_db.read_u32::<NativeEndian>()
                                .map_err(|e| Error::Read(filename_as_string(&self.filename_db), From::from(e)))) as usize;
            try!(loader.set_len(data_len).map_err(|e| LoadError::Load(e)));

            {
                let mut target = loader.contents();
                while !target.is_empty() {
                    match self.fd_db.read(target) {
                        Ok(0) if target.is_empty() => break,
                        Ok(0) => return Err(LoadError::Db(Error::EofReadingData { index: index, len: data_len, read: data_len - target.len(), })),
                        Ok(n) => { let tmp = target; target = &mut tmp[n ..]; },
                        Err(ref e) if e.kind() == io::ErrorKind::Interrupted => { },
                        Err(e) => return Err(LoadError::Db(Error::Read(filename_as_string(&self.filename_db), e))),
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::fs;
    use super::{Database, Error, LoadError};

    fn mkdb(path: &str, use_mem_cache: bool) -> Database {
        let _ = fs::remove_dir_all(path);
        Database::new(path, use_mem_cache).unwrap()
    }

    #[test]
    fn make() {
        let db = mkdb("/tmp/spiderq_a", false);
        assert_eq!(db.count().unwrap(), 0);
    }

    #[test]
    fn reopen() {
        {
            let db = mkdb("/tmp/spiderq_b", false);
            assert_eq!(db.count().unwrap(), 0);
        }
        {
            let db = Database::new("/tmp/spiderq_b", false).unwrap();
            assert_eq!(db.count().unwrap(), 0);
        }
    }

    #[test]
    fn open_database_fail() {
        match Database::new("/qwe", false) {
            Ok(..) => panic!("expected fail"),
            Err(..) => (),
        }
    }

    fn mkfill(path: &str, use_mem_cache: bool) -> Database {
        let mut db = mkdb(path, use_mem_cache);
        assert_eq!(db.add(&[1, 2, 3]).unwrap(), 0);
        assert_eq!(db.add(&[4, 5, 6, 7]).unwrap(), 1);
        assert_eq!(db.add(&[8, 9]).unwrap(), 2);
        assert_eq!(db.count().unwrap(), 3);
        db
    }

    #[test]
    fn open_database_fill() {
        let _ = mkfill("/tmp/spiderq_c", false);
    }

    #[test]
    fn open_database_fill_cache() {
        let _ = mkfill("/tmp/spiderq_d", true);
    }

    fn open_database_check_db(mut db: Database) {
        let mut data = Vec::new();
        db.load(0, &mut data).unwrap(); assert_eq!(&data, &[1, 2, 3]);
        db.load(1, &mut data).unwrap(); assert_eq!(&data, &[4, 5, 6, 7]);
        db.load(2, &mut data).unwrap(); assert_eq!(&data, &[8, 9]);
        match db.load(3, &mut data) {
            Err(LoadError::Db(Error::IndexIsTooBig { given: 3, total: 3, })) => (),
            other => panic!("unexpected Database::load return value: {:?}", other),
        }
    }

    #[test]
    fn open_database_check() {
        open_database_check_db(mkfill("/tmp/spiderq_e", false));
    }

    #[test]
    fn open_database_check_cache() {
        open_database_check_db(mkfill("/tmp/spiderq_f", true));
    }
}

