use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::{Crc, CRC_32_ISO_HDLC};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    borrow::Borrow,
    fs::OpenOptions,
    io::{self, BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write},
    path::PathBuf,
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error("data corruption encountered ({checksum:08x} != {expected_checksum:08x})")]
    MismatchedChecksum {
        checksum: u32,
        expected_checksum: u32,
    },
    #[error("inserted data too large, object of {0} bytes > u32::MAX")]
    DataTooLarge(usize),
    #[error(transparent)]
    SerdeError(#[from] bincode::Error),
}

const CRC: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

#[derive(Debug)]
pub struct Client {
    path: PathBuf,
}

impl Client {
    pub fn new<T: AsRef<str>>(db: T) -> Self {
        let mut path = PathBuf::new();
        path.push(db.as_ref());
        path.set_extension("cdb");
        Self { path }
    }

    pub fn load<T>(self) -> Result<Collection<T>, DatabaseError> {
        let file = OpenOptions::new().read(true).open(&self.path)?;
        let mut f = BufReader::new(file);
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        f.seek(SeekFrom::Start(0))?;
        let mut count = 0;
        loop {
            let raw_doc = Self::process_document(&mut f);
            if let Err(err) = raw_doc {
                match err {
                    DatabaseError::IO(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    _ => return Err(err),
                }
            }
            count += 1;
        }
        Ok(Collection::new(buf, self.path, count))
    }

    fn process_document<R: Read>(f: &mut R) -> Result<(), DatabaseError> {
        let saved_checksum = f.read_u32::<LittleEndian>()?;
        let data_len = f.read_u32::<LittleEndian>()?;
        let mut data = Vec::with_capacity(data_len as usize);
        f.take(u64::from(data_len)).read_to_end(&mut data)?;
        let checksum = CRC.checksum(&data);
        if checksum != saved_checksum {
            return Err(DatabaseError::MismatchedChecksum {
                checksum,
                expected_checksum: saved_checksum,
            });
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct Collection<T> {
    buffer: Vec<u8>,
    db_path: PathBuf,
    delete_pos: Vec<u64>,
    length: usize,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> Collection<T> {
    fn new(buffer: Vec<u8>, path: PathBuf, count: usize) -> Self {
        Self {
            buffer,
            db_path: path,
            delete_pos: Vec::new(),
            length: count,
            _phantom: std::marker::PhantomData::default(),
        }
    }

    pub fn flush(&mut self) -> Result<(), DatabaseError> {
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.db_path)?;
        let mut f = BufWriter::new(file);
        let mut readable = Cursor::new(self.buffer.as_slice());
        loop {
            let current_position = readable.seek(SeekFrom::Current(0))?;
            let raw_data = Self::flush_inner(&mut readable);
            let raw_data = match raw_data {
                Ok(d) => d,
                Err(err) => match err {
                    DatabaseError::IO(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    _ => return Err(err),
                },
            };
            if !self.delete_pos.contains(&current_position) {
                f.write_all(&raw_data)?;
            }
        }
        f.flush()?;
        Ok(())
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.length
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    fn insert_inner(&mut self, encoded: &[u8]) -> Result<(), DatabaseError> {
        let data_len = encoded.len();
        if data_len > u32::MAX as usize {
            return Err(DatabaseError::DataTooLarge(data_len));
        }
        let checksum = CRC.checksum(encoded);
        self.buffer.write_u32::<LittleEndian>(checksum)?;
        self.buffer.write_u32::<LittleEndian>(data_len as u32)?;
        self.buffer.write_all(encoded)?;
        Ok(())
    }

    fn get_inner<R: Read>(f: &mut R) -> Result<Vec<u8>, DatabaseError> {
        f.read_u32::<LittleEndian>()?;
        let data_len = f.read_u32::<LittleEndian>()?;
        let mut data = Vec::with_capacity(data_len as usize);
        f.take(u64::from(data_len)).read_to_end(&mut data)?;
        Ok(data)
    }

    fn flush_inner<R: Read>(f: &mut R) -> Result<Vec<u8>, DatabaseError> {
        let checksum = f.read_u32::<LittleEndian>()?;
        let data_len = f.read_u32::<LittleEndian>()?;
        let mut data = Vec::with_capacity(data_len as usize + 8);
        data.write_u32::<LittleEndian>(checksum)?;
        data.write_u32::<LittleEndian>(data_len)?;
        f.take(data_len as u64).read_to_end(&mut data)?;
        Ok(data)
    }
}

impl<T> Collection<T>
where
    T: Serialize + DeserializeOwned,
{
    pub fn insert(&mut self, item: impl Borrow<T>) -> Result<(), DatabaseError> {
        let encoded = bincode::serialize(item.borrow())?;
        self.insert_inner(&encoded)?;
        self.length += 1;
        Ok(())
    }

    pub fn get<F>(&mut self, filter: F) -> Result<Vec<T>, DatabaseError>
    where
        F: Fn(&T) -> bool,
    {
        let mut readable = self.buffer.as_slice();
        let mut result = Vec::new();
        loop {
            let raw_data = Self::get_inner(&mut readable);
            let raw_data = match raw_data {
                Ok(d) => d,
                Err(err) => match err {
                    DatabaseError::IO(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    _ => return Err(err),
                },
            };
            let data = bincode::deserialize(&raw_data)?;
            if filter(&data) {
                result.push(data);
            }
        }
        Ok(result)
    }

    pub fn update<F, M>(&mut self, filter: F, map: M) -> Result<(), DatabaseError>
    where
        F: Fn(&T) -> bool,
        M: Fn(T) -> T,
    {
        let mut readable = Cursor::new(self.buffer.as_slice());
        let mut transformed_values = Vec::new();
        loop {
            let current_position = readable.seek(SeekFrom::Current(0))?;
            let raw_data = Self::get_inner(&mut readable);
            let raw_data = match raw_data {
                Ok(d) => d,
                Err(err) => match err {
                    DatabaseError::IO(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    _ => return Err(err),
                },
            };
            let data = bincode::deserialize(&raw_data)?;
            if filter(&data) {
                self.delete_pos.push(current_position);
                let transformed_value = map(data);
                transformed_values.push(transformed_value);
            }
        }
        for value in transformed_values {
            self.insert(value)?;
        }
        Ok(())
    }

    pub fn delete<F>(&mut self, filter: F) -> Result<(), DatabaseError>
    where
        F: Fn(T) -> bool,
    {
        let mut readable = Cursor::new(self.buffer.as_slice());
        loop {
            let current_position = readable.seek(SeekFrom::Current(0))?;
            let raw_data = Self::get_inner(&mut readable);
            let raw_data = match raw_data {
                Ok(d) => d,
                Err(err) => match err {
                    DatabaseError::IO(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    _ => return Err(err),
                },
            };
            let data = bincode::deserialize(&raw_data)?;
            if filter(data) {
                self.delete_pos.push(current_position);
                self.length -= 1;
            }
        }
        Ok(())
    }
}

impl<T> Drop for Collection<T> {
    fn drop(&mut self) {
        let _r = self.flush();
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
