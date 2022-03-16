use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::{Crc, CRC_32_ISO_HDLC};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    borrow::Borrow,
    fs::{File, OpenOptions},
    io::{self, Cursor, Read, Seek, SeekFrom, Write},
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
}

const CRC: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

#[derive(Debug)]
pub struct Client {
    f: File,
}

impl Client {
    pub fn new(db: &str) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(db)?;
        Ok(Self { f: file })
    }

    pub fn load<T>(mut self) -> Result<Collection<T>, DatabaseError> {
        let mut buf = Vec::new();
        self.f.read_to_end(&mut buf)?;
        let readable = buf.as_slice();
        loop {
            let raw_doc = Self::process_document(readable);
            if let Err(err) = raw_doc {
                match err {
                    DatabaseError::IO(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    _ => return Err(err),
                }
            }
        }
        Ok(Collection::new(buf, self))
    }

    fn process_document<R: Read>(mut f: R) -> Result<(), DatabaseError> {
        let saved_checksum = f.read_u32::<LittleEndian>()?;
        let data_len = f.read_u32::<LittleEndian>()?;
        let mut data = Vec::with_capacity(data_len as usize);
        f.take(data_len as u64).read_to_end(&mut data)?;
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
    client: Client,
    delete_pos: Vec<u64>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> Collection<T> {
    fn new(buffer: Vec<u8>, client: Client) -> Self {
        Self {
            buffer,
            client,
            delete_pos: Vec::new(),
            _phantom: Default::default(),
        }
    }

    fn insert_inner(&mut self, encoded: Vec<u8>) -> Result<(), DatabaseError> {
        let data_len = encoded.len();
        let checksum = CRC.checksum(&encoded);
        self.buffer.write_u32::<LittleEndian>(checksum)?;
        self.buffer.write_u32::<LittleEndian>(data_len as u32)?;
        self.buffer.write_all(&encoded)?;
        Ok(())
    }

    fn get_inner<R: Read>(f: &mut R) -> Result<Vec<u8>, DatabaseError> {
        f.read_u32::<LittleEndian>()?;
        let data_len = f.read_u32::<LittleEndian>()?;
        let mut data = Vec::with_capacity(data_len as usize);
        f.take(data_len as u64).read_to_end(&mut data)?;
        Ok(data)
    }
}

impl<T> Collection<T>
where
    T: Serialize + DeserializeOwned,
{
    pub fn insert(&mut self, item: impl Borrow<T>) -> Result<(), DatabaseError> {
        let encoded = bincode::serialize(item.borrow()).unwrap();
        self.insert_inner(encoded)?;
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
            let data = bincode::deserialize(&raw_data).unwrap();
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
            let data = bincode::deserialize(&raw_data).unwrap();
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
            let data = bincode::deserialize(&raw_data).unwrap();
            if filter(data) {
                self.delete_pos.push(current_position);
            }
        }
        Ok(())
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
