use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::{Crc, CRC_32_ISO_HDLC};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    borrow::Borrow,
    fs::{File, OpenOptions},
    io::{self, Cursor, Read, Seek, SeekFrom, Write},
    os::windows::raw,
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
    _phantom: std::marker::PhantomData<T>,
}

impl<T> Collection<T> {
    fn new(buffer: Vec<u8>, client: Client) -> Self {
        Self {
            buffer,
            client,
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

    fn get_inner(f: &mut Cursor<&[u8]>) -> Result<Vec<u8>, DatabaseError> {
        f.seek(SeekFrom::Current(4))?;
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
        let mut readable = Cursor::new(self.buffer.as_slice());
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
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
