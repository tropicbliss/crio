use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::{Crc, CRC_32_ISO_HDLC};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fs::OpenOptions,
    io::{self, Cursor, ErrorKind, Read, Write},
    path::PathBuf,
};
use thiserror::Error;

const DATA_VERSION: u32 = 1;

#[derive(Error, Debug)]
pub enum DatabaseError<T> {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    MismatchedChecksum(DataPoisonError<T>),
    #[error("inserted data too large, object of {0} bytes > u32::MAX")]
    DataTooLarge(usize),
    #[error(transparent)]
    SerdeError(#[from] bincode::Error),
    #[error("wrong data version: expected {DATA_VERSION}, found {0}")]
    WrongDataVersion(u32),
}

#[derive(Error, Debug)]
enum InnerDatabaseError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("you should not be able to see this error")]
    MismatchedChecksum(Checksum, Vec<u8>),
}

#[derive(Debug)]
struct Checksum {
    saved_checksum: u32,
    expected_checksum: u32,
}

impl Checksum {
    fn new(saved_checksum: u32, expected_checksum: u32) -> Self {
        Self {
            saved_checksum,
            expected_checksum,
        }
    }
}

const CRC: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

#[derive(Debug)]
pub struct Client<T: Serialize + DeserializeOwned> {
    path: PathBuf,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> Client<T>
where
    T: Serialize + DeserializeOwned,
{
    #[must_use]
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            _phantom: std::marker::PhantomData::default(),
        }
    }

    fn validate_data_scheme<R: Read>(f: &mut R) -> Result<(), DatabaseError<T>> {
        let saved_version = f.read_u32::<LittleEndian>()?;
        if saved_version != DATA_VERSION {
            return Err(DatabaseError::WrongDataVersion(saved_version));
        }
        Ok(())
    }

    pub fn load(self) -> Result<Option<Vec<T>>, DatabaseError<T>> {
        let mut is_corrupted = None;
        let file = OpenOptions::new().read(true).open(&self.path);
        let mut file = match file {
            Ok(f) => f,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(e) => return Err(DatabaseError::Io(e)),
        };
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        drop(file);
        let mut f = Cursor::new(buf);
        Self::validate_data_scheme(&mut f)?;
        let mut result = Vec::new();
        loop {
            let raw_doc = Self::process_document(&mut f);
            let raw_doc = match raw_doc {
                Ok(d) => d,
                Err(e) => match e {
                    InnerDatabaseError::Io(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    InnerDatabaseError::MismatchedChecksum(checksum, data) => {
                        is_corrupted = Some(checksum);
                        data
                    }
                    InnerDatabaseError::Io(e) => return Err(DatabaseError::Io(e)),
                },
            };
            let data = bincode::deserialize(&raw_doc)?;
            result.push(data);
        }
        if let Some(checksum_data) = is_corrupted {
            let error = DataPoisonError::new(result, checksum_data);
            return Err(DatabaseError::MismatchedChecksum(error));
        }
        Ok(Some(result))
    }

    fn process_document<R: Read>(f: &mut R) -> Result<Vec<u8>, InnerDatabaseError> {
        let saved_checksum = f.read_u32::<LittleEndian>()?;
        let data_len = f.read_u32::<LittleEndian>()?;
        let mut data = Vec::with_capacity(data_len as usize);
        f.take(u64::from(data_len)).read_to_end(&mut data)?;
        let expected_checksum = CRC.checksum(&data);
        if expected_checksum != saved_checksum {
            let checksum = Checksum::new(saved_checksum, expected_checksum);
            return Err(InnerDatabaseError::MismatchedChecksum(checksum, data));
        }
        Ok(data)
    }

    pub fn write(&self, data: Vec<T>) -> Result<(), DatabaseError<T>> {
        let mut buf = Cursor::new(Vec::new());
        buf.write_u32::<LittleEndian>(DATA_VERSION)?;
        for document in data {
            let raw_data = bincode::serialize(&document)?;
            let data_len = raw_data.len();
            if data_len > u32::MAX as usize {
                return Err(DatabaseError::DataTooLarge(data_len));
            }
            let checksum = CRC.checksum(&raw_data);
            buf.write_u32::<LittleEndian>(checksum)?;
            buf.write_u32::<LittleEndian>(data_len as u32)?;
            buf.write_all(&raw_data)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        file.write_all(buf.get_ref())?;
        Ok(())
    }
}

#[derive(Error, Debug)]
#[error("data corruption encountered ({:08x} != {:08x})", .checksum.saved_checksum, .checksum.expected_checksum)]
pub struct DataPoisonError<T> {
    collection: Vec<T>,
    checksum: Checksum,
}

impl<T> DataPoisonError<T> {
    fn new(collection: Vec<T>, checksum: Checksum) -> Self {
        Self {
            collection,
            checksum,
        }
    }

    #[must_use]
    pub fn into_inner(self) -> Vec<T> {
        self.collection
    }

    #[must_use]
    pub fn get_ref(&self) -> &[T] {
        &self.collection
    }

    pub fn get_mut(&mut self) -> &mut [T] {
        &mut self.collection
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
