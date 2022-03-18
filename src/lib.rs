//! This crate provides an easy to use API to store persistent data of the same type.
//!
//! Any type that is able to be deserialized or serialized using Serde can be stored on disk.
//! Data is stored on disk with a CRC32 checksum associated with every object stored to ensure
//! data integrity. In the event of a checksum mismatch, this API returns a `DataPoisonError<T>`,
//! similar to the concept of poisoning in mutexes, in which data stored on disk might be in a
//! bad state and probably should not be used. However, like `PoisonError` in std, the API provides
//! you with methods to get the underlying value if you really need it.
//!
//! This crate is meant for storing small serializable data that stores the state of an application
//! after exit. Since all the data is loaded onto memory, handling large amounts of data is not
//! advised. However, the data stored on disk has a relatively small footprint and should not take
//! that much space.
//!
//! Note that this is not an embedded database and there are other libraries which are better suited
//! for this task, such as `sled`:
//! https://github.com/spacejam/sled

#![deny(missing_docs)]

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::{Crc, CRC_32_ISO_HDLC};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Cursor, ErrorKind, Read, Write},
    path::PathBuf,
};
use thiserror::Error;

const DATA_VERSION: u32 = 1;

/// This is the main error type for `pdc`.
#[derive(Error, Debug)]
pub enum DatabaseError<T> {
    /// This returns `std::io::Error`
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// This returns an error if the saved checksum does not match the checksum of its
    /// associated object. The underlying data can be accessed through its `into_inner()`
    /// method.
    #[error(transparent)]
    MismatchedChecksum(#[from] DataPoisonError<T>),
    /// `pdc` can only store an object that takes up `u32::MAX` bytes of space. If you run
    /// into this error you should consider some other library.
    #[error("inserted data too large, object of {0} bytes > u32::MAX")]
    DataTooLarge(usize),
    /// Serialization/deserialization error for an object.
    #[error(transparent)]
    SerdeError(#[from] bincode::Error),
    /// Each version of `pdc` has its own data version. The data format should remain stable
    /// in perpetuity unless some unforseen circumstances arises. In that case, the data
    /// version is incremented by 1. The version is stored in each `.pdc` file and if the
    /// data version in file does not match the data version of the current version of `pdc`,
    /// this error occurs, in which case, you should change the version of `pdc` you are using
    /// for your crate to match the data version of the file you are accessing. Such major
    /// changes would be documented in the README.
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

/// An object that is responsible for handling IO operations with regards to file
/// opening/closing/writing as well as serialization and deserialization. The
/// main data type of this crate.
#[derive(Debug)]
pub struct Client<T: Serialize + DeserializeOwned> {
    file: File,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> Client<T>
where
    T: Serialize + DeserializeOwned,
{
    /// Creates a new client object. It opens a file if a file with the same name exists or
    /// creates a new file if it doesn't exist. This returns an error if it fails to open
    /// or create a new file.
    ///
    /// NOTE: This methods automatically appends a `.pdc` file extension to your path if
    /// the extension is not included in the `PathBuf` passed to this method.
    pub fn new(mut path: PathBuf) -> Result<Self, DatabaseError<T>> {
        path.set_extension("pdc");
        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        Ok(Self {
            file,
            _phantom: std::marker::PhantomData::default(),
        })
    }

    fn validate_data_scheme<R: Read>(f: &mut R) -> Result<(), DatabaseError<T>> {
        let saved_version = f.read_u32::<LittleEndian>()?;
        if saved_version != DATA_VERSION {
            return Err(DatabaseError::WrongDataVersion(saved_version));
        }
        Ok(())
    }

    /// Returns a vector of the deserialized object. If the file is empty, this method
    /// returns `Ok(None)`. If a checksum mismatch occurs, a `DataPoisonError<T>` is
    /// returned, in which you can get the underlying deserialized objects via the
    /// method `into_inner()`.
    pub fn load(&mut self) -> Result<Option<Vec<T>>, DatabaseError<T>> {
        let mut buf = Vec::new();
        self.file.read_to_end(&mut buf)?;
        if buf.is_empty() {
            return Ok(None);
        }
        let result = Self::binary_to_vec(buf)?;
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

    /// Writes the provided serializable objects to disk. If no file is found,
    /// a new file will be created and written to.
    pub fn write(&mut self, data: &[T]) -> Result<(), DatabaseError<T>> {
        let buf = Self::vec_to_binary(data)?;
        self.file.write_all(&buf)?;
        Ok(())
    }

    fn binary_to_vec(raw_data: Vec<u8>) -> Result<Vec<T>, DatabaseError<T>> {
        let mut is_corrupted = None;
        let mut f = Cursor::new(raw_data);
        Self::validate_data_scheme(&mut f)?;
        let mut result = Vec::new();
        loop {
            let raw_doc = Self::process_document(&mut f);
            let raw_doc = match raw_doc {
                Ok(d) => d,
                Err(e) => match e {
                    InnerDatabaseError::Io(err) if err.kind() == ErrorKind::UnexpectedEof => {
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
        Ok(result)
    }

    fn vec_to_binary(data: &[T]) -> Result<Vec<u8>, DatabaseError<T>> {
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
        Ok(buf.into_inner())
    }
}

/// This errors occurs due to a checksum mismatch. Thus it is important to backup your
/// `.pdc` files periodically to prevent data loss. However, you can still get the underlying
/// objects if you are sure only one or two objects are malformed via the `into_inner()` method
/// or its equivalents, in which case count your lucky stars as `serde` is still able to
/// deserialize your objects, or that the saved checksum is the one that is corrupted instead
/// of your objects.
#[derive(Error, Debug)]
#[error("data corruption encountered ({:08x} != {:08x})", .checksum.expected_checksum, .checksum.saved_checksum)]
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

    /// Consumes this error returning its underlying objects.
    #[must_use]
    pub fn into_inner(self) -> Vec<T> {
        self.collection
    }

    /// Returns a reference to the underlying objects.
    #[must_use]
    pub fn get_ref(&self) -> &[T] {
        &self.collection
    }

    /// Returns a mutable reference to the underlying objects.
    pub fn get_mut(&mut self) -> &mut [T] {
        &mut self.collection
    }
}

#[cfg(test)]
mod tests {
    use crate::{Checksum, Client, DataPoisonError};
    use serde_derive::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    struct Test {
        id: usize,
        message: String,
    }

    fn generate_test_data() -> Vec<Test> {
        let test1 = Test {
            id: 1,
            message: "Hello there, you suck".to_string(),
        };
        let test2 = Test {
            id: 2,
            message: "No you".to_string(),
        };
        let test3 = Test {
            id: 3,
            message: "You both suck".to_string(),
        };
        vec![test1, test2, test3]
    }

    #[test]
    fn binary_vec_conversion() {
        let test_messages = generate_test_data();
        let binary = Client::vec_to_binary(&test_messages).unwrap();
        let vec: Vec<Test> = Client::binary_to_vec(binary).unwrap();
        assert_eq!(test_messages, vec);
    }

    #[test]
    fn poisoning() {
        let test_messages = generate_test_data();
        let checksum = Checksum::new(23, 45);
        let mut error = DataPoisonError::new(test_messages.clone(), checksum);
        assert_eq!(&test_messages, error.get_ref());
        assert_eq!(&test_messages, error.get_mut());
        assert_eq!(test_messages, error.into_inner());
    }
}
