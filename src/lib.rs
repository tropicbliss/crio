#![warn(clippy::pedantic)]

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
//! after exit. Since all the data is loaded
//! onto memory when calling load on a `Client<T>`, handling large amounts of data is not advised. However,
//! the data stored on disk has a relatively small footprint and should not take up that much space.
//!
//! Note that this is not an embedded database and there are other libraries which are better suited
//! for this task, such as `sled`:
//! <https://github.com/spacejam/sled>
//!
//! # Example
//!
//! ```ignore
//! use crio::Client;
//! use serde_derive::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize, Debug)]
//! struct Message {
//!     id: usize,
//!     message: String,
//! }
//!
//! let msg1 = Message {
//!     id: 1,
//!     message: "Hello there, you suck".to_string(),
//! };
//! let msg2 = Message {
//!     id: 2,
//!     message: "No you".to_string(),
//! };
//! let msg3 = Message {
//!     id: 3,
//!     message: "You both suck".to_string(),
//! };
//! let messages = vec![msg1, msg2, msg3];
//! let client: Client<Message> = Client::new("messages", false)?; // If no file is found, a new empty file is created.
//! client.write_many(&messages)?; // If no file is found, a new file is created and then written to. Append is set to false such that it overwrites any previous value stored on the same file
//! let returned_messages = client.load()?;
//! if let Some(data) = returned_messages {
//!     assert_eq!(messages, data);
//! } else {
//!     panic!("File is empty");
//! }
//! ```

#![deny(missing_docs)]

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::{Crc, CRC_32_ISO_HDLC};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{ErrorKind, Read, Seek, SeekFrom, Write},
    num::TryFromIntError,
    path::Path,
};
use thiserror::Error;

const FILE_HEADER: u32 = 67297350;
const FILE_VERSION: u32 = 2;

/// This is the main error type of this crate.
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
    /// `crio` can only store an object that takes up `u32::MAX` bytes of space. If you run
    /// into this error you should consider some other library.
    #[error("inserted data too large: object > u32::MAX")]
    DataTooLarge(#[from] TryFromIntError),
    /// Serialization/deserialization error for an object.
    #[error(transparent)]
    SerdeError(#[from] bincode::Error),
    /// Invalid file header. You might not be reading a valid `crio` file.
    #[error("invalid file header")]
    FileHeader,
    /// Wrong file version. Use another version of this library to read the file correctly.
    ///
    /// # File versions:
    ///
    /// 1: 0.2 versions and below
    /// 2: 0.3 versions and above
    #[error("wrong file version: expected {}, found {0}", FILE_VERSION)]
    FileVersion(u32),
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
/// opening/closing/writing as well as serialization and deserialization.
/// The main data type of this crate.
pub struct Client<T: Serialize + DeserializeOwned> {
    file: File,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> Client<T>
where
    T: Serialize + DeserializeOwned,
{
    /// Creates a new client object. It opens a file if a file with the same name exists or
    /// creates a new file if it doesn't exist. Set the `append` parameter to false if you want to
    /// overwrite all data while calling `write()` or `write_many()`, or true if you
    /// simply want to append data to the file.
    ///
    /// # Errors
    ///
    /// - The usual `std::io::Error` if it fails to open or create a new file.
    pub fn new<P: AsRef<Path>>(path: P, append: bool) -> Result<Self, DatabaseError<T>> {
        let file = if append {
            OpenOptions::new()
                .read(true)
                .create(true)
                .append(true)
                .open(path.as_ref())?
        } else {
            OpenOptions::new()
                .read(true)
                .create(true)
                .write(true)
                .truncate(true)
                .open(path.as_ref())?
        };
        Ok(Self {
            file,
            _phantom: std::marker::PhantomData::default(),
        })
    }

    /// Returns a vector of the deserialized object. If the file is empty, this method
    /// returns `Ok(None)`.
    ///
    /// # Errors
    ///
    /// - If a checksum mismatch occurs, a `DataPoisonError<T>` is
    /// returned, in which you can get the underlying deserialized objects via the
    /// method `into_inner()`.
    ///
    /// - `bincode::Error` occurs if the deserializer fails to deserialize bytes from
    /// the file to your requested object. In that case, the most probable reason
    /// is that the data in that file stores some other data type and you are
    /// attempting to deserialize it to the wrong data type.
    ///
    /// - The usual `std::io::Error` such as `ErrorKind::UnexpectedEof` if the file
    /// that is being accessed is malformed and there are no more bytes to be read
    /// when the method is expecting more data.
    pub fn load(&mut self) -> Result<Option<Vec<T>>, DatabaseError<T>> {
        let mut buf = Vec::new();
        self.file.seek(SeekFrom::Start(0))?;
        self.file.read_to_end(&mut buf)?;
        if buf.is_empty() {
            return Ok(None);
        }
        let result = binary_to_vec(&buf)?;
        Ok(Some(result))
    }

    /// Writes the provided serializable objects to disk. If no file is found,
    /// a new file will be created and written to.
    ///
    /// # Errors
    ///
    /// - `std::num::TryFromIntError` occurs when an object you are inserting
    /// takes up more space than `u32::MAX` bytes. In that case, seek help.
    ///
    /// - The usual `std::io::Error` such as `ErrorKind::UnexpectedEof` if the file
    /// that is being accessed is malformed and there are no more bytes to be read
    /// when the method is expecting more data.
    ///
    /// - Serialization errors when the data provided fails to serialize for some reason.
    pub fn write_many(&mut self, data: &[T]) -> Result<(), DatabaseError<T>> {
        let buf = vec_to_binary(data)?;
        self.file.write_all(&buf)?;
        Ok(())
    }

    /// Writes the provided serializable object to disk. If no file is found,
    /// a new file will be created and written to.
    ///
    /// # Errors
    ///
    /// - `std::num::TryFromIntError` occurs when an object you are inserting
    /// takes up more space than `u32::MAX` bytes. In that case, seek help.
    ///
    /// - The usual `std::io::Error` such as `ErrorKind::UnexpectedEof` if the file
    /// that is being accessed is malformed and there are no more bytes to be read
    /// when the method is expecting more data.
    pub fn write(&mut self, data: &T) -> Result<(), DatabaseError<T>> {
        let buf = vec_to_binary(std::array::from_ref(data))?;
        self.file.write_all(&buf)?;
        Ok(())
    }
}

fn binary_to_vec<T: DeserializeOwned>(mut raw_data: &[u8]) -> Result<Vec<T>, DatabaseError<T>> {
    validate_collection(&mut raw_data)?;
    let mut is_corrupted = None;
    let mut result = Vec::new();
    loop {
        let raw_doc = process_document(&mut raw_data);
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

fn validate_collection<R: Read, T>(f: &mut R) -> Result<(), DatabaseError<T>> {
    let saved_file_header = f
        .read_u32::<LittleEndian>()
        .map_err(|_| DatabaseError::FileHeader)?;
    if saved_file_header != FILE_HEADER {
        return Err(DatabaseError::FileHeader);
    }
    let saved_file_version = f
        .read_u32::<LittleEndian>()
        .map_err(|_| DatabaseError::FileVersion(1))?;
    if saved_file_version != FILE_VERSION {
        return Err(DatabaseError::FileVersion(saved_file_version));
    }
    Ok(())
}

fn vec_to_binary<T: Serialize>(data: &[T]) -> Result<Vec<u8>, DatabaseError<T>> {
    let mut buf = Vec::new();
    buf.write_u32::<LittleEndian>(FILE_HEADER)?;
    buf.write_u32::<LittleEndian>(FILE_VERSION)?;
    for document in data {
        let raw_data = bincode::serialize(&document)?;
        let data_len = raw_data.len();
        let checksum = CRC.checksum(&raw_data);
        buf.write_u32::<LittleEndian>(checksum)?;
        buf.write_u32::<LittleEndian>(u32::try_from(data_len)?)?;
        buf.write_all(&raw_data)?;
    }
    Ok(buf)
}

/// This error occurs due to a checksum mismatch. Therefore it is important to backup your
/// files periodically to prevent data loss. However, you can still get the underlying
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
    use crate::{binary_to_vec, vec_to_binary, Checksum, DataPoisonError};
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
        let binary = vec_to_binary(&test_messages).unwrap();
        let vec: Vec<Test> = binary_to_vec(&binary).unwrap();
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
