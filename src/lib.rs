#![warn(clippy::pedantic)]

//! This crate provides an easy to use API to store persistent data of the same type.
//!
//! Any type that is able to be deserialized or serialized using Serde can be stored on disk.
//! Data is stored on disk with a CRC32 checksum associated with every object to ensure
//! data integrity.
//!
//! The client has two modes: append mode and overwrite mode (non-append mode). Append mode appends
//! data to the original file when any of the write methods are called while overwrite mode overwrites
//! data in the file with new data provided.
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
//! #[derive(Serialize, Deserialize)]
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

/// This is the main error type of this crate.
#[derive(Error, Debug)]
pub enum DatabaseError {
    /// This returns `std::io::Error`
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// This error occurs if the saved checksum does not match the expected checksum of the saved object.
    /// This is likely due to data corruption. Data backup is outside the scope of this crate,
    /// thus an external backup solution is recommended.
    #[error("data corruption encountered ({expected:08x} != {saved:08x})")]
    MismatchedChecksum { saved: u32, expected: u32 },
    /// `crio` can only store an object that takes up `u32::MAX` bytes of space. If you run
    /// into this error you should consider some other library.
    #[error("inserted data too large (object > u32::MAX)")]
    DataTooLarge(#[from] TryFromIntError),
    /// Serialization/deserialization error for an object.
    #[error(transparent)]
    SerdeError(#[from] bincode::Error),
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
    pub fn new<P: AsRef<Path>>(path: P, append: bool) -> Result<Self, DatabaseError> {
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
    pub fn load(&mut self) -> Result<Option<Vec<T>>, DatabaseError> {
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
    pub fn write_many(&mut self, data: &[T]) -> Result<(), DatabaseError> {
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
    pub fn write(&mut self, data: &T) -> Result<(), DatabaseError> {
        let buf = vec_to_binary(std::array::from_ref(data))?;
        self.file.write_all(&buf)?;
        Ok(())
    }
}

fn binary_to_vec<T: DeserializeOwned>(mut raw_data: &[u8]) -> Result<Vec<T>, DatabaseError> {
    let mut result = Vec::new();
    loop {
        let raw_doc = process_document(&mut raw_data);
        let raw_doc = match raw_doc {
            Ok(d) => d,
            Err(e) => match e {
                DatabaseError::Io(e) if e.kind() == ErrorKind::UnexpectedEof => {
                    break;
                }
                _ => return Err(e),
            },
        };
        let data = bincode::deserialize(&raw_doc)?;
        result.push(data);
    }
    Ok(result)
}

fn process_document<R: Read>(f: &mut R) -> Result<Vec<u8>, DatabaseError> {
    let saved_checksum = f.read_u32::<LittleEndian>()?;
    let data_len = f.read_u32::<LittleEndian>()?;
    let mut data = Vec::with_capacity(data_len as usize);
    f.take(u64::from(data_len)).read_to_end(&mut data)?;
    let expected_checksum = CRC.checksum(&data);
    if expected_checksum != saved_checksum {
        return Err(DatabaseError::MismatchedChecksum {
            saved: saved_checksum,
            expected: expected_checksum,
        });
    }
    Ok(data)
}

fn vec_to_binary<T: Serialize>(data: &[T]) -> Result<Vec<u8>, DatabaseError> {
    let mut buf = Vec::new();
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

#[cfg(test)]
mod tests {
    use crate::{binary_to_vec, vec_to_binary};
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
}
