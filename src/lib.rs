use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::{Crc, CRC_32_ISO_HDLC};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    borrow::Borrow,
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

type ByteString = Vec<u8>;
type ByteStr = [u8];

pub const ALGORITHM: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

pub struct Client<T> {
    f: File,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> Client<T> {
    pub fn open(database: &str) -> io::Result<Self> {
        let mut path = PathBuf::new();
        path.push(database);
        path.set_extension("cdb");
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self {
            f,
            _phantom: Default::default(),
        })
    }

    fn insert_inner(&mut self, raw_data: &ByteStr) -> io::Result<()> {
        let mut f = BufWriter::new(&mut self.f);
        let data_len = raw_data.len();
        let checksum = ALGORITHM.checksum(&raw_data);
        f.seek(SeekFrom::End(0))?;
        f.write_u32::<LittleEndian>(checksum)?;
        f.write_u32::<LittleEndian>(data_len as u32)?;
        f.write_all(&raw_data)?;
        Ok(())
    }

    fn delete_inner<R: Read>(f: &mut R) -> io::Result<()> {
        todo!()
    }

    fn process_document<R: Read>(f: &mut R) -> io::Result<ByteString> {
        let saved_checksum = f.read_u32::<LittleEndian>()?;
        let raw_data_len = f.read_u32::<LittleEndian>()?;
        let mut raw_data = ByteString::with_capacity(raw_data_len as usize);
        {
            f.by_ref()
                .take(raw_data_len as u64)
                .read_to_end(&mut raw_data)?;
        }
        let checksum = ALGORITHM.checksum(&raw_data);
        if checksum != saved_checksum {
            panic!(
                "data corruption encountered ({:08x} != {:08x})",
                checksum, saved_checksum
            );
        }
        Ok(raw_data)
    }
}

impl<T> Client<T>
where
    T: Serialize + DeserializeOwned,
{
    pub fn insert(&mut self, item: impl Borrow<T>) -> io::Result<()> {
        let encoded = bincode::serialize(item.borrow()).unwrap();
        self.insert_inner(&encoded)?;
        Ok(())
    }

    pub fn get<F>(&mut self, filter: F) -> io::Result<Vec<T>>
    where
        F: Fn(&T) -> bool,
    {
        let mut result = Vec::new();
        let mut f = BufReader::new(&mut self.f);
        loop {
            f.seek(SeekFrom::Start(0))?;
            let raw_data = Self::process_document(&mut f);
            let raw_data = match raw_data {
                Ok(d) => d,
                Err(e) => match e.kind() {
                    io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    _ => return Err(e),
                },
            };
            let data = bincode::deserialize(&raw_data).unwrap();
            if filter(&data) {
                result.push(data);
            }
        }
        Ok(result)
    }

    pub fn delete<F>(&mut self, filter: F) -> io::Result<()>
    where
        F: Fn(&T) -> bool,
    {
        let mut f = BufReader::new(&mut self.f);
        loop {
            f.seek(SeekFrom::Start(0))?;
            let raw_data = Self::process_document(&mut f);
            let raw_data = match raw_data {
                Ok(d) => d,
                Err(e) => match e.kind() {
                    io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    _ => return Err(e),
                },
            };
            let data: T = bincode::deserialize(&raw_data).unwrap();
            if filter(&data) {
                Self::delete_inner(&mut f)?;
            }
        }
        Ok(())
    }

    pub fn update<F, M>(&mut self, filter: F, map: M) -> io::Result<()>
    where
        F: Fn(&T) -> bool,
        M: FnOnce(T) -> T,
    {
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
