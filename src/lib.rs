use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc::{Crc, CRC_32_ISO_HDLC};
use std::{
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

type ByteString = Vec<u8>;
type ByteStr = [u8];

pub const ALGORITHM: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

pub struct Client {
    f: File,
    index: Vec<(ByteString, u64)>,
}

impl Client {
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
        let index = Vec::new();
        Ok(Self { f, index })
    }

    pub fn load(&mut self) -> io::Result<()> {
        let mut f = BufReader::new(&mut self.f);
        loop {
            let current_position = f.seek(SeekFrom::Current(0))?;
            let raw_data = Self::process_record(&mut f);
            let data = match raw_data {
                Ok(d) => d,
                Err(e) => match e.kind() {
                    io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    _ => return Err(e),
                },
            };
            self.index.push((data, current_position));
        }
        Ok(())
    }

    fn process_record<R: Read>(f: &mut R) -> io::Result<ByteString> {
        let saved_checksum = f.read_u32::<LittleEndian>()?;
        let data_len = f.read_f32::<LittleEndian>()?;
        let mut data = ByteString::with_capacity(data_len as usize);
        {
            f.by_ref().take(data_len as u64).read_to_end(&mut data)?;
        }
        let checksum = ALGORITHM.checksum(&data);
        if checksum != saved_checksum {
            panic!(
                "data corruption encountered ({:08x} != {:08x})",
                checksum, saved_checksum
            );
        }
        Ok(data)
    }

    pub fn insert(&mut self, data: &ByteStr) -> io::Result<()> {
        let position = self.insert_but_ignore_index(data)?;
        self.index.push((data.to_vec(), position));
        Ok(())
    }

    pub fn insert_but_ignore_index(&mut self, data: &ByteStr) -> io::Result<u64> {
        let mut f = BufWriter::new(&mut self.f);
        let data_len = data.len();
        let mut tmp = ByteString::with_capacity(data_len);
        for byte in data {
            tmp.push(*byte);
        }
        let checksum = ALGORITHM.checksum(&tmp);
        let next_byte = SeekFrom::End(0);
        let current_position = f.seek(SeekFrom::Current(0))?;
        f.seek(next_byte)?;
        f.write_u32::<LittleEndian>(checksum)?;
        f.write_u32::<LittleEndian>(data_len as u32)?;
        f.write_all(&tmp)?;
        Ok(current_position)
    }

    pub fn get(&mut self, index: usize) -> io::Result<Option<ByteString>> {
        let position = match self.index.get(index) {
            None => return Ok(None),
            Some((_, position)) => *position,
        };
        let data = self.get_at(position)?;
        Ok(Some(data))
    }

    pub fn get_at(&mut self, position: u64) -> io::Result<ByteString> {
        let mut f = BufReader::new(&mut self.f);
        f.seek(SeekFrom::Start(position))?;
        let data = Self::process_record(&mut f)?;
        Ok(data)
    }

    pub fn find(&mut self, target: &ByteStr) -> io::Result<Option<(u64, ByteString)>> {
        let mut f = BufReader::new(&mut self.f);
        let mut found: Option<(u64, ByteString)> = None;
        loop {
            let position = f.seek(SeekFrom::Current(0))?;
            let raw_data = Self::process_record(&mut f);
            let data = match raw_data {
                Ok(data) => data,
                Err(e) => match e.kind() {
                    io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    _ => return Err(e),
                },
            };
            if data == target {
                found = Some((position, data));
            }
        }
        Ok(found)
    }

    #[inline]
    pub fn update(&mut self, data: &ByteStr) -> io::Result<()> {
        self.insert(data)
    }

    #[inline]
    pub fn delete(&mut self, data: &ByteStr) -> io::Result<()> {
        self.insert(data)
    }
}

pub struct Collection<T> {
    index: T,
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
