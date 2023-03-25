use std::marker::PhantomData;
use std::path::PathBuf;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("API request error")]
    IOError(#[from] std::io::Error),

    #[error("Serialization error")]
    SerializationError(#[from] rmp_serde::encode::Error),

    #[error("Deserialization error")]
    DeserializationError(#[from] rmp_serde::decode::Error),
}

pub struct FileWriter<T> {
    filepath: PathBuf,
    writer: Option<BufWriter<File>>,
    buf: Vec<u8>,
    _p: PhantomData<T>,
}

impl<T: serde::Serialize> FileWriter<T> {
    pub fn new(filepath: PathBuf) -> Self {
        FileWriter {
            filepath,
            writer: None,
            buf: Vec::with_capacity(256),
            _p: PhantomData,
        }
    }

    pub async fn write(&mut self, val: &T) -> Result<(), Error> {
        if self.writer.is_none() {
            // Create the file
            let f = File::create(&self.filepath).await?;
            let writer = BufWriter::new(f);
            self.writer = Some(writer);
        }
        let writer = self.writer.as_mut().unwrap();

        val.serialize(&mut rmp_serde::Serializer::new(&mut self.buf))?;

        let size = self.buf.len() as u64;
        writer.write_all(&size.to_be_bytes()).await?;
        writer.write_all(&self.buf).await?;
        writer.write_all(&[b'\n']).await?;

        self.buf.clear();

        Ok(())
    }

    pub async fn close(self) -> Result<(), Error> {
        if let Some(mut writer) = self.writer {
            writer.flush().await?;
        }
        Ok(())
    }
}

pub struct FileReader<T> {
    filepath: PathBuf,
    reader: Option<BufReader<File>>,
    buf: Vec<u8>,
    eof: bool,
    _p: PhantomData<T>,
}

impl<T: for<'a> serde::Deserialize<'a>> FileReader<T> {
    pub fn new(filepath: PathBuf) -> Self {
        FileReader {
            filepath,
            reader: None,
            buf: vec![],
            eof: false,
            _p: PhantomData,
        }
    }

    /// Read the next item from the file. Returns None when the end of the file is reached.
    pub async fn next(&mut self) -> Result<Option<T>, Error> {
        if self.eof {
            return Ok(None);
        }
        if self.reader.is_none() {
            let f = File::open(&self.filepath).await?;
            let reader = BufReader::new(f);
            self.reader = Some(reader);
        }
        let reader = self.reader.as_mut().unwrap();

        // Get the size of the next item. Check if we've reached the end of the file.
        let res = reader.read_u64().await;
        if res.is_err() {
            let err = res.unwrap_err();
            if err.kind() == std::io::ErrorKind::UnexpectedEof {
                self.eof = true;
                return Ok(None);
            }
            return Err(Error::from(err));
        }
        let size = res.unwrap();

        reader.take(size).read_to_end(&mut self.buf).await?;
        let item: T = rmp_serde::from_slice(&self.buf)?;

        // Consume the newline
        reader.read_u8().await?;

        self.buf.clear();

        Ok(Some(item))
    }
}
