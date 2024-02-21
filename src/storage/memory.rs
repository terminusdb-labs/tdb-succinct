use std::{
    pin::Pin,
    sync::{Arc, RwLock},
    task::{Context, Poll},
};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use tokio::io::{self, AsyncRead, AsyncWrite, ReadBuf};

use super::types::{FileLoad, FileStore, SyncableFile};

enum MemoryBackedStoreContents {
    Nonexistent,
    Existent(Bytes),
}

#[derive(Clone)]
pub struct MemoryBackedStore {
    contents: Arc<RwLock<MemoryBackedStoreContents>>,
}

impl MemoryBackedStore {
    pub fn new() -> Self {
        Self {
            contents: Arc::new(RwLock::new(MemoryBackedStoreContents::Nonexistent)),
        }
    }
}

pub struct MemoryBackedStoreWriter {
    file: MemoryBackedStore,
    bytes: BytesMut,
}

#[async_trait]
impl SyncableFile for MemoryBackedStoreWriter {
    async fn sync_all(self) -> io::Result<()> {
        let mut contents = self.file.contents.write().unwrap();
        *contents = MemoryBackedStoreContents::Existent(self.bytes.freeze());

        Ok(())
    }
}

impl std::io::Write for MemoryBackedStoreWriter {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        self.bytes.extend_from_slice(buf);

        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }
}

impl AsyncWrite for MemoryBackedStoreWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Poll::Ready(std::io::Write::write(self.get_mut(), buf))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), io::Error>> {
        Poll::Ready(std::io::Write::flush(self.get_mut()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), io::Error>> {
        self.poll_flush(cx)
    }
}

#[async_trait]
impl FileStore for MemoryBackedStore {
    type Write = MemoryBackedStoreWriter;

    async fn open_write(&self) -> io::Result<Self::Write> {
        Ok(MemoryBackedStoreWriter {
            file: self.clone(),
            bytes: BytesMut::new(),
        })
    }
}

pub struct MemoryBackedStoreReader {
    bytes: Bytes,
    pos: usize,
}

impl std::io::Read for MemoryBackedStoreReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        if self.bytes.len() == self.pos {
            // end of file
            Ok(0)
        } else if self.bytes.len() < self.pos + buf.len() {
            // read up to end
            let len = self.bytes.len() - self.pos;
            buf[..len].copy_from_slice(&self.bytes[self.pos..]);

            self.pos += len;

            Ok(len)
        } else {
            // read full buf
            buf.copy_from_slice(&self.bytes[self.pos..self.pos + buf.len()]);

            self.pos += buf.len();

            Ok(buf.len())
        }
    }
}

impl AsyncRead for MemoryBackedStoreReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<Result<(), io::Error>> {
        let slice = buf.initialize_unfilled();
        let count = std::io::Read::read(self.get_mut(), slice);
        if count.is_ok() {
            buf.advance(*count.as_ref().unwrap());
        }

        Poll::Ready(count.map(|_| ()))
    }
}

#[async_trait]
impl FileLoad for MemoryBackedStore {
    type Read = MemoryBackedStoreReader;

    async fn exists(&self) -> io::Result<bool> {
        match &*self.contents.read().unwrap() {
            MemoryBackedStoreContents::Nonexistent => Ok(false),
            _ => Ok(true),
        }
    }

    async fn size(&self) -> io::Result<usize> {
        match &*self.contents.read().unwrap() {
            MemoryBackedStoreContents::Nonexistent => {
                panic!("tried to retrieve size of nonexistent memory file")
            }
            MemoryBackedStoreContents::Existent(bytes) => Ok(bytes.len()),
        }
    }

    async fn open_read_from(&self, offset: usize) -> io::Result<MemoryBackedStoreReader> {
        match &*self.contents.read().unwrap() {
            MemoryBackedStoreContents::Nonexistent => {
                panic!("tried to open nonexistent memory file for reading")
            }
            MemoryBackedStoreContents::Existent(bytes) => Ok(MemoryBackedStoreReader {
                bytes: bytes.clone(),
                pos: offset,
            }),
        }
    }

    async fn map(&self) -> io::Result<Bytes> {
        match &*self.contents.read().unwrap() {
            MemoryBackedStoreContents::Nonexistent => Err(io::Error::new(
                io::ErrorKind::NotFound,
                "tried to open a nonexistent memory file for reading",
            )),
            MemoryBackedStoreContents::Existent(bytes) => Ok(bytes.clone()),
        }
    }
}
