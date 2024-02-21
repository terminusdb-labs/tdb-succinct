use async_trait::async_trait;
use bytes::{Buf, Bytes};
use tokio::io::{self, AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::{AdjacencyList, BitIndex};

#[async_trait]
pub trait SyncableFile: AsyncWrite + Unpin + Send {
    async fn sync_all(self) -> io::Result<()>;
}

#[async_trait]
pub trait FileStore: Clone + Send + Sync {
    type Write: SyncableFile;
    async fn open_write(&self) -> io::Result<Self::Write>;
}

#[async_trait]
pub trait FileLoad: Clone + Send + Sync {
    type Read: AsyncRead + Unpin + Send;

    async fn exists(&self) -> io::Result<bool>;
    async fn size(&self) -> io::Result<usize>;
    async fn open_read(&self) -> io::Result<Self::Read> {
        self.open_read_from(0).await
    }
    async fn open_read_from(&self, offset: usize) -> io::Result<Self::Read>;
    async fn map(&self) -> io::Result<Bytes>;

    async fn map_if_exists(&self) -> io::Result<Option<Bytes>> {
        match self.exists().await? {
            false => Ok(None),
            true => {
                let mapped = self.map().await?;
                Ok(Some(mapped))
            }
        }
    }
}

#[derive(Clone)]
pub struct TypedDictionaryMaps {
    pub types_present_map: Bytes,
    pub type_offsets_map: Bytes,
    pub blocks_map: Bytes,
    pub offsets_map: Bytes,
}

#[derive(Clone)]
pub struct TypedDictionaryFiles<F: 'static + FileLoad + FileStore> {
    pub types_present_file: F,
    pub type_offsets_file: F,
    pub blocks_file: F,
    pub offsets_file: F,
}

impl<F: 'static + FileLoad + FileStore> TypedDictionaryFiles<F> {
    pub async fn map_all(&self) -> io::Result<TypedDictionaryMaps> {
        let types_present_map = self.types_present_file.map().await?;
        let type_offsets_map = self.type_offsets_file.map().await?;
        let offsets_map = self.offsets_file.map().await?;
        let blocks_map = self.blocks_file.map().await?;

        Ok(TypedDictionaryMaps {
            types_present_map,
            type_offsets_map,
            offsets_map,
            blocks_map,
        })
    }

    pub async fn write_all_from_bufs<B1: Buf, B2: Buf, B3: Buf, B4: Buf>(
        &self,
        types_present_buf: &mut B1,
        type_offsets_buf: &mut B2,
        offsets_buf: &mut B3,
        blocks_buf: &mut B4,
    ) -> io::Result<()> {
        let mut types_present_writer = self.types_present_file.open_write().await?;
        let mut type_offsets_writer = self.type_offsets_file.open_write().await?;
        let mut offsets_writer = self.offsets_file.open_write().await?;
        let mut blocks_writer = self.blocks_file.open_write().await?;

        types_present_writer
            .write_all_buf(types_present_buf)
            .await?;
        type_offsets_writer.write_all_buf(type_offsets_buf).await?;
        offsets_writer.write_all_buf(offsets_buf).await?;
        blocks_writer.write_all_buf(blocks_buf).await?;

        types_present_writer.flush().await?;
        types_present_writer.sync_all().await?;

        type_offsets_writer.flush().await?;
        type_offsets_writer.sync_all().await?;

        offsets_writer.flush().await?;
        offsets_writer.sync_all().await?;

        blocks_writer.flush().await?;
        blocks_writer.sync_all().await?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct DictionaryMaps {
    pub blocks_map: Bytes,
    pub offsets_map: Bytes,
}

#[derive(Clone)]
pub struct DictionaryFiles<F: 'static + FileLoad + FileStore> {
    pub blocks_file: F,
    pub offsets_file: F,
    //    pub map_files: Option<BitIndexFiles<F>>
}

impl<F: 'static + FileLoad + FileStore> DictionaryFiles<F> {
    pub async fn map_all(&self) -> io::Result<DictionaryMaps> {
        let offsets_map = self.offsets_file.map().await?;
        let blocks_map = self.blocks_file.map().await?;

        Ok(DictionaryMaps {
            offsets_map,
            blocks_map,
        })
    }

    pub async fn write_all_from_bufs<B1: Buf, B2: Buf>(
        &self,
        blocks_buf: &mut B1,
        offsets_buf: &mut B2,
    ) -> io::Result<()> {
        let mut offsets_writer = self.offsets_file.open_write().await?;
        let mut blocks_writer = self.blocks_file.open_write().await?;

        offsets_writer.write_all_buf(offsets_buf).await?;
        blocks_writer.write_all_buf(blocks_buf).await?;

        offsets_writer.flush().await?;
        offsets_writer.sync_all().await?;

        blocks_writer.flush().await?;
        blocks_writer.sync_all().await?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct IdMapMaps {
    pub node_value_idmap_maps: Option<BitIndexMaps>,
    pub predicate_idmap_maps: Option<BitIndexMaps>,
}

#[derive(Clone)]
pub struct IdMapFiles<F: 'static + FileLoad + FileStore> {
    pub node_value_idmap_files: BitIndexFiles<F>,
    pub predicate_idmap_files: BitIndexFiles<F>,
}

impl<F: 'static + FileLoad + FileStore> IdMapFiles<F> {
    pub async fn map_all(&self) -> io::Result<IdMapMaps> {
        let node_value_idmap_maps = self.node_value_idmap_files.map_all_if_exists().await?;
        let predicate_idmap_maps = self.predicate_idmap_files.map_all_if_exists().await?;

        Ok(IdMapMaps {
            node_value_idmap_maps,
            predicate_idmap_maps,
        })
    }
}

#[derive(Clone)]
pub struct BitIndexMaps {
    pub bits_map: Bytes,
    pub blocks_map: Bytes,
    pub sblocks_map: Bytes,
}

impl Into<BitIndex> for BitIndexMaps {
    fn into(self) -> BitIndex {
        BitIndex::from_maps(self.bits_map, self.blocks_map, self.sblocks_map)
    }
}

#[derive(Clone)]
pub struct BitIndexFiles<F: 'static + FileLoad> {
    pub bits_file: F,
    pub blocks_file: F,
    pub sblocks_file: F,
}

impl<F: 'static + FileLoad + FileStore> BitIndexFiles<F> {
    pub async fn map_all(&self) -> io::Result<BitIndexMaps> {
        let bits_map = self.bits_file.map().await?;
        let blocks_map = self.blocks_file.map().await?;
        let sblocks_map = self.sblocks_file.map().await?;

        Ok(BitIndexMaps {
            bits_map,
            blocks_map,
            sblocks_map,
        })
    }

    pub async fn map_all_if_exists(&self) -> io::Result<Option<BitIndexMaps>> {
        if self.bits_file.exists().await? {
            Ok(Some(self.map_all().await?))
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone)]
pub struct AdjacencyListMaps {
    pub bitindex_maps: BitIndexMaps,
    pub nums_map: Bytes,
}

impl Into<AdjacencyList> for AdjacencyListMaps {
    fn into(self) -> AdjacencyList {
        AdjacencyList::parse(
            self.nums_map,
            self.bitindex_maps.bits_map,
            self.bitindex_maps.blocks_map,
            self.bitindex_maps.sblocks_map,
        )
    }
}

#[derive(Clone)]
pub struct AdjacencyListFiles<F: 'static + FileLoad> {
    pub bitindex_files: BitIndexFiles<F>,
    pub nums_file: F,
}

impl<F: 'static + FileLoad + FileStore> AdjacencyListFiles<F> {
    pub async fn map_all(&self) -> io::Result<AdjacencyListMaps> {
        let bitindex_maps = self.bitindex_files.map_all().await?;
        let nums_map = self.nums_file.map().await?;

        Ok(AdjacencyListMaps {
            bitindex_maps,
            nums_map,
        })
    }
}

pub async fn copy_file<F1: FileLoad, F2: FileStore>(f1: &F1, f2: &F2) -> io::Result<()> {
    if !f1.exists().await? {
        return Ok(());
    }
    let mut input = f1.open_read().await?;
    let mut output = f2.open_write().await?;

    tokio::io::copy(&mut input, &mut output).await?;
    output.flush().await?;
    output.sync_all().await?;

    Ok(())
}

impl<F1: 'static + FileLoad + FileStore> DictionaryFiles<F1> {
    pub async fn copy_from<F2: 'static + FileLoad + FileStore>(
        &self,
        from: &DictionaryFiles<F2>,
    ) -> io::Result<()> {
        copy_file(&from.blocks_file, &self.blocks_file).await?;
        copy_file(&from.offsets_file, &self.offsets_file).await?;

        Ok(())
    }
}

impl<F1: 'static + FileLoad + FileStore> TypedDictionaryFiles<F1> {
    pub async fn copy_from<F2: 'static + FileLoad + FileStore>(
        &self,
        from: &TypedDictionaryFiles<F2>,
    ) -> io::Result<()> {
        copy_file(&from.types_present_file, &self.types_present_file).await?;
        copy_file(&from.type_offsets_file, &self.type_offsets_file).await?;
        copy_file(&from.blocks_file, &self.blocks_file).await?;
        copy_file(&from.offsets_file, &self.offsets_file).await?;

        Ok(())
    }
}

impl<F1: 'static + FileLoad + FileStore> BitIndexFiles<F1> {
    pub async fn copy_from<F2: 'static + FileLoad + FileStore>(
        &self,
        from: &BitIndexFiles<F2>,
    ) -> io::Result<()> {
        copy_file(&from.bits_file, &self.bits_file).await?;
        copy_file(&from.blocks_file, &self.blocks_file).await?;
        copy_file(&from.sblocks_file, &self.sblocks_file).await?;

        Ok(())
    }
}
impl<F1: 'static + FileLoad + FileStore> AdjacencyListFiles<F1> {
    pub async fn copy_from<F2: 'static + FileLoad + FileStore>(
        &self,
        from: &AdjacencyListFiles<F2>,
    ) -> io::Result<()> {
        copy_file(&from.nums_file, &self.nums_file).await?;
        self.bitindex_files.copy_from(&from.bitindex_files).await?;

        Ok(())
    }
}

impl<F1: 'static + FileLoad + FileStore> IdMapFiles<F1> {
    pub async fn copy_from<F2: 'static + FileLoad + FileStore>(
        &self,
        from: &IdMapFiles<F2>,
    ) -> io::Result<()> {
        self.node_value_idmap_files
            .copy_from(&from.node_value_idmap_files)
            .await?;
        self.predicate_idmap_files
            .copy_from(&from.predicate_idmap_files)
            .await?;

        Ok(())
    }
}
