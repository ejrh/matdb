use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use log::debug;
use zstd::zstd_safe;

use crate::block::Block;
use crate::storage::{get_segment_path, read_tag, skip_to_next_tag, write_tag};
use crate::storage::Tag::{BlockTag, EndTag};
use crate::{BlockKey, Error, SegmentId};
use crate::schema::Schema;

pub struct Segment {
    pub id: SegmentId,
    pub path: PathBuf,
    pub(crate) cached_blocks: HashMap<BlockKey, Block>
}

impl Segment {
    /**
     * Create a new segment, and save the given blocks to it.
     */
    pub(crate) fn create(
        database_path: &Path,
        seg_id: SegmentId,
        blocks: HashMap<BlockKey, Block>
    ) -> Result<Segment, Error> {
        let path = get_segment_path(database_path, seg_id, false);

        let mut segment = Segment {
            id: seg_id,
            path,
            cached_blocks: blocks
        };

        segment.save()?;

        Ok(segment)
    }

    pub(crate) fn load(&mut self, schema: &Schema) -> Result<(), Error> {
        let file = File::open(&self.path)?;
        let mut src = BufReader::with_capacity(zstd_safe::DCtx::in_size(), file);

        loop {
            let tag = read_tag(&mut src);

            match tag {
                BlockTag => self.load_block(&mut src, schema)?,
                EndTag => break
            }
        }

        debug!("Read segment file {:?}", self.path);

        Ok(())
    }

    pub(crate) fn load_block(&mut self, src: &mut BufReader<File>, schema: &Schema) -> Result<(), Error> {
        let mut block = Block::new(0);

        block.load(src)?;

        /* Pick the first row and use it as the chunk for for the whole block */
        let first = block.iter().next();
        if first.is_none() { return Ok(()); }
        let key = schema.get_chunk_key(&first.unwrap());

        self.cached_blocks.insert(key, block);

        /* ZStd leaves the last byte of a stream in the buffer, meaning we cant just read any other
           data after it.  This seems to be the "hostage byte" in the decompressor:
           https://github.com/facebook/zstd/blob/dev/lib/decompress/zstd_decompress.c#L2238
           To work around it, we scan for something that looks like a tag.  If there is only
           ever one byte to skip over, we should be able to do this unambiguously.  If not...?
         */
        skip_to_next_tag(src)?;

        Ok(())
    }

    pub(crate) fn save(&mut self) -> Result<(), Error> {
        let mut file = File::create(&self.path)?;

        for buf in self.cached_blocks.values() {
            write_tag(&mut file, BlockTag)?;
            buf.save(&mut file)?;
        }

        write_tag(&mut file, EndTag)?;

        debug!("Wrote segment file {:?}", self.path);

        Ok(())
    }

    pub(crate) fn make_visible(&mut self, database_path: &Path) -> Result<(), Error> {
        let new_path = get_segment_path(database_path,self.id, true);
        std::fs::rename(self.path.as_path(), new_path.as_path())?;
        self.path = new_path;
        Ok(())
    }

    pub(crate) fn delete(self) -> Result<(), Error> {
        std::fs::remove_file(self.path)?;
        Ok(())
    }
}
