use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use log::{debug};
use zstd::zstd_safe;

use crate::block::Block;
use crate::storage::{get_segment_path, read_tag, skip_to_next_tag, write_tag};
use crate::storage::Tag::{BlockTag, EndTag};
use crate::{BlockNum, Error, SegmentId};
use crate::schema::Schema;

pub struct Segment {
    pub id: SegmentId,
    pub path: PathBuf,
    pub num_blocks: u16
}

impl Segment {
    /**
     * Create a new segment, and save the given blocks to it.
     */
    pub(crate) fn create(
        database_path: &Path,
        seg_id: SegmentId,
        blocks: &Vec<&Block>
    ) -> Result<Segment, Error> {
        let path = get_segment_path(database_path, seg_id, false);

        let mut segment = Segment {
            id: seg_id,
            path,
            num_blocks: blocks.len() as u16
        };

        segment.save(blocks)?;

        Ok(segment)
    }

    pub(crate) fn load(
        database_path: &Path,
        seg_id: SegmentId,
        schema: &Schema
    ) -> Result<Segment, Error> {
        let mut path = get_segment_path(database_path, seg_id, true);
        if !path.exists() {
            path = get_segment_path(database_path, seg_id, false);
        }

        let mut segment = Segment {
            id: seg_id,
            path,
            num_blocks: 0
        };

        segment.load_into(schema)?;

        Ok(segment)
    }

    pub(crate) fn load_one_block(
        database_path: &Path,
        seg_id: SegmentId,
        schema: &Schema,
        block_num: BlockNum
    ) -> Result<Block, Error> {
        let mut path = get_segment_path(database_path, seg_id, true);
        if !path.exists() {
            path = get_segment_path(database_path, seg_id, false);
        }

        let mut segment = Segment {
            id: seg_id,
            path,
            num_blocks: 0
        };

        let mut blocks = segment.load_into(schema)?;
        let block = blocks.remove(block_num as usize);

        Ok(block)
    }

    fn load_into(&mut self, schema: &Schema) -> Result<Vec<Block>, Error> {
        let file = File::open(&self.path)?;
        let mut src = BufReader::with_capacity(zstd_safe::DCtx::in_size(), file);

        let mut blocks = Vec::new();
        loop {
            let tag = read_tag(&mut src);

            match tag {
                BlockTag => {
                    let block = self.load_block(&mut src, schema)?;
                    blocks.push(block);
                },
                EndTag => break
            }
        }

        self.num_blocks = blocks.len() as u16;

        debug!("Read segment file {:?}", self.path);

        Ok(blocks)
    }

    fn load_block(&mut self, src: &mut BufReader<File>, schema: &Schema) -> Result<Block, Error> {
        let mut block = Block::new(0);

        block.load(src)?;

        /* ZStd leaves the last byte of a stream in the buffer, meaning we cant just read any other
           data after it.  This seems to be the "hostage byte" in the decompressor:
           https://github.com/facebook/zstd/blob/dev/lib/decompress/zstd_decompress.c#L2238
           To work around it, we scan for something that looks like a tag.  If there is only
           ever one byte to skip over, we should be able to do this unambiguously.  If not...?
         */
        skip_to_next_tag(src)?;

        Ok(block)
    }

    fn save(&mut self, blocks: &Vec<&Block>) -> Result<(), Error> {
        let mut file = File::create(&self.path)?;

        for &block in blocks {
            write_tag(&mut file, BlockTag)?;
            block.save(&mut file)?;
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

    pub(crate) fn delete(&self) -> Result<(), Error> {
        std::fs::remove_file(&self.path)?;
        Ok(())
    }
}
