use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::mem::size_of;
use std::path::{Path, PathBuf};

use byteorder::{BE, ReadBytesExt, WriteBytesExt};
use log::debug;
use zstd::zstd_safe;

use crate::block::Block;
use crate::storage::{get_segment_path, read_expected_tag, skip_to_next_tag, Tag, TAG_LENGTH, write_tag};
use crate::{BlockNum, Datum, Error, SegmentId};

pub(crate) struct BlockInfo {
    pub min_bounds: Vec<Datum>,
    pub max_bounds: Vec<Datum>,
    block_pos: u64
}

pub struct Segment {
    pub id: SegmentId,
    pub path: PathBuf,
    pub(crate) block_info: Vec<BlockInfo>
}

impl Segment {
    /**
     * Create a new segment, and save the given blocks to it.
     */
    pub(crate) fn create(
        database_path: &Path,
        seg_id: SegmentId,
        blocks: &[&Block]
    ) -> Result<Segment, Error> {
        let path = get_segment_path(database_path, seg_id, false);

        let mut segment = Segment {
            id: seg_id,
            path,
            block_info: Vec::new()
        };

        segment.save(blocks)?;

        Ok(segment)
    }

    pub(crate) fn load(
        database_path: &Path,
        seg_id: SegmentId
    ) -> Result<Segment, Error> {
        let mut path = get_segment_path(database_path, seg_id, true);
        if !path.exists() {
            path = get_segment_path(database_path, seg_id, false);
        }

        let mut segment = Segment {
            id: seg_id,
            path: path.clone(),
            block_info: Vec::new()
        };

        let file = File::open(path)?;
        let mut src = BufReader::with_capacity(zstd_safe::DCtx::in_size(), file);

        /* Seek to the end and read the end tag and the offset of the segment info */
        const END_SIZE: i64 = TAG_LENGTH as i64 + size_of::<u64>() as i64;
        src.seek(SeekFrom::End(-END_SIZE))?;
        read_expected_tag(&mut src, Tag::End)?;
        let segment_info_pos = src.read_u64::<BE>()?;

        /* Load the segment info */
        src.seek(SeekFrom::Start(segment_info_pos))?;
        read_expected_tag(&mut src, Tag::Segment)?;
        segment.load_segment_info(&mut src)?;

        Ok(segment)
    }

    pub(crate) fn load_one_block(&self, block_num: BlockNum) -> Result<Block, Error> {
        let file = File::open(&self.path)?;
        let mut src = BufReader::with_capacity(zstd_safe::DCtx::in_size(), file);

        src.seek(SeekFrom::Start(self.block_info[block_num as usize].block_pos))?;
        read_expected_tag(&mut src, Tag::Block)?;

        let block = self.load_block(&mut src)?;

        Ok(block)
    }

    fn load_block(&self, src: &mut BufReader<File>) -> Result<Block, Error> {
        let mut block = Block::new(0);

        let mut decoder = zstd::stream::read::Decoder::with_buffer(src)?;
        block.load(&mut decoder)?;
        let src = decoder.finish();

        /* ZStd leaves the last byte of a stream in the buffer, meaning we cant just read any other
           data after it.  This seems to be the "hostage byte" in the decompressor:
           https://github.com/facebook/zstd/blob/dev/lib/decompress/zstd_decompress.c#L2238
           To work around it, we scan for something that looks like a tag.  If there is only
           ever one byte to skip over, we should be able to do this unambiguously.  If not...?
         */
        skip_to_next_tag(src)?;

        Ok(block)
    }

    fn load_segment_info<R: BufRead>(&mut self, src: R) -> Result<(), Error> {
        let mut decoder = zstd::stream::read::Decoder::with_buffer(src)?;

        self.block_info.clear();

        let num_blocks = decoder.read_u16::<BE>()?;
        self.block_info.reserve_exact(num_blocks as usize);
        let num_dims = decoder.read_u16::<BE>()?;
        for _ in 0..num_blocks {
            let mut min_bounds = Vec::new();
            for _ in 0..num_dims {
                let val = decoder.read_u64::<BE>()? as Datum;
                min_bounds.push(val);
            }
            let mut max_bounds = Vec::new();
            for _ in 0..num_dims {
                let val = decoder.read_u64::<BE>()? as Datum;
                max_bounds.push(val);
            }
            let block_pos = decoder.read_u64::<BE>()?;
            let block_info = BlockInfo { min_bounds, max_bounds, block_pos };
            self.block_info.push(block_info);
        }

        decoder.finish();
        Ok(())
    }

    fn save(&mut self, blocks: &[&Block]) -> Result<(), Error> {
        let mut file = File::create(&self.path)?;

        for &block in blocks.iter() {
            let block_pos = file.stream_position()?;
            write_tag(&mut file, Tag::Block)?;
            self.save_block(&mut file, block)?;
            let block_info = BlockInfo {
                min_bounds: block.get_min_bounds(),
                max_bounds: block.get_max_bounds(),
                block_pos
            };
            self.block_info.push(block_info);
        }

        let segment_info_pos = file.stream_position()?;
        write_tag(&mut file, Tag::Segment)?;
        self.save_segment_info(&mut file)?;

        write_tag(&mut file, Tag::End)?;
        file.write_u64::<BE>(segment_info_pos)?;

        debug!("Wrote segment file {:?}", self.path);

        Ok(())
    }

    fn save_block(&self, file: &mut File, block: &Block) -> Result<(), Error> {
        let mut encoder = zstd::stream::write::Encoder::new(file, 1)?;
        block.save(&mut encoder)?;
        encoder.finish()?;

        Ok(())
    }

    fn save_segment_info(&self, file: &mut File) -> Result<(), Error> {
        let mut encoder = zstd::stream::write::Encoder::new(file, 1)?;

        let num_dims = self.block_info[0].min_bounds.len() as u16;

        encoder.write_u16::<BE>(self.block_info.len() as u16)?;
        encoder.write_u16::<BE>(num_dims)?;
        for bi in &self.block_info {
            for dim_val in &bi.min_bounds {
                encoder.write_u64::<BE>(*dim_val as u64)?;
            }
            for dim_val in &bi.max_bounds {
                encoder.write_u64::<BE>(*dim_val as u64)?;
            }
            encoder.write_u64::<BE>(bi.block_pos)?;
        }

        encoder.finish()?;

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
