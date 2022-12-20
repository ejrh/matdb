use std::fs::File;
use std::io::{BufRead, BufReader, ErrorKind, Read, Seek, Write};
use std::path::{Path, PathBuf};

use crate::{SegmentId, SegmentNum, TransactionId};

const TAG_PREFIX: &[u8] = "MD:".as_bytes();
const TAG_PREFIX_LENGTH: usize = TAG_PREFIX.len();
const TAG_LENGTH: usize = 6;

pub enum Tag {
    BlockTag,
    EndTag
}

pub const SCHEMA_FILENAME: &str = "schema.json";

pub fn check_for_prefix<F>(reader: &mut BufReader<F>) -> std::io::Result<bool>
where F: Read + Seek
{
    let mut buffer:[u8; TAG_PREFIX_LENGTH] = [0; TAG_PREFIX_LENGTH];
    reader.read_exact(&mut buffer)?;

    reader.seek_relative(-(TAG_PREFIX_LENGTH as i64))?;

    Ok(buffer.eq(TAG_PREFIX))
}

pub fn skip_to_next_tag<F>(reader: &mut BufReader<F>) -> std::io::Result<()>
where F: Read + Seek
{
    if check_for_prefix(reader)? {
        return Ok(());
    }

    reader.seek_relative(1)?;

    if !check_for_prefix(reader)? {
        return Err(std::io::Error::new(ErrorKind::InvalidInput, "Couldn't find tag"));
    }

    Ok(())
}

pub fn read_tag<R>(reader: &mut R) -> Tag
where R: BufRead
{
    let mut buffer:[u8; TAG_LENGTH] = [0; TAG_LENGTH];
    reader.read_exact(&mut buffer).expect("Insuffient data for tag");

    if buffer.eq("MD:BLK".as_bytes()) {
        Tag::BlockTag
    } else if buffer.eq("MD:END".as_bytes()) {
        Tag::EndTag
    } else {
        panic!("Unknown tag")
    }
}

pub fn write_tag(file: &mut File, tag: Tag) -> std::io::Result<()> {
    file.write_all(
        match tag {
            Tag::BlockTag => "MD:BLK".as_bytes(),
            Tag::EndTag => "MD:END".as_bytes()
        }
    )
}

pub fn get_segment_path(
    database_path: &Path,
    seg_id: SegmentId,
    visible: bool
) -> PathBuf {
    let segment_filename = if visible {
        format!("{:08x}.{:08x}", seg_id.0, seg_id.1)
    } else {
        format!("{:08x}.{:08x}.tmp", seg_id.0, seg_id.1)
    };
    database_path.join(segment_filename)
}

pub fn decode_segment_path(path: &Path) -> Option<(TransactionId, SegmentNum, bool)> {
    let filename = path.file_name()?.to_str()?;
    let mut parts = filename.split('.');
    let txn_id: TransactionId = TransactionId::from_str_radix(parts.next()?, 16).ok()?;
    let seg_num: SegmentNum = SegmentNum::from_str_radix(parts.next()?, 16).ok()?;
    let tail = parts.next();
    let committed = match tail {
        None => true,
        Some("tmp") => false,
        _ => { return None; }
    };
    Some((txn_id, seg_num, committed))
}

#[cfg(test)]
mod storage_tests {
    use super::*;

    #[test]
    fn decode() {
        let (txn_id, seg_num, committed) = decode_segment_path(Path::new("/a long path/of.some.kind/000A0000.0001")).unwrap();
        assert_eq!(txn_id, 655360);
        assert_eq!(seg_num, 1);
        assert!(committed);

        let (txn_id, seg_num, committed) = decode_segment_path(Path::new("10000000.FFFE.tmp")).unwrap();
        assert_eq!(txn_id, 268435456);
        assert_eq!(seg_num, 65534);
        assert!(!committed);

        assert!(decode_segment_path(Path::new("bogusfilename")).is_none());
    }
}
