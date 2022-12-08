use std::fs::File;
use std::io::{BufRead, BufReader, ErrorKind, Read, Seek, Write};

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
        return Tag::BlockTag;
    } else if buffer.eq("MD:END".as_bytes()) {
        return Tag::EndTag;
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
