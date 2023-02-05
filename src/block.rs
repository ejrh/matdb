use std::fs::File;
use std::io::{BufRead, Read, Write};
use std::io;
use std::mem::size_of;
use std::rc::Rc;

use byteorder::{BE, ReadBytesExt, WriteBytesExt};
use crate::{Datum};

pub struct Block {
    pub(crate) dimension_values: Vec<Vec<Datum>>,
    pub(crate) values: Vec<Option<Datum>>,
}

#[derive(Debug)]
struct SliceInsertionParams {
    new_size: usize,
    moves: usize,
    len: usize,
    step: usize,
    offset: usize
}

#[derive(Clone)]
pub struct BlockIter {
    block: Rc<Block>,
    indexes: Vec<usize>,
    value_index: usize
}

impl Block {
    pub(crate) fn new(num_dimensions: usize) -> Self {
        Block {
            dimension_values: vec![Vec::new(); num_dimensions],
            values: Vec::new()
        }
    }

    pub(crate) fn add_row(&mut self, values: &[Datum]) {
        let mut dim_idxs = Vec::new();
        for dim_no in 0..self.dimension_values.len() {
            let dim_value = values[dim_no];
            let dim_idx = self.add_dimension_value(dim_no, dim_value);
            dim_idxs.push(dim_idx);
        }

        for value_no in self.dimension_values.len()..values.len() {
            let value = values[value_no];
            let idx = self.get_index(&dim_idxs);
            self.values[idx] = Some(value);
        }
    }

    fn get_index(&self, dim_indexes: &[usize]) -> usize {
        let mut idx = 0;

        let num_dims = self.dimension_values.len();

        for (i, x) in dim_indexes.iter().enumerate() {
            if i < num_dims - 1 {
                let scale = self.dimension_values[i + 1].len();
                idx += scale * x;
            } else { idx += x; }
        }

        idx
    }

    fn add_dimension_value(&mut self, dim_no: usize, value: Datum) -> usize {
        match self.dimension_values[dim_no].binary_search(&value) {
            Ok(idx) => idx,
            Err(idx) => {
                self.insert_slice(dim_no, idx);
                self.dimension_values[dim_no].insert(idx, value);
                idx
            }
        }
    }

    fn insert_slice(&mut self, dim_no: usize, idx: usize) {
        let params = self.get_slice_insertion_params(dim_no, idx);

        self.values.resize(params.new_size, None);

        for i in (0..params.moves).rev() {
            let from_offset = i * params.len + params.offset;
            let to_offset = from_offset + (i + 1) * params.step;
            self.copy_elements(from_offset, to_offset, params.len);
            self.clear_elements(from_offset, to_offset);
        }
    }

    fn copy_elements(&mut self, from_idx: usize, to_idx: usize, num: usize) {
        let mut num = num;
        if to_idx + num > self.values.len() {
            num = self.values.len() - to_idx;
        }
        let value_slice = self.values.as_mut_slice();
        value_slice.copy_within(from_idx..from_idx+num, to_idx);
    }

    fn clear_elements(&mut self, from_idx: usize, to_idx: usize) {
        for i in from_idx..to_idx {
            self.values[i] = None;
        }
    }

    fn get_slice_insertion_params(&self, dim_no: usize, index: usize) -> SliceInsertionParams {
        let sizes: Vec<usize> = self.dimension_values.iter().map(|x| x.len()).collect();
        let mut num_moves = 1;
        let mut move_step = 1;

        for size in sizes.iter().take(dim_no) {
            num_moves *= size;
        }

        for size in sizes.iter().skip(dim_no + 1) {
            move_step *= size;
        }

        let move_size = sizes[dim_no] * move_step;
        let new_size = num_moves * (sizes[dim_no] + 1) * move_step;
        let current_size = num_moves * move_size;

        let move_offset = move_step * index;

        if move_size == 0 || move_offset >= current_size { num_moves = 0; }

        SliceInsertionParams {
            new_size,
            moves: num_moves,
            len: move_size,
            step: move_step,
            offset: move_offset
        }
    }

    pub(crate) fn load<R: BufRead>(&mut self, src: &mut R) -> io::Result<()> {
        let mut decoder = zstd::stream::read::Decoder::with_buffer(src)?;

        let mut num_values = 1;

        /* Read the dimensions */
        let num_dimensions = decoder.read_u16::<BE>()?;
        self.dimension_values.clear();
        for _ in 0..num_dimensions {
            let mut dim_vals: Vec<Datum> = Vec::new();
            let dim_size = decoder.read_u32::<BE>()? as usize;
            for _ in 0..dim_size {
                let dim_idx = decoder.read_u64::<BE>()?;
                dim_vals.push(dim_idx as Datum);
            }
            self.dimension_values.push(dim_vals);
            num_values *= dim_size;
        }

        /* Read the values */
        self.values.clear();
        self.values.reserve(num_values);

        let mut missing_bytes: Vec<u8> = vec![1; num_values];
        decoder.read_exact(&mut missing_bytes)?;

        for &missing in &missing_bytes {
            if missing == 1 {
                self.values.push(None);
            } else {
                let val = decoder.read_u64::<BE>()? as Datum;
                self.values.push(Some(val));
            }
        }

        decoder.finish();

        Ok(())
    }

    pub(crate) fn save(&self, file: &mut File) -> io::Result<()> {
        let mut encoder = zstd::stream::write::Encoder::new(file, 1)?;

        /* Write the dimensions */
        encoder.write_u16::<BE>(self.dimension_values.len() as u16)?;
        for dim in &self.dimension_values {
            encoder.write_u32::<BE>(dim.len() as u32)?;
            for &dim_val in dim {
                encoder.write_u64::<BE>(dim_val as u64)?;
            }
        }

        /* Write the values */
        let mut missing_bytes: Vec<u8> = Vec::new();
        let mut values_bytes: Vec<u8> = Vec::new();

        for &val in &self.values {
            if let Some(value) = val {
                missing_bytes.push(0);
                values_bytes.extend(usize::to_be_bytes(value));
            } else {
                missing_bytes.push(1);
            }
        }

        encoder.write_all(missing_bytes.as_slice())?;
        encoder.write_all(values_bytes.as_slice())?;

        encoder.finish()?;

        Ok(())
    }

    pub(crate) fn get_start_point(&self) -> Option<Vec<Datum>> {
        let mut point = Vec::with_capacity(self.dimension_values.len());
        for dimvals in &self.dimension_values {
            if dimvals.is_empty() { return None; }
            point.push(dimvals[0]);
        }
        Some(point)
    }

    pub(crate) fn iter(this: &Rc<Self>) -> BlockIter {
        BlockIter {
            block: this.clone(),
            indexes: vec![0; this.dimension_values.len()],
            value_index: 0
        }
    }
}

impl BlockIter {
    fn increment_indexes(&mut self) {
        self.value_index += 1;
        let mut incr_pos = self.indexes.len() - 1;
        loop {
            self.indexes[incr_pos] += 1;
            if self.indexes[incr_pos] >= self.block.dimension_values[incr_pos].len() {
                if incr_pos == 0 { break; }
                self.indexes[incr_pos] = 0;
                incr_pos -= 1;
                continue;
            }
            break;
        }
    }
}

impl Iterator for BlockIter {
    type Item = Vec<Datum>;

    fn next(&mut self) -> Option<Vec<Datum>>
    {
        loop {
            // Check if indexes are already past the size of the block
            if self.indexes[0] >= self.block.dimension_values[0].len() {
                return None;
            }

            // Turn this index into a single number and get the result
            //let calculated_idx = self.block.get_index(&self.indexes);
            //assert_eq!(self.value_index, calculated_idx);
            let value: Option<Datum> = self.block.values[self.value_index];

            // If it's empty, increment and try the next one
            if value.is_none() {
                self.increment_indexes();
                continue;
            }

            let value = value.unwrap();
            let mut va = Vec::new();
            for i in 0..self.indexes.len() {
                va.push(self.block.dimension_values[i][self.indexes[i]]);
            }
            va.push(value);

            // Move to to the next index and return the row
            self.increment_indexes();
            return Some(va);
        }
    }
}

#[cfg(test)]
mod get_slice_insertion_params_tests {
    use super::Block;

    #[test]
    fn one_dimension() {
        // One empty dimension, can only insert in one place
        let mut b = Block::new(1);
        let params = b.get_slice_insertion_params(0, 0);
        assert_eq!(params.new_size, 1);
        assert_eq!(params.moves, 0);

        // Add a single element
        b.dimension_values[0].push(0);

        // Inserting before it should move it
        let params = b.get_slice_insertion_params(0, 0);
        assert_eq!(params.new_size, 2);
        assert_eq!(params.moves, 1);
        assert_eq!(params.len, 1);
        assert_eq!(params.step, 1);
        assert_eq!(params.offset, 0);

        // Inserting after it shouldn't
        let params = b.get_slice_insertion_params(0, 1);
        assert_eq!(params.new_size, 2);
        assert_eq!(params.moves, 0);

        // Add onother element
        b.dimension_values[0].push(1);

        // Inserting at front should move both
        let params = b.get_slice_insertion_params(0, 0);
        assert_eq!(params.new_size, 3);
        assert_eq!(params.moves, 1);
        assert_eq!(params.len, 2);
        assert_eq!(params.step, 1);
        assert_eq!(params.offset, 0);

        // Inserting in the middle should move one, but the "move_size" will be 2 and simply get truncated down
        let params = b.get_slice_insertion_params(0, 1);
        assert_eq!(params.new_size, 3);
        assert_eq!(params.moves, 1);
        assert_eq!(params.len, 2);
        assert_eq!(params.step, 1);
        assert_eq!(params.offset, 1);

        // Inserting at the end shouldn't move anything
        let params = b.get_slice_insertion_params(0, 3);
        assert_eq!(params.new_size, 3);
        assert_eq!(params.moves, 0);
    }

    #[test]
    fn two_dimension() {
        // One empty dimension, can only insert in one place
        let mut b = Block::new(2);
        b.dimension_values[1].push(0);

        let params = b.get_slice_insertion_params(0, 0);
        assert_eq!(params.new_size, 1);
        assert_eq!(params.moves, 0);

        // Add a single element
        b.dimension_values[0].push(0);

        // Inserting before it should move it
        let params = b.get_slice_insertion_params(0, 0);
        assert_eq!(params.new_size, 2);
        assert_eq!(params.moves, 1);
        assert_eq!(params.len, 1);
        assert_eq!(params.step, 1);
        assert_eq!(params.offset, 0);

        // Inserting after it shouldn't
        let params = b.get_slice_insertion_params(0, 1);
        assert_eq!(params.new_size, 2);
        assert_eq!(params.moves, 0);

        // Add onother element
        b.dimension_values[0].push(1);

        // Inserting at front should move both
        let params = b.get_slice_insertion_params(0, 0);
        assert_eq!(params.new_size, 3);
        assert_eq!(params.moves, 1);
        assert_eq!(params.len, 2);
        assert_eq!(params.step, 1);
        assert_eq!(params.offset, 0);

        // Inserting in the middle should move one, but the "move_size" will be 2 and simply get truncated down
        let params = b.get_slice_insertion_params(0, 1);
        assert_eq!(params.new_size, 3);
        assert_eq!(params.moves, 1);
        assert_eq!(params.len, 2);
        assert_eq!(params.step, 1);
        assert_eq!(params.offset, 1);

        // Inserting at the end shouldn't move anything
        let params = b.get_slice_insertion_params(0, 3);
        assert_eq!(params.new_size, 3);
        assert_eq!(params.moves, 0);
    }
}

#[cfg(test)]
mod slice_insert_tests {
    use super::Block;

    #[test]
    fn one_dimension() {
        let mut b = Block::new(1);

        assert_eq!(b.dimension_values.len(), 1);
        assert_eq!(b.dimension_values[0].len(), 0);

        b.add_dimension_value(0, 42);

        assert_eq!(b.dimension_values[0].len(), 1);
        assert_eq!(b.dimension_values[0][0], 42);

        assert_eq!(b.values.len(), 1);
        assert_eq!(b.values[0], None);

        b.values[0] = Some(1000);

        /* Add a value before the previous one, requiring it to be shifted. */

        b.add_dimension_value(0, 40);

        assert_eq!(b.dimension_values[0].len(), 2);
        assert_eq!(b.dimension_values[0][0], 40);
        assert_eq!(b.dimension_values[0][1], 42);

        assert_eq!(b.values.len(), 2);
        assert_eq!(b.values[0], None);
        assert_eq!(b.values[1], Some(1000));

        b.values[0] = Some(2000);

        /* Add one in between. */

        b.add_dimension_value(0, 41);

        assert_eq!(b.dimension_values[0].len(), 3);
        assert_eq!(b.dimension_values[0][0], 40);
        assert_eq!(b.dimension_values[0][1], 41);
        assert_eq!(b.dimension_values[0][2], 42);

        assert_eq!(b.values.len(), 3);
        assert_eq!(b.values[0], Some(2000));
        assert_eq!(b.values[1], None);
        assert_eq!(b.values[2], Some(1000));
    }

    #[test]
    fn two_dimensions() {
        let mut b = Block::new(2);

        assert_eq!(b.dimension_values.len(), 2);
        assert_eq!(b.dimension_values[0].len(), 0);
        assert_eq!(b.dimension_values[1].len(), 0);

        b.add_dimension_value(0, 42);

        assert_eq!(b.dimension_values[0].len(), 1);
        assert_eq!(b.dimension_values[0][0], 42);

        assert_eq!(b.values.len(), 0);

        b.add_dimension_value(1, 99);

        assert_eq!(b.dimension_values[1].len(), 1);
        assert_eq!(b.dimension_values[1][0], 99);

        assert_eq!(b.values.len(), 1);
    }
}

#[cfg(test)]
mod iterate_tests {
    use super::*;

    #[test]
    fn empty_block() {
        let mut b = Rc::new(Block::new(1));

        let count = Block::iter(&b).count();
        assert_eq!(count, 0);

        let mut b = Block::new(1);
        b.add_dimension_value(0, 42);
        let b = Rc::new(b);

        let count = Block::iter(&b).count();
        assert_eq!(count, 0);
    }

    #[test]
    fn one_dimension() {
        let mut b = Block::new(1);
        b.add_row(&[42, 99]);
        let b = Rc::new(b);

        let items : Vec<_> = Block::iter(&b).collect();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0][0], 42);

        let mut b = Block::new(1);
        b.add_row(&[42, 99]);
        b.values[0] = None;
        let b = Rc::new(b);

        let count = Block::iter(&b).count();
        assert_eq!(count, 0);
    }
}
