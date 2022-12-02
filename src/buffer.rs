use std::fs::File;
use std::io::{BufRead, Read, Write};
use std::{io, ptr};
use std::mem::size_of;

use crate::{Datum, QueryRow};

pub struct Buffer {
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

pub struct BufferIter<'buf> {
    buffer: &'buf Buffer,
    indexes: Vec<usize>,
    value_index: usize,
    values_array: *mut Vec<Datum>
}

impl Buffer {
    pub(crate) fn new(num_dimensions: usize) -> Self {
        Buffer {
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
        unsafe {
            let src = self.values.as_mut_ptr().add(from_idx);
            let dest = src.add(to_idx - from_idx);
            ptr::copy(src, dest, num);
        }
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

        for i in 0..dim_no {
            num_moves *= sizes[i];
        }

        for i in (dim_no + 1)..sizes.len() {
            move_step *= sizes[i];
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

    pub(crate) fn iter<'buf>(&'buf self, values_array: &'buf mut Vec<Datum>) -> BufferIter {
        BufferIter {
            buffer: self,
            indexes: vec![0; self.dimension_values.len()],
            value_index: 0,
            values_array
        }
    }

    pub(crate) fn load<R: BufRead>(&mut self, src: &mut R) -> io::Result<()> {
        let mut decoder = zstd::stream::read::Decoder::with_buffer(src)?;

        const SZ: usize = size_of::<usize>();
        let mut read_buffer: [u8; SZ] = [0; SZ];

        let mut num_values = 1;

        /* Read the dimensions */
        decoder.read(&mut read_buffer)?;
        let num_dimensions = usize::from_ne_bytes(read_buffer);
        self.dimension_values.clear();
        for _ in 0..num_dimensions {
            let mut dim_vals: Vec<Datum>= Vec::new();
            decoder.read(&mut read_buffer)?;
            let dim_size = usize::from_ne_bytes(read_buffer);
            for _ in 0..dim_size {
                decoder.read(&mut read_buffer)?;
                let dim_idx = usize::from_ne_bytes(read_buffer);
                dim_vals.push(dim_idx);
            }
            self.dimension_values.push(dim_vals);
            num_values *= dim_size;
        }

        /* Read the values */
        self.values.clear();
        self.values.reserve(num_values);
        for _ in 0..num_values {
            decoder.read(&mut read_buffer)?;
            let val = usize::from_ne_bytes(read_buffer);
            self.values.push(Some(val));
        }

        decoder.finish();

        Ok(())
    }

    pub(crate) fn save(&self, file: &mut File) -> io::Result<()> {
        let mut encoder = zstd::stream::write::Encoder::new(file, 1)?;

        /* Write the dimensions */
        encoder.write(&usize::to_ne_bytes(self.dimension_values.len()));
        for dim in &self.dimension_values {
            encoder.write(&usize::to_ne_bytes(dim.len()));
            for &dim_val in dim {
                encoder.write(&usize::to_ne_bytes(dim_val));
            }
        }

        /* Write the values */
        for &val in &self.values {
            let val = val.unwrap();
            encoder.write(&usize::to_ne_bytes(val));
        }

        encoder.finish();

        Ok(())
    }
}

impl<'buf> BufferIter<'buf> {
    fn increment_indexes(&mut self) {
        self.value_index += 1;
        let mut incr_pos = self.indexes.len() - 1;
        loop {
            self.indexes[incr_pos] += 1;
            if self.indexes[incr_pos] >= self.buffer.dimension_values[incr_pos].len() {
                if incr_pos == 0 { break; }
                self.indexes[incr_pos] = 0;
                incr_pos -= 1;
                continue;
            }
            break;
        }
    }
}

impl<'buf> Iterator for BufferIter<'buf> {
    type Item = QueryRow;

    fn next(&mut self) -> Option<QueryRow>
    {
        loop {
            // Check if indexes are already past the size of the buffer
            if self.indexes[0] >= self.buffer.dimension_values[0].len() {
                return None;
            }

            // Turn this index into a single number and get the result
            //let calculated_idx = self.buffer.get_index(&self.indexes);
            //assert_eq!(self.value_index, calculated_idx);
            let value: Option<Datum> = self.buffer.values[self.value_index];

            // If it's empty, increment and try the next one
            if value.is_none() {
                self.increment_indexes();
                continue;
            }

            let value = value.unwrap();
            let va = unsafe { self.values_array.as_mut() }.unwrap();
            va.clear();
            for i in 0..self.indexes.len() {
                va.push(self.buffer.dimension_values[i][self.indexes[i]]);
            }
            va.push(value);

            // Move to to the next index and return the row
            self.increment_indexes();
            return Some(QueryRow::new(self.values_array));
        }
    }
}

#[cfg(test)]
mod get_slice_insertion_params_tests {
    use super::Buffer;

    #[test]
    fn one_dimension() {
        // One empty dimension, can only insert in one place
        let mut b = Buffer::new(1);
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
        let mut b = Buffer::new(2);
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
    use super::Buffer;

    #[test]
    fn one_dimension() {
        let mut b = Buffer::new(1);

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
        let mut b = Buffer::new(2);

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
    use crate::buffer::Buffer;
    use crate::Datum;

    #[test]
    fn empty_buffer() {
        let mut b = Buffer::new(1);

        let mut values_array: Vec<Datum> = Vec::new();
        let count = b.iter(&mut values_array).count();
        assert_eq!(count, 0);

        b.add_dimension_value(0, 42);
        let count = b.iter(&mut values_array).count();
        assert_eq!(count, 0);
    }

    #[test]
    fn one_dimension() {
        let mut b = Buffer::new(1);

        b.add_row(&[42, 99]);

        let mut values_array: Vec<Datum> = Vec::new();
        let items : Vec<_> = b.iter(&mut values_array).collect();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0][0], 42);

        b.values[0] = None;
        let count = b.iter(&mut values_array).count();
        assert_eq!(count, 0);
    }
}
