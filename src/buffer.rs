use std::ptr;

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
    buffer : &'buf Buffer,
    next_index : usize
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
            let to_offset = from_offset + (i+1) * params.step;
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
            ptr::copy(src,dest, num);
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

        for i in (dim_no+1)..sizes.len() {
            move_step *= sizes[i];
        }

        let move_size = sizes[dim_no] * move_step;
        let new_size = num_moves * (sizes[dim_no] + 1) * move_step;
        let current_size= num_moves * move_size;

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

    pub(crate) fn iter(&self) -> BufferIter {
        BufferIter { buffer: self, next_index: 0 }
    }

    pub(crate) fn build_query_row(&self, index: usize, value: Datum) -> QueryRow {
        let mut index = index;
        let mut values = Vec::new();
        let mut dim_slice_size : usize = self.dimension_values.iter().map(|x| x.len()).product();
        for dim in &self.dimension_values {
            dim_slice_size /= dim.len();
            let dim_idx = index / dim_slice_size;
            index -= dim_idx * dim_slice_size;
            values.push(dim[dim_idx]);
        }
        values.push(value);
        QueryRow { values }
    }
}

impl<'buf> Iterator for BufferIter<'buf> {
    type Item = QueryRow;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.next_index >= self.buffer.values.len() {
                return None;
            }

            let index = self.next_index;
            let value = self.buffer.values[index];
            self.next_index += 1;
            if value.is_none() { continue; }
            return Some(self.buffer.build_query_row(index, value.unwrap()));
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

mod slice_insert_tests {
    use crate::buffer::Buffer;

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
