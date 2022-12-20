use std::fmt::{Debug, Formatter};
use std::ops::Index;

use crate::{Datum, TransactionId};

#[derive(Clone)]
pub struct QueryRow {
    pub txn_id: TransactionId,
    pub(crate) values_array: Vec<Datum>
}

impl Index<usize> for QueryRow {
    type Output = Datum;

    fn index(&self, index: usize) -> &Self::Output {
        &self.values_array[index]
    }
}

impl Debug for QueryRow {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(&self.values_array).finish()
   }
}
