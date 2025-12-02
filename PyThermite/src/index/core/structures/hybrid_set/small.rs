use croaring::Bitmap;

use crate::index::core::structures::hybrid_set::{HybridSet, HybridSetOps, hybrid_set::{HybridSetIter, MED_LIMIT, SMALL_LIMIT}, medium::Medium};



#[derive(Clone, Debug)]
pub struct Small {
    pub len: usize,
    pub data: [u32; SMALL_LIMIT]
}

impl Small{
    pub fn new() -> Self {
        Self{
            len: 0, 
            data: [0;SMALL_LIMIT]
        }
    }

    pub fn as_slice(&self) -> &[u32] {
        &self.data[..self.len]
    }

    pub fn contains(&self, idx: u32) -> bool {
        self.data[..self.len].contains(&idx)
    }

    pub fn cardinality(&self) -> u64 {
        self.len as u64
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn remove(&mut self, idx: u32) {
        if let Some(pos) = self.data[..self.len].iter().position(|&x| x == idx) {
            for i in pos..self.len - 1 {
                self.data[i] = self.data[i + 1];
            }
            self.len -= 1;
        }
    }

    pub fn and_inplace_small(&mut self, other: &Small) -> HybridSet {
        let mut new_data: [u32; SMALL_LIMIT] = [0; SMALL_LIMIT];
        let mut new_len = 0;

        for &val in self.as_slice() {
            if other.contains(val) {
                new_data[new_len] = val;
                new_len += 1;
            }
        }

        HybridSet::Small(Small {
            len: new_len,
            data: new_data,
        })
    }

    pub fn and_inplace_large(&mut self, other: &Bitmap) -> HybridSet {
        let mut new_data: [u32; SMALL_LIMIT] = [0; SMALL_LIMIT];
        let mut new_len = 0;

        for &val in self.as_slice() {
            if other.contains(val) {
                new_data[new_len] = val;
                new_len += 1;
            }
        }

        HybridSet::Small(Small {
            len: new_len,
            data: new_data,
        })
    }

    pub fn or_inplace_small(mut self, other: &Small) -> HybridSet {
        if self.len + other.len <= SMALL_LIMIT {
            self.data[self.len .. self.len + other.len].copy_from_slice(&other.data[..other.len]);
            self.len += other.len;
            HybridSet::Small(self)
        } else {
            let mut new_bmp = Bitmap::of(self.as_slice());
            new_bmp.add_many(other.as_slice());
            HybridSet::Large(new_bmp)
        }
    }

    pub fn or_inplace_medium(self, other: &Medium) -> HybridSet {
        if self.len + other.len <= MED_LIMIT {
            let mut new_med = HybridSet::Medium(
                Medium { len: 0, data: other.data }
            );
            for &val in self.as_slice() {
                new_med.add(val);
            }
            new_med
        } else {
            let mut new_bmp = Bitmap::of(self.as_slice());
            new_bmp.add_many(other.as_slice());
            HybridSet::Large(new_bmp)
        }
    }

    pub fn or_inplace_large(self, other: &Bitmap) -> HybridSet {
        let mut new_bmp = other.clone();
        new_bmp.add_many(self.as_slice());
        HybridSet::Large(new_bmp)
    }

    pub fn iter(&self) -> HybridSetIter<'_> {
        HybridSetIter::Small(self.as_slice().iter())
    }
}