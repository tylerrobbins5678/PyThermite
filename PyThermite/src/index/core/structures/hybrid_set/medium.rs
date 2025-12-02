use croaring::Bitmap;

use crate::index::core::structures::hybrid_set::{HybridSet, hybrid_set::{HybridSetIter, MED_LIMIT}, small::Small};



#[derive(Clone, Debug)]
pub struct Medium {
    pub len: usize,
    pub data: [u32; MED_LIMIT]
}

impl Medium{
    pub fn new() -> Self {
        Self{
            len: 0, 
            data: [0;MED_LIMIT]
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

    pub fn add_many(&mut self, vals: &[u32]) {
        let mut sorted_vals = vals.to_owned();
        sorted_vals.sort_unstable();
        self.add_many_sorted(&sorted_vals);
    }

    pub fn add_many_sorted(&mut self, vals: &[u32]) {
        // zipper the two sorted arrays acheiving O(n) time
        let mut i = 0;
        let mut j = 0;
        let mut data = [0; MED_LIMIT];
        let mut len = 0;
        while i < self.len && j < vals.len() {
            if self.data[i] < vals[j] {
                data[len] = self.data[i];
                i += 1;
            } else if self.data[i] > vals[j] {
                data[len] = vals[j];
                j += 1;
            } else {
                data[len] = self.data[i];
                i += 1;
                j += 1;
            }
            len += 1;
        }

        // copy remaining from self
        if i < self.len {
            let count = self.len - i;
            data[len..len + count].copy_from_slice(&self.data[i..]);
            len += count;
        }

        // copy remaining from vals
        if j < vals.len() {
            let count = vals.len() - j;
            data[len..len + count].copy_from_slice(&vals[j..]);
            len += count;
        }

        self.data = data;
    }

    pub fn and_many_sorted(&mut self, vals: &[u32]) {
        let mut new_data: [u32; MED_LIMIT] = [0; MED_LIMIT];
        let mut new_len = 0;

        let mut i = 0;
        let mut j = 0;

        while i < self.len && j < vals.len() {
            if self.data[i] < vals[j] {
                i += 1;
            } else if self.data[i] > vals[j] {
                j += 1;
            } else {
                new_data[new_len] = self.data[i];
                new_len += 1;
                i += 1;
                j += 1;
            }
        }

        self.data = new_data;
        self.len = new_len;
    }

    pub fn add_in_order(&mut self, val: u32) {
        let mut pos = 0;
        while pos < self.len && self.data[pos] < val {
            pos += 1;
        }
        for i in (pos..self.len).rev() {
            self.data[i + 1] = self.data[i];
        }
        self.data[pos] = val;
        self.len += 1;
    }

    pub fn and_inplace_small(&mut self, other: &Small) {
        let mut new_data: [u32; MED_LIMIT] = [0; MED_LIMIT];
        let mut new_len = 0;

        for &val in other.as_slice() {
            if self.contains(val) {
                new_data[new_len] = val;
                new_len += 1;
            }
        }

        self.data = new_data;
        self.len = new_len;
    }

    pub fn and_inplace_medium(&mut self, other: &Medium) {
        self.and_many_sorted(other.as_slice());
    }

    pub fn and_inplace_large(&mut self, other: &Bitmap) {
        let mut new_data: [u32; MED_LIMIT] = [0; MED_LIMIT];
        let mut new_len = 0;

        for &val in self.as_slice() {
            if other.contains(val) {
                new_data[new_len] = val;
                new_len += 1;
            }
        }
        self.data = new_data;
        self.len = new_len;
    }

    pub fn or_inplace_small(mut self, other: &Small) -> HybridSet {
        if self.len + other.len <= MED_LIMIT {
            self.add_many(other.as_slice());
            HybridSet::Medium(self)
        } else {
            let mut new_bmp = Bitmap::of(self.as_slice());
            new_bmp.add_many(other.as_slice());
            HybridSet::Large(new_bmp)
        }
    }

    pub fn or_inplace_medium(mut self, other: &Medium) -> HybridSet {
        if self.len + other.len <= MED_LIMIT {
            // assumes other is sorted already
            self.add_many_sorted(other.as_slice());
            HybridSet::Medium(self)
        } else {
            let mut new_bmp = Bitmap::of(self.as_slice());
            new_bmp.add_many(other.as_slice());
            HybridSet::Large(new_bmp)
        }
    }

    pub fn or_inplace_large(self, other: &Bitmap) -> HybridSet {
        let mut new_bmp = Bitmap::of(self.as_slice());
        new_bmp.or_inplace(other);
        HybridSet::Large(new_bmp)
    }

    pub fn iter(&self) -> HybridSetIter<'_> {
        HybridSetIter::Small(self.as_slice().iter())
    }
}