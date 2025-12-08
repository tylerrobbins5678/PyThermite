use croaring::Bitmap;

use crate::index::core::structures::{centered_array::CenteredArray, hybrid_set::{HybridSet, hybrid_set::{HybridSetIter, MED_LIMIT}, small::Small}};



#[derive(Clone, Debug)]
pub struct Medium {
    pub data: CenteredArray<MED_LIMIT>,
}

impl Medium{
    pub fn new() -> Self {
        Self{
            data: CenteredArray::new()
        }
    }

    pub fn of(items: &[u32]) -> Self {
        let mut arr = CenteredArray::<MED_LIMIT>::new();
        for &item in items {
            arr.insert(item);
        }
        Self {
            data: arr,
        }
    }

    pub fn add(&mut self, val: u32) {
        self.data.insert(val);
    }

    pub fn as_slice(&self) -> &[u32] {
        &self.data.iter() // get slice of valid data
    }

    pub fn contains(&self, idx: u32) -> bool {
        self.data.contains(&idx)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn cardinality(&self) -> u64 {
        self.len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.data.len() == 0
    }

    pub fn remove(&mut self, idx: u32) {
        self.data.remove(&idx);
    }

    pub fn and_inplace_small(mut self, other: &Small) -> HybridSet {
        self.data.and_with(&other.data);
        HybridSet::Medium(Box::new(self))
    }

    pub fn and_inplace_medium(mut self, other: &Medium) -> HybridSet {
        self.data.and_with(&other.data);
        HybridSet::Medium(Box::new(self))
    }

    pub fn and_inplace_large(mut self, other: &Bitmap) -> HybridSet{
        let mut new_data: [u32; MED_LIMIT] = [0; MED_LIMIT];
        let mut new_len = 0;

        for &val in self.as_slice() {
            if other.contains(val) {
                new_data[new_len] = val;
                new_len += 1;
            }
        }

        self.data = CenteredArray::consuming_sorted_slice(
            new_data,
        );
        HybridSet::Medium(Box::new(self))

    }

    pub fn or_inplace_small(mut self, other: &Small) -> HybridSet {
        if self.data.len() + other.len() <= MED_LIMIT {
            self.data.union_with(&other.data);
            HybridSet::Medium(Box::new(self))
        } else {
            let mut new_bmp = Bitmap::of(self.as_slice());
            new_bmp.add_many(other.as_slice());
            HybridSet::Large(new_bmp)
        }
    }

    pub fn or_inplace_medium(mut self, other: &Medium) -> HybridSet {
        if self.data.len() + other.data.len() <= MED_LIMIT {
            // assumes other is sorted already
            self.data.union_with(&other.data);
            HybridSet::Medium(Box::new(self))
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