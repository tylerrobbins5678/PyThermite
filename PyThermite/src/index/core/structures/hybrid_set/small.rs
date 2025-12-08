use croaring::Bitmap;

use crate::index::core::structures::{centered_array::CenteredArray, hybrid_set::{HybridSet, HybridSetOps, hybrid_set::{HybridSetIter, MED_LIMIT, SMALL_LIMIT}, medium::Medium}};



#[derive(Clone, Debug)]
pub struct Small {
    pub data: CenteredArray<SMALL_LIMIT>,
}

impl Small{
    pub fn new() -> Self {
        Self{
            data: CenteredArray::new(),
        }
    }

    pub fn of(items: &[u32]) -> Self {
        let mut arr = CenteredArray::<SMALL_LIMIT>::new();
        for &item in items {
            arr.insert(item);
        }
        Self {
            data: arr,
        }
    }

    pub fn from_sorted(items: &[u32]) -> Self {
        let slf = Self {
            data: CenteredArray::from_sorted_slice(items)
        };
        slf
    }

    pub fn add(&mut self, val: u32) {
        self.data.insert(val);
    }

    pub fn as_slice(&self) -> &[u32] {
        &self.data.iter()
    }

    pub fn contains(&self, idx: u32) -> bool {
        self.data.contains(&idx)
    }

    pub fn cardinality(&self) -> u64 {
        self.len() as u64
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn remove(&mut self, idx: u32) {
        self.data.remove(&idx);
    }

    pub fn and_inplace_small(mut self, other: &Small) -> HybridSet {
        self.data.and_with(&other.data);
        HybridSet::Small(self)
    }

    pub fn and_inplace_medium(mut self, other: &Medium) -> HybridSet {
        self.data.and_with(&other.data);
        HybridSet::Small(self)
    }

    pub fn and_inplace_large(mut self, other: &Bitmap) -> HybridSet {
        let mut to_keep = CenteredArray::<SMALL_LIMIT>::new();

        for &val in self.as_slice() {
            if other.contains(val) {
                to_keep.insert(val);
            }
        }

        self.data = to_keep;

        HybridSet::Small(self)
    }

    pub fn or_inplace_small(mut self, other: &Small) -> HybridSet {
        let size = self.len() + other.len();
        if size <= SMALL_LIMIT {
            self.data.union_with(&other.data);
            HybridSet::Small(self)
        } else if size <= MED_LIMIT {
            let mut arr = CenteredArray::<MED_LIMIT>::new();
            arr.union_with(&other.data);
            HybridSet::Medium(
                Box::new(Medium { data: arr })
            )
        } else {
            let mut new_bmp = Bitmap::of(self.as_slice());
            new_bmp.add_many(other.as_slice());
            HybridSet::Large(new_bmp)
        }
    }

    pub fn or_inplace_medium(self, other: &Medium) -> HybridSet {
        let size = self.len() + other.len();
        if size <= MED_LIMIT {
            let mut arr = CenteredArray::<MED_LIMIT>::new();
            arr.union_with(&other.data);
            HybridSet::Medium(
                Box::new(Medium { data: arr })
            )
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