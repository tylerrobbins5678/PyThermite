use std::{array::from_fn, hash::Hash};
use std::hash::Hasher;

use croaring::Bitmap;
use rustc_hash::FxHasher;

use crate::index::value::PyValue;



pub struct RadixMap<const D: usize> {
    supermap: [[Bitmap; 256]; D]
}

impl <const D: usize>RadixMap<D> {
    pub fn new() -> Self {
        Self { 
            supermap: from_fn(|_| from_fn(|_| Bitmap::default()))
        }
    }

    #[inline(always)]
    pub fn add(&mut self, val: &PyValue, id: u32){
        let hash = val.get_hash();
        let bytes = hash.to_le_bytes();
        for i in 0..D {
            self.get_bitmap_mut(i, &bytes).add(id);
        }
    }

    #[inline(always)]
    pub fn remove(&mut self, val: &PyValue, id: u32){
        let hash = val.get_hash();
        let bytes = hash.to_le_bytes();
        for i in 0..D {
            self.get_bitmap_mut(i, &bytes).remove(id);
        }
    }

    #[inline(always)]
    pub fn get(&self, val: &PyValue) -> Bitmap{
        let bytes = val.get_hash().to_le_bytes();
        let mut result = self.get_bitmap(0, &bytes).clone();
        for i in 1..D {
            result.and_inplace(self.get_bitmap(i, &bytes));
        }
        result
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        let refs: [&Bitmap; 256] = std::array::from_fn(|i| &self.supermap[0][i]);
        Bitmap::fast_or(&refs).cardinality() == 0
    }

    #[inline(always)]
    pub fn and_all(&mut self, keep: Bitmap) {
        for i in 0..D {
            for j in 0..256 {
                unsafe{
                    self.supermap.get_unchecked_mut(i).get_unchecked_mut(j).and_inplace(&keep);
                }
            }
        }
    }

    #[inline(always)]
    fn get_bitmap_mut(
        &mut self,
        i: usize,
        bytes: &[u8; 8],
    ) -> &mut Bitmap {
        unsafe{
            self.supermap
                .get_unchecked_mut(i)
                .get_unchecked_mut(bytes[i] as usize)
        }
    }

    #[inline(always)]
    fn get_bitmap(
        &self,
        i: usize,
        bytes: &[u8; 8],
    ) -> &Bitmap {
        unsafe{
            self.supermap
                .get_unchecked(i)
                .get_unchecked(bytes[i] as usize)
        }
    }

}


impl <const D: usize>Default for RadixMap<D> {
    fn default() -> Self {
        Self {
            supermap: from_fn(|_| {
                // Each row is [Bitmap; 256]
                from_fn(|_| Bitmap::default())
            }),
        }
    }
}