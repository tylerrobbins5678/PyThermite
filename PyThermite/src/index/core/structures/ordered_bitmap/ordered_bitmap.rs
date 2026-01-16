use std::{cell::RefCell, sync::OnceLock};

use croaring::Bitmap;

pub(crate) const BIT_LENGTH: usize = 76; // do not use the whole 128

thread_local! {
    pub(crate) static TMP_BITMAP: RefCell<Bitmap> = RefCell::new(Bitmap::new());
}

#[derive(Debug, Clone)]
pub(crate) struct NumericBitIndex {
    bits: [Bitmap; 2],
}

impl NumericBitIndex {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline(always)]
    pub fn add(&mut self, byte_id: usize, id: u32) {
        unsafe {
            self.bits.get_unchecked_mut(byte_id).add(id)
        }
    }

    #[inline(always)]
    pub fn remove(&mut self, byte_id: usize, id: u32) {
        unsafe {
            self.bits.get_unchecked_mut(byte_id).remove(id)
        }
    }

    #[inline(always)]
    pub fn contains(&self, byte_id: usize) -> &Bitmap {
        unsafe {
            self.bits.get_unchecked(byte_id)
        }
    }

    pub fn all(&self) -> Bitmap {
        Bitmap::fast_or(
            unsafe{
                &[
                    self.bits.get_unchecked(0 as usize),
                    self.bits.get_unchecked(1 as usize)
                ]
            }
        )
    }
}

impl Default for NumericBitIndex {
    fn default() -> Self {
        Self { 
            bits: std::array::from_fn( |_| Bitmap::new()),
        }
    }
}

#[derive(Debug)]
pub struct NumericalBitmap {
    pub(crate) bits: [NumericBitIndex; BIT_LENGTH],
}

impl NumericalBitmap {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline(always)]
    pub fn add(&mut self, value: u128, id: u32) {
        for bit in 0..BIT_LENGTH {
            let v = ((value >> bit) & 1) as usize;
            self.bits[bit].add(v, id);
        }
    }

    #[inline(always)]
    pub fn remove(&mut self, value: u128, id: u32) {
        for bit in 0..BIT_LENGTH {
            let v = ((value >> bit) & 1) as usize;
            self.bits[bit].remove(v, id);
        }
    }

    #[inline(always)]
    pub fn get_exact(&self, value: u128) -> Bitmap {
        let mut res = Bitmap::new();
        self.get_exact_into(value, &mut res);
        res
    }

    #[inline(always)]
    pub fn get_exact_into(&self, value: u128, out: &mut Bitmap) {
        let first_bit = 0;
        let first_v = ((value >> first_bit) & 1) as usize;

        out.or_inplace(self.bits[first_bit].contains(first_v));

        for bit in 1..BIT_LENGTH {
            let v = ((value >> bit) & 1) as usize;
            out.and_inplace(self.bits[bit].contains(v));
        }
    }

}

impl Default for NumericalBitmap {
    fn default() -> Self {
        Self {
            bits: std::array::from_fn(|_| NumericBitIndex::default()),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use croaring::Bitmap;

    #[test]
    fn empty_index_returns_empty() {
        let idx = NumericalBitmap::new();
        let res = idx.get_exact(0);
        assert!(res.is_empty());
    }

    #[test]
    fn single_insert_single_query() {
        let mut idx = NumericalBitmap::new();

        idx.add(0b1011u128, 42);

        let res = idx.get_exact(0b1011);
        assert!(res.contains(42));
        assert_eq!(res.cardinality(), 1);
    }

    #[test]
    fn insert_two_distinct_values() {
        let mut idx = NumericalBitmap::new();

        idx.add(0b1011u128, 1);
        idx.add(0b0101u128, 2);

        let r1 = idx.get_exact(0b1011);
        let r2 = idx.get_exact(0b0101);

        assert!(r1.contains(1));
        assert!(!r1.contains(2));

        assert!(r2.contains(2));
        assert!(!r2.contains(1));
    }

    #[test]
    fn identical_values_multiple_ids() {
        let mut idx = NumericalBitmap::new();

        idx.add(0b1110u128, 1);
        idx.add(0b1110u128, 2);
        idx.add(0b1110u128, 3);

        let res = idx.get_exact(0b1110);

        assert_eq!(res.cardinality(), 3);
        assert!(res.contains(1));
        assert!(res.contains(2));
        assert!(res.contains(3));
    }

    #[test]
    fn remove_existing_id() {
        let mut idx = NumericalBitmap::new();

        idx.add(0b1101u128, 7);
        idx.remove(0b1101u128, 7);

        let res = idx.get_exact(0b1101);
        assert!(res.is_empty());
    }

    #[test]
    fn remove_one_of_many() {
        let mut idx = NumericalBitmap::new();

        idx.add(0b1001u128, 1);
        idx.add(0b1001u128, 2);

        idx.remove(0b1001u128, 1);

        let res = idx.get_exact(0b1001);
        assert_eq!(res.cardinality(), 1);
        assert!(res.contains(2));
        assert!(!res.contains(1));
    }

    #[test]
    fn bit_collision_does_not_match() {
        let mut idx = NumericalBitmap::new();

        idx.add(0b1010u128, 1);
        idx.add(0b1011u128, 2);

        let r1 = idx.get_exact(0b1010);
        let r2 = idx.get_exact(0b1011);

        assert!(r1.contains(1));
        assert!(!r1.contains(2));

        assert!(r2.contains(2));
        assert!(!r2.contains(1));
    }

    #[test]
    fn high_bits_are_indexed() {
        let mut idx = NumericalBitmap::new();

        let value = 1u128 << 75; // highest indexed bit
        idx.add(value, 99);

        let res = idx.get_exact(value);
        assert!(res.contains(99));
    }

    #[test]
    fn query_nonexistent_value() {
        let mut idx = NumericalBitmap::new();

        idx.add(0b1010u128, 1);

        let res = idx.get_exact(0b0101);
        assert!(res.is_empty());
    }

    #[test]
    fn multiple_values_shared_bits() {
        let mut idx = NumericalBitmap::new();

        idx.add(0b111100u128, 1);
        idx.add(0b111101u128, 2);
        idx.add(0b111110u128, 3);

        let r1 = idx.get_exact(0b111100);
        let r2 = idx.get_exact(0b111101);
        let r3 = idx.get_exact(0b111110);

        assert!(r1.contains(1));
        assert!(!r1.contains(2));
        assert!(!r1.contains(3));

        assert!(r2.contains(2));
        assert!(r3.contains(3));
    }

}