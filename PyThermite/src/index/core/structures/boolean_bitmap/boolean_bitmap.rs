use croaring::Bitmap;
use crate::index::core::structures::buffered_bitmap::BufferedBitmap;


const BUFF_SIZE: usize = 64;

#[derive(Debug, Clone)]
pub struct BooleanBitmap {
    true_bitmap: BufferedBitmap<BUFF_SIZE>,
    false_bitmap: BufferedBitmap<BUFF_SIZE>,
}

impl BooleanBitmap {
    pub fn new() -> Self {
        Self {
            true_bitmap: BufferedBitmap::new(),
            false_bitmap: BufferedBitmap::new(),
        }
    }

    pub fn add(&mut self, value: bool, id: u32) {
        [&mut self.false_bitmap, &mut self.true_bitmap][value as usize].add(id);
    }

    #[inline(always)]
    pub fn add_delayed(&mut self, value: bool, id: u32) {
        [&mut self.false_bitmap, &mut self.true_bitmap][value as usize].add_delayed(id);
    }

    #[inline(always)]
    pub fn keep_only(&mut self, ids: &Bitmap) {
        self.true_bitmap.and_inplace(ids);
        self.false_bitmap.and_inplace(ids);
    }

    #[inline(always)]
    pub fn remove(&mut self, value: bool, id: u32) {
        [&mut self.false_bitmap, &mut self.true_bitmap][value as usize].remove(id);
    }

    #[inline(always)]
    pub fn flush_true(&mut self) {
        self.true_bitmap.flush();
    }

    #[inline(always)]
    pub fn flush_false(&mut self) {
        self.false_bitmap.flush();
    }

    pub fn flush(&mut self) {
        self.flush_true();
        self.flush_false();
    }

    #[inline(always)]
    pub fn merge(&mut self, other: &BooleanBitmap) {
        self.true_bitmap.or_inplace(&other.true_bitmap);
        self.false_bitmap.or_inplace(&other.false_bitmap);
    }

    #[inline(always)]
    pub fn get_exact(&self, value: bool) -> &Bitmap {
        [&self.false_bitmap, &self.true_bitmap][value as usize]
    }
}

impl Default for BooleanBitmap {
    fn default() -> Self {
        Self::new()
    }
}