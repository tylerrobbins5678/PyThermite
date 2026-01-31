use croaring::Bitmap;


const BUFF_SIZE: usize = 128;

#[derive(Debug, Clone)]
pub struct BooleanBitmap {
    true_bitmap: Bitmap,
    false_bitmap: Bitmap,
    true_buffer: [u32; BUFF_SIZE],
    true_buff_length: usize,
    false_buffer: [u32; BUFF_SIZE],
    false_buff_length: usize,
}

impl BooleanBitmap {
    pub fn new() -> Self {
        Self {
            true_bitmap: Bitmap::new(),
            false_bitmap: Bitmap::new(),
            true_buffer: [0; BUFF_SIZE],
            true_buff_length: 0,
            false_buffer: [0; BUFF_SIZE],
            false_buff_length: 0,
        }
    }

    pub fn add(&mut self, value: bool, id: u32) {
        if value {
            self.true_bitmap.add(id);
        } else {
            self.false_bitmap.add(id);
        }
    }

    #[inline(always)]
    pub fn add_delayed(&mut self, value: bool, id: u32) {
        if value {
            unsafe {
                *self.true_buffer.get_unchecked_mut(self.true_buff_length) = id;
            }
            self.true_buff_length += 1;
            if self.true_buff_length == BUFF_SIZE {
                self.flush_true();
            }
        } else {
            unsafe {
                *self.false_buffer.get_unchecked_mut(self.true_buff_length) = id;
            }
            self.false_buff_length += 1;
            if self.false_buff_length == BUFF_SIZE {
                self.flush_false();
            }
        }
    }

    #[inline(always)]
    pub fn keep_only(&mut self, ids: &Bitmap) {
        self.true_bitmap.and_inplace(ids);
        self.false_bitmap.and_inplace(ids);
    }

    #[inline(always)]
    pub fn remove(&mut self, value: bool, id: u32) {
        if value {
            self.true_bitmap.remove(id);
        } else {
            self.false_bitmap.remove(id);
        }
    }

    #[inline(always)]
    pub fn flush_true(&mut self) {
        self.true_bitmap.add_many(&self.true_buffer[0..self.true_buff_length]);
        self.true_buff_length = 0;
    }

    #[inline(always)]
    pub fn flush_false(&mut self) {
        self.false_bitmap.add_many(&self.false_buffer[0..self.false_buff_length]);
        self.false_buff_length = 0;
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
        if value {
            &self.true_bitmap
        } else {
            &self.false_bitmap
        }
    }
}

impl Default for BooleanBitmap {
    fn default() -> Self {
        Self::new()
    }
}