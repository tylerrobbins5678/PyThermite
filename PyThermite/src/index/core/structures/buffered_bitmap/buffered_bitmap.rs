use std::ops::{Deref, DerefMut};

use croaring::Bitmap;

macro_rules! forward_bitmap_immutable_methods {
    (
        $(
            fn $name:ident ( &self $(, $arg:ident : $ty:ty )* ) $( -> $ret:ty )?;
        )*
    ) => {
        $(
            #[inline(always)]
            pub fn $name(&self $(, $arg : $ty )* ) $( -> $ret )? {
                self.inner.$name($($arg),*)
            }
        )*
    };
}

macro_rules! forward_bitmap_mutable_methods {
    (
        $(
            fn $name:ident ( &mut self $(, $arg:ident : $ty:ty )* ) $( -> $ret:ty )?;
        )*
    ) => {
        $(
            #[inline(always)]
            pub fn $name(&mut self $(, $arg : $ty )* ) $( -> $ret )? {
                self.inner.$name($($arg),*)
            }
        )*
    };
}

#[derive(Debug, Clone)]
pub struct BufferedBitmap<const N: usize> {
    inner: Bitmap,
    buffer: [u32; N],
    buff_len: usize,
}

impl<const N: usize> BufferedBitmap<N> {

    pub fn new() -> Self {
        Self {
            inner: Bitmap::new(),
            buffer: [0u32; N],
            buff_len: 0
        }
    }

    #[inline(always)]
    pub fn add_delayed(&mut self, id: u32) {
        if self.buff_len == N {
            self.flush();
        }

        unsafe {
            *self.buffer.get_unchecked_mut(self.buff_len) = id;
        }
        self.buff_len += 1;
    }

    #[inline(always)]
    pub fn flush(&mut self) {
        if self.buff_len != 0 {
            self.inner.add_many(&self.buffer[0..self.buff_len]);
            self.buff_len = 0;
        }
    }

    forward_bitmap_immutable_methods! {
        fn cardinality(&self) -> u64;
        fn is_empty(&self) -> bool;
        fn contains(&self, value: u32) -> bool;
        fn minimum(&self) -> Option<u32>;
        fn maximum(&self) -> Option<u32>;
    }

    forward_bitmap_mutable_methods! {
        fn and_inplace(&mut self, other: &Bitmap);
        fn or_inplace(&mut self, other: &Bitmap);
        fn add_many(&mut self, to_add: &[u32]);
        fn remove(&mut self, to_remove: u32);
    }

}

impl<const N: usize> Default for BufferedBitmap<N> {
    fn default() -> Self {
        Self { 
            inner: Default::default(),
            buffer: [0u32; N],
            buff_len: Default::default()
        }
    }
}

impl<const N: usize> Deref for BufferedBitmap<N> {
    type Target = Bitmap;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<const N: usize> DerefMut for BufferedBitmap<N> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}