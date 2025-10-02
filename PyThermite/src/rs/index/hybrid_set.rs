
use std::mem;
use croaring::Bitmap;
use croaring::bitmap::BitmapIterator;

const SMALL_LIMIT: usize = 4;

#[derive(Clone, Debug)]
pub enum HybridSet {
    Empty,
    Small(Small),  // stack
    Large(Bitmap), // heap
}

#[derive(Clone, Debug)]
pub struct Small {
    len: usize,
    data: [u32; SMALL_LIMIT] 
}

impl Small{
    fn new() -> Self {
        Self{
            len: 0, 
            data: [0;SMALL_LIMIT]
        }
    }

    pub fn as_slice(&self) -> &[u32] {
        &self.data[..self.len]
    }

    pub fn remove(&mut self, idx: u32) {
        if let Some(pos) = self.data[..self.len].iter().position(|&x| x == idx) {
            for i in pos..self.len - 1 {
                self.data[i] = self.data[i + 1];
            }
            self.len -= 1;
        }
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

    pub fn or_inplace_large(mut self, other: &Bitmap) -> HybridSet {
        // Promote `small` to large bitmap, then OR
        let mut new_bmp = Bitmap::of(self.as_slice());
        new_bmp.or_inplace(other);
        HybridSet::Large(new_bmp)
    }
}

impl HybridSet {
    pub fn new() -> Self {
        HybridSet::Small(Small::new())
    }

    pub fn as_bitmap(self) -> Bitmap {
        match self {
            HybridSet::Empty => Bitmap::new(),
            HybridSet::Small(small) => Bitmap::of(small.as_slice()),
            HybridSet::Large(bitmap) => bitmap,
        }
    }

    pub fn of(items: &[u32]) -> Self {
        if items.len() <= SMALL_LIMIT {
            let mut data: [u32; SMALL_LIMIT] = [0; SMALL_LIMIT];
            data[..items.len()].copy_from_slice(&items[..items.len()]);
            HybridSet::Small(Small {
                len: items.len(), 
                data
            })
        } else {
            HybridSet::Large( Bitmap::of(items) )
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            HybridSet::Small(small) => small.len == 0,
            HybridSet::Large(bitmap) => bitmap.is_empty(),
            HybridSet::Empty => todo!(),
        }
    }

    pub fn remove(&mut self, idx: u32) {
        match self {
            HybridSet::Small(small) => small.remove(idx),
            HybridSet::Large(bitmap) => bitmap.remove(idx),
            HybridSet::Empty => todo!(),
        }
    }

    pub fn cardinality(&self) -> u64 {
        match self {
            HybridSet::Empty => 0,
            HybridSet::Small(small) => small.len as u64,
            HybridSet::Large(bitmap) => bitmap.cardinality(),
        }
    }

    pub fn or_inplace(&mut self, other: &HybridSet) {

        let old_self = mem::replace(self, HybridSet::Empty);

        let replacement = match (old_self, other) {
            (HybridSet::Small(small), HybridSet::Small(small_other)) => {
                        small.or_inplace_small(small_other)
                    }
            (HybridSet::Small(small), HybridSet::Large(bitmap_other)) => {
                        // Returns Some(new_large) if promoted
                        small.or_inplace_large(bitmap_other)
                    }
            (HybridSet::Large(mut bitmap), HybridSet::Small(small_other)) => {
                        bitmap.add_many(small_other.as_slice());
                        HybridSet::Large(bitmap)
                    }
            (HybridSet::Large(mut bitmap), HybridSet::Large(bitmap_other)) => {
                        bitmap.or_inplace(bitmap_other);
                        HybridSet::Large(bitmap)
                    }
            (HybridSet::Empty, HybridSet::Small(small)) => other.clone(),
            (HybridSet::Empty, HybridSet::Large(bitmap)) => other.clone(),
            (HybridSet::Small(small), HybridSet::Empty) => HybridSet::Small(small),
            (HybridSet::Large(bitmap), HybridSet::Empty) => HybridSet::Large(bitmap),
            (HybridSet::Empty, HybridSet::Empty) => HybridSet::Empty,
        };

        *self = replacement;
    }

    pub fn and_inplace(&mut self, other: &Self) {
        let old_self = mem::replace(self, HybridSet::Empty);

        let replacement = match (old_self, other) {
            (HybridSet::Small(small), HybridSet::Small(small_other)) => {
                small.or_inplace_small(&small_other)
            }
            (HybridSet::Small(small), HybridSet::Large(bitmap_other)) => {
                // Returns Some(new_large) if promoted
                small.or_inplace_large(&bitmap_other)
            }
            (HybridSet::Large(mut bitmap), HybridSet::Small(small_other)) => {
                bitmap.and_inplace(&Bitmap::of(small_other.as_slice()));
                HybridSet::Large(bitmap)
            }
            (HybridSet::Large(mut bitmap), HybridSet::Large(bitmap_other)) => {
                bitmap.and_inplace(&bitmap_other);
                HybridSet::Large(bitmap)
            }
            _ => HybridSet::Small(Small::new()),
        };

        *self = replacement;
    }

    #[inline]
    pub fn add(&mut self, val: u32) {
        match self {
            HybridSet::Small(sm) => {
                if sm.len < SMALL_LIMIT {
                    sm.data[sm.len] = val;
                    sm.len += 1;
                } else {
                    let mut bitmap = Bitmap::of(&sm.data[..sm.len]);
                    bitmap.add(val);
                    *self = HybridSet::Large(bitmap);
                }
            }
            HybridSet::Large(bmp) => {
                bmp.add(val);
            }
            HybridSet::Empty => unimplemented!()
        }
    }

    pub fn contains(&self, val: u32) -> bool {
        match self {
            HybridSet::Small(sm) => sm.data[..sm.len].contains(&val),
            HybridSet::Large(bmp) => bmp.contains(val),
            HybridSet::Empty => false
        }
    }

    pub fn iter(&self) -> HybridSetIter<'_> {
        match self {
            HybridSet::Empty => HybridSetIter::Small([].iter()),
            HybridSet::Small(small) => HybridSetIter::Small(small.as_slice().iter()),
            HybridSet::Large(bitmap) => HybridSetIter::Large(bitmap.iter()),
        }
    }


}


pub enum HybridSetIter<'a> {
    Small(std::slice::Iter<'a, u32>),
    Large(BitmapIterator<'a>), // adjust path/type as needed
}

impl<'a> Iterator for HybridSetIter<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            HybridSetIter::Small(iter) => iter.next().copied(),
            HybridSetIter::Large(iter) => iter.next(),
        }
    }
}

