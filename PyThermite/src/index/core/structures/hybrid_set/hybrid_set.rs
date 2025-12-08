
use std::mem;
use croaring::Bitmap;
use croaring::bitmap::BitmapIterator;

use crate::index::core::structures::hybrid_set::{small::Small, medium::Medium};

pub const SMALL_LIMIT: usize = 4;
pub const MED_LIMIT: usize = 4;

#[derive(Clone, Debug)]
pub enum HybridSet {
    Empty,
    Small(Small),   // stack
    Medium(Box<Medium>), // stack - sorted
    Large(Bitmap),  // heap
}

macro_rules! delegate_ref {
    ($self:expr, $method:ident $(, $args:expr )* ) => {
        match $self {
            HybridSet::Empty => panic!("called {} on Empty", stringify!($method)),
            HybridSet::Small(inner) => inner.$method($($args),*),
            HybridSet::Medium(inner) => inner.$method($($args),*),
            HybridSet::Large(inner) => inner.$method($($args),*),
        }
    };
}

macro_rules! delegate_mut {
    ($self:expr, $method:ident $(, $args:expr )* ) => {
        match $self {
            HybridSet::Empty => panic!("called {} on Empty", stringify!($method)),
            HybridSet::Small(inner) => inner.$method($($args),*),
            HybridSet::Medium(inner) => inner.$method($($args),*),
            HybridSet::Large(inner) => inner.$method($($args),*),
        }
    };
}

pub trait HybridSetOps {
    fn new() -> HybridSet;
    fn add(&mut self, value: u32);
    fn from_sorted(items: &[u32]) -> HybridSet;
    fn of(items: &[u32]) -> HybridSet;
    fn contains(&self, value: u32) -> bool;
    fn is_empty(&self) -> bool;
    fn cardinality(&self) -> u64;
    fn or_inplace(&mut self, other: &HybridSet);
    fn and_inplace(&mut self, other: &HybridSet);
    fn as_bitmap(&self) -> Bitmap;
    fn remove(&mut self, idx: u32);
    fn iter(&self) -> HybridSetIter<'_>;
}

impl HybridSetOps for HybridSet {
    fn new() -> Self {
        HybridSet::Small(Small::new())
    }

    fn from_sorted(slice: &[u32]) -> HybridSet {
        let size = slice.len();
        if size == 0 {
            HybridSet::Empty
        } else if size < MED_LIMIT {
            HybridSet::Small(Small::from_sorted(slice) )
        } else {
            HybridSet::Large(Bitmap::from(slice))
        }
    }

    fn add(&mut self, val: u32) {
        match self {
            HybridSet::Small(sm) => {
                if sm.len() + 1 < SMALL_LIMIT {
                    sm.add(val);
                } else if sm.len() + 1 < MED_LIMIT {
                    let mut md = Medium::new();
                    md.add(val);
                    let md = md.or_inplace_small(sm);
                    *self = md;
                } else {
                    let mut bitmap = Bitmap::of(sm.as_slice());
                    bitmap.add(val);
                    *self = HybridSet::Large(bitmap);
                }
            }
            HybridSet::Medium(md) => {
                if md.len() < MED_LIMIT {
                    md.add(val);
                } else {
                    let mut bitmap = Bitmap::of(md.as_slice());
                    bitmap.add(val);
                    *self = HybridSet::Large(bitmap);
                }
            },
            HybridSet::Large(bmp) => {
                bmp.add(val);
            }
            HybridSet::Empty => {
                let small = HybridSet::of(&[val]);
                *self = small;
            }
        }
    }

    fn contains(&self, value: u32) -> bool {
        delegate_ref!(self, contains, value)
    }
    
    fn cardinality(&self) -> u64 {
        delegate_ref!(self, cardinality)
    }
    
    fn or_inplace(&mut self, other: &HybridSet) {

        let old_self = mem::replace(self, HybridSet::Empty);

        let replacement = match (old_self, other) {
            (HybridSet::Small(small), HybridSet::Small(small_other)) => {
                        small.or_inplace_small(small_other)
                    }
            (HybridSet::Small(small), HybridSet::Medium(medium)) => small.or_inplace_medium(medium),
            (HybridSet::Small(small), HybridSet::Large(bitmap_other)) => {
                        small.or_inplace_large(bitmap_other)
                    }
            (HybridSet::Small(small), HybridSet::Empty) => HybridSet::Small(small),

            (HybridSet::Medium(md), HybridSet::Empty) => HybridSet::Medium(md),
            (HybridSet::Medium(md), HybridSet::Small(small)) => md.or_inplace_small(small),
            (HybridSet::Medium(md), HybridSet::Medium(other_md)) => md.or_inplace_medium(other_md),
            (HybridSet::Medium(md), HybridSet::Large(bitmap)) => md.or_inplace_large(bitmap),

            (HybridSet::Large(mut bitmap), HybridSet::Small(small_other)) => {
                        bitmap.add_many(small_other.as_slice());
                        HybridSet::Large(bitmap)
                    }
            (HybridSet::Large(mut bitmap), HybridSet::Large(bitmap_other)) => {
                        bitmap.or_inplace(bitmap_other);
                        HybridSet::Large(bitmap)
                    }
            
            (HybridSet::Large(bitmap), HybridSet::Empty) => HybridSet::Large(bitmap),
            (HybridSet::Large(mut bitmap), HybridSet::Medium(md)) => {
                bitmap.add_many(md.as_slice());
                HybridSet::Large(bitmap)
            },

            (HybridSet::Empty, HybridSet::Small(_)) => other.clone(),
            (HybridSet::Empty, HybridSet::Large(_)) => other.clone(),
            (HybridSet::Empty, HybridSet::Empty) => HybridSet::Empty,
            (HybridSet::Empty, HybridSet::Medium(_)) => other.clone(),
        };

        *self = replacement;
    }
    
    fn and_inplace(&mut self, other: &Self) {
        let old_self = mem::replace(self, HybridSet::Empty);

        let replacement = match (old_self, other) {
            (HybridSet::Small(small), HybridSet::Small(small_other)) => {
                small.and_inplace_small(&small_other)
            }
            (HybridSet::Small(small), HybridSet::Large(bitmap_other)) => {
                small.and_inplace_large(&bitmap_other)
            }
            (HybridSet::Small(small), HybridSet::Medium(medium)) => {
                small.and_inplace_medium(medium)
            },

            (HybridSet::Medium(medium), HybridSet::Small(small)) => {
                medium.and_inplace_small(small)
            },
            (HybridSet::Medium(medium), HybridSet::Medium(other_medium)) => {
                medium.and_inplace_medium(other_medium)
            },
            (HybridSet::Medium(medium), HybridSet::Large(bitmap)) => {
                medium.and_inplace_large(bitmap)
            },
            
            (HybridSet::Large(mut bitmap), HybridSet::Medium(medium)) => {
                bitmap.and_inplace(&Bitmap::of(medium.as_slice()));
                HybridSet::Large(bitmap)
            },
            (HybridSet::Large(mut bitmap), HybridSet::Small(small_other)) => {
                bitmap.and_inplace(&Bitmap::of(small_other.as_slice()));
                HybridSet::Large(bitmap)
            }
            (HybridSet::Large(mut bitmap), HybridSet::Large(bitmap_other)) => {
                bitmap.and_inplace(&bitmap_other);
                HybridSet::Large(bitmap)
            }

            (HybridSet::Empty, _) => HybridSet::Empty,
            (_, HybridSet::Empty) => HybridSet::Empty,
            
        };

        *self = replacement;
    }

    fn as_bitmap(&self) -> Bitmap {
        match self {
            HybridSet::Empty => Bitmap::new(),
            HybridSet::Small(small) => Bitmap::of(small.as_slice()),
            HybridSet::Medium(md) => Bitmap::of(md.as_slice()),
            HybridSet::Large(bitmap) => bitmap.clone(),
        }
    }

    fn is_empty(&self) -> bool {
        delegate_ref!(self, is_empty)
    }
    
    fn iter(&self) -> HybridSetIter<'_> {
        match self {
            HybridSet::Small(small) => HybridSetIter::Small(small.as_slice().iter()),
            HybridSet::Medium(medium) => HybridSetIter::Medium(medium.as_slice().iter()),
            HybridSet::Large(bitmap) => HybridSetIter::Large(bitmap.iter()),
            HybridSet::Empty => panic!("called iter on Empty"),
        }
    }
    
    fn remove(&mut self, idx: u32) {
        delegate_ref!(self, remove, idx)
    }

    fn of(items: &[u32]) -> Self {
        if items.len() < SMALL_LIMIT {
            HybridSet::Small( Small::of(items) )
        } else if items.len() < MED_LIMIT {
            HybridSet::Medium( Box::new(Medium::of(items)) )
        } else {
            HybridSet::Large( Bitmap::of(items) )
        }
    }
    
}


pub enum HybridSetIter<'a> {
    Small(std::slice::Iter<'a, u32>),
    Medium(std::slice::Iter<'a, u32>),
    Large(BitmapIterator<'a>), // adjust path/type as needed
}

impl<'a> Iterator for HybridSetIter<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            HybridSetIter::Small(iter) => iter.next().copied(),
            HybridSetIter::Medium(iter) => iter.next().copied(),
            HybridSetIter::Large(iter) => iter.next(),
        }
    }
}

