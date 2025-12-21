use std::{mem, vec};
use std::{array::from_fn, hash::Hash};
use std::hash::Hasher;

use croaring::Bitmap;
use rustc_hash::FxHasher;

use crate::index::core::query::QueryMap;
use crate::index::core::stored_item;
use crate::index::value::PyValue;



pub struct RadixMap<const D: usize> {
    supermap: [[Bitmap; 256]; D],
    overflow_map: Vec<(PyValue, Bitmap)>
}

impl <const D: usize>RadixMap<D> {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline(always)]
    pub fn add(mut self, val: &PyValue, id: u32, query_map: QueryMap) -> Self {
        // build hash
        let hash = val.get_hash();
        let bytes = hash.to_le_bytes();
        // process first bitmap
        let bm = self.get_bitmap_mut(0, &bytes);
        let mut conflicts = bm.clone();
        bm.add(id);
        // check for conflicts and insert into rest of bitmaps
        for i in 1..D {
            let bm = self.get_bitmap_mut(i, &bytes);
            conflicts.and_inplace(bm);
            bm.add(id);
        }

        if conflicts.is_empty() {
            return self;
        }

        // check existing overflow map
        for (existing_val, bm) in self.overflow_map.iter_mut() {
            if existing_val == val {
                bm.add(id);
                return self;
            }
        }

        // check if is true conflict
        let stored_items = query_map.get_stored_items().read().unwrap();
        let cid =  conflicts.iter().next().unwrap();
        let stored_item = stored_items.get(cid as usize).unwrap();
        let mut new_length: u32 = 0;
        stored_item.with_attr_id(cid, |existing_val| {
            if existing_val == val {
                // true conflict, add to overflow
                let new_bm = Bitmap::from([id]);
                self.overflow_map.push((val.clone(), new_bm));
                return;
            } else {
                // real conflict, need to expand
                // calculate bits shared by conflicts
                let existing_bytes = existing_val.get_hash().to_le_bytes();
                for b in existing_bytes.iter().zip(bytes.iter()).enumerate() {
                    let (i, (eb, nb)) = b;
                    if eb != nb {
                        new_length = i as u32 + 1;
                        return;
                    }
                }
                // asusme all bits same - need to push to overflow
                new_length = 0u32;
                let new_bm = Bitmap::from([id]);
                self.overflow_map.push((val.clone(), new_bm));
            }
        });

        drop(stored_items);

        match new_length {
            0 => return self, // already handled
            1 => self = Self::expand_from_other::<D, 1>(self, query_map),
            2 => self = Self::expand_from_other::<D, 2>(self, query_map),
            3 => self = Self::expand_from_other::<D, 3>(self, query_map),
            4 => self = Self::expand_from_other::<D, 4>(self, query_map),
            5 => self = Self::expand_from_other::<D, 5>(self, query_map),
            6 => self = Self::expand_from_other::<D, 6>(self, query_map),
            7 => self = Self::expand_from_other::<D, 7>(self, query_map),
            8 => self = Self::expand_from_other::<D, 8>(self, query_map),
            _ => self = Self::expand_from_other::<D, 16>(self, query_map),
        }

        self
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
        self.overflow_map.iter().find_map( | (v, bm) | {
            if v == val {
                Some(bm.clone())
            } else {
                None
            }
        }).unwrap_or_else( || {
            let bytes = val.get_hash().to_le_bytes();
            let mut result = self.get_bitmap(0, &bytes).clone();
            for i in 1..D {
                result.and_inplace(self.get_bitmap(i, &bytes));
            }
            result
        })
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.all_held().cardinality() == 0
    }

    #[inline(always)]
    pub fn all_held(&self) -> Bitmap {
        let refs: [&Bitmap; 256] = std::array::from_fn(|i| &self.supermap[0][i]);
        Bitmap::fast_or(&refs)
    }

    #[inline(always)]
    pub fn and_all(&mut self, keep: Bitmap) {
        for i in 0..D {
            for j in 0..255 {
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


    pub fn expand_from_other<const O: usize, const N: usize>(mut other: RadixMap<O>, qm: QueryMap) -> Self {
        let mut new_self = Self::default();
        // copy existing bits
        for i in 0..O {
            new_self.supermap[i] = std::mem::replace(
                &mut other.supermap[i],
                std::array::from_fn(|_| Bitmap::default())
            );
        }

        let reader  =qm.get_stored_items().read().unwrap();
        other.all_held().iter().for_each( | id | {
            let stored_item = reader.get(id as usize).unwrap();
            let values = stored_item.get_owned_handle().get_py_values();
            match values.get(&qm.attr_stored) {
                Some(pv) => {
                    for i in O..D {
                        new_self.get_bitmap_mut(i, &pv.get_hash().to_le_bytes()).add(id);
                    }
                }, 
                None => {}
            }
            // get bits for new size - get all existing PyValues
        });
        new_self
    }
}


impl <const D: usize>Default for RadixMap<D> {
    fn default() -> Self {
        Self {
            supermap: from_fn(|_| {
                // Each row is [Bitmap; 256]
                from_fn(|_| Bitmap::default())
            }),
            overflow_map: vec![],
        }
    }
}
