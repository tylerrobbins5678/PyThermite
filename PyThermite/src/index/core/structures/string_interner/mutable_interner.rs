use std::{hash::BuildHasherDefault, ptr::NonNull};

use bumpalo::Bump;
use hashbrown::HashMap;
use rustc_hash::FxHasher;
use smallvec::SmallVec;
use crate::index::core::structures::string_interner::{ImmutableInterner, InternedStr, immutable_interner::StrId};

type FxBuildHasher = BuildHasherDefault<FxHasher>;

pub struct MutableInterner {
    pub(crate) arena: Bump,
    pub(crate) strings: Vec<InternedStr>,
    pub(crate) table: HashMap<(u64, u32), SmallVec<[StrId; 1]>, FxBuildHasher>,
}

impl MutableInterner {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            arena: Bump::with_capacity(cap * 16),
            strings: Vec::with_capacity(cap),
            table: HashMap::with_capacity_and_hasher(cap, FxBuildHasher::default()),
        }
    }

    pub(crate) fn intern(&mut self, s: &str) -> StrId {
        let len = s.len() as u32;
        let hash = ImmutableInterner::hash_str(s);

        let entry = self.table.entry((hash, len)).or_default();

        for &id in entry.iter() {
            let stored = unsafe { self.strings.get_unchecked(id as usize) };
            let bytes = unsafe { std::slice::from_raw_parts(stored.ptr.as_ptr(), stored.len as usize) };
            if bytes == s.as_bytes() {
                return id;
            }
        }

        let dst = self.arena.alloc_slice_copy(s.as_bytes());
        let ptr = unsafe { NonNull::new_unchecked(dst.as_ptr() as *mut u8) };

        let id = self.strings.len() as StrId;
        self.strings.push(InternedStr { ptr, len });
        entry.push(id);

        id
    }

    pub(crate) fn freeze(&self) -> ImmutableInterner {
        ImmutableInterner {
            strings: self.strings.clone(),
            table: self.table.clone(),
        }
    }
}