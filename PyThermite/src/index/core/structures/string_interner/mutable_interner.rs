use std::{hash::BuildHasherDefault, ptr::NonNull, sync::Arc};

use bumpalo::Bump;
use hashbrown::HashMap;
use rustc_hash::FxHasher;
use smallvec::SmallVec;
use crate::index::{core::structures::string_interner::{ImmutableInterner, InternedStr}, types::StrId};

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
            let stored = &self.strings[id as usize];
            let bytes: &[u8] = &stored.ptr;
            if bytes == s.as_bytes() {
                return id;
            }
        }

        let ptr = Arc::from(s.as_bytes());

        let id = self.strings.len() as StrId;
        self.strings.push(InternedStr { ptr });
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