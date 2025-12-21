use std::sync::{Arc, Mutex};
use arc_swap::ArcSwap;
use bumpalo::Bump;
use smallvec::SmallVec;
use hashbrown::HashMap;
use std::ptr::NonNull;
use std::hash::BuildHasherDefault;
use rustc_hash::FxHasher;

use crate::index::core::structures::string_interner::InternedStr;
use crate::index::types::StrId;

type FxBuildHasher = BuildHasherDefault<FxHasher>;

pub struct ImmutableInterner {
    pub(crate) strings: Vec<InternedStr>,
    pub(crate) table: HashMap<(u64, u32), SmallVec<[StrId; 1]>, FxBuildHasher>,
}

impl ImmutableInterner {
    pub(crate) fn resolve(&self, id: StrId) -> &str {
        let stored = &self.strings[id as usize];
        let bytes: &[u8] = &stored.ptr;
        unsafe { std::str::from_utf8_unchecked(bytes) }
    }

    pub(crate) fn get(&self, s: &str) -> Option<StrId> {
        let hash = Self::hash_str(s);
        let len = s.len() as u32;

        self.table.get(&(hash, len)).and_then(|bucket| {
            for &id in bucket.iter() {
                let stored = &self.strings[id as usize];
                let bytes: &[u8] = &stored.ptr;
                if bytes == s.as_bytes() {
                    return Some(id);
                }
            }
            None
        })
    }

    pub(crate) fn hash_str(s: &str) -> u64 {
        use std::hash::Hasher;
        let mut h = FxHasher::default();
        h.write(s.as_bytes());
        h.finish()
    }

    pub(crate) fn len(&self) -> usize {
        self.strings.len()
    }
}