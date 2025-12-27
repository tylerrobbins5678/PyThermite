use std::{ptr::NonNull, sync::{Arc, Mutex}};

use arc_swap::ArcSwap;

use crate::index::{core::structures::string_interner::{ImmutableInterner, MutableInterner}, types::StrId};



#[derive(Clone)]
pub struct InternedStr {
    pub(crate) ptr: Arc<[u8]>,
}

pub struct StrInterner {
    pub(crate) snapshot: ArcSwap<ImmutableInterner>,
    pub(crate) write_lock: Mutex<MutableInterner>,
}

impl StrInterner {
    pub fn with_capacity(cap: usize) -> Self {
        let mutable = MutableInterner::new(cap);
        let snapshot = Arc::new(mutable.freeze());
        Self {
            snapshot: ArcSwap::from(snapshot),
            write_lock: Mutex::new(mutable),
        }
    }

    pub fn intern(&self, s: &str) -> StrId {
        if let Some(id) = self.snapshot.load().get(s) {
            return id;
        }

        let mut lock = self.write_lock.lock().unwrap();
        let id = lock.intern(s);

        let new_snapshot = Arc::new(lock.freeze());
        self.snapshot.store(new_snapshot);

        id
    }

    pub fn resolve(&self, id: StrId) -> String {
        self.snapshot.load().resolve(id).to_owned()
    }

    pub fn len(&self) -> usize {
        self.snapshot.load().len()
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intern_thousand_strings() {
        let interner = StrInterner::with_capacity(1);
        
        for i in 0..1_000 {
            for _ in 0..2 {
                let s = format!("string_{}", i);
                let id = interner.intern(&s);
                let resolved = interner.resolve(id);
                assert_eq!(s, resolved);
            }
        }

        assert_eq!(interner.len(), 1_000);
    }

    #[test]
    fn same_string_returns_same_id() {
        let interner = StrInterner::with_capacity(16);

        let a = interner.intern("hello");
        let b = interner.intern("hello");

        assert_eq!(a, b);
        assert_eq!(interner.len(), 1);
        assert_eq!(interner.resolve(a), "hello");
    }

    #[test]
    fn different_strings_return_different_ids() {
        let interner = StrInterner::with_capacity(16);

        let a = interner.intern("hello");
        let b = interner.intern("world");

        assert_ne!(a, b);
        assert_eq!(interner.len(), 2);
        assert_eq!(interner.resolve(a), "hello");
        assert_eq!(interner.resolve(b), "world");
    }

    #[test]
    fn same_bytes_different_instances_dedup() {
        let interner = StrInterner::with_capacity(16);

        let s1 = String::from("abc");
        let s2 = String::from("abc");

        let a = interner.intern(&s1);
        let b = interner.intern(&s2);

        assert_eq!(a, b);
        assert_eq!(interner.len(), 1);
    }

    #[test]
    fn different_lengths_same_prefix_do_not_collide() {
        let interner = StrInterner::with_capacity(16);

        let a = interner.intern("abc");
        let b = interner.intern("abcd");

        assert_ne!(a, b);
        assert_eq!(interner.resolve(a), "abc");
        assert_eq!(interner.resolve(b), "abcd");
        assert_eq!(interner.len(), 2);
    }

    #[test]
    fn resolve_round_trip_for_many_strings() {
        let interner = StrInterner::with_capacity(128);

        let inputs = [
            "alpha", "beta", "gamma", "delta",
            "epsilon", "zeta", "eta", "theta",
        ];

        let ids: Vec<_> = inputs.iter().map(|s| interner.intern(s)).collect();

        for (id, &expected) in ids.iter().zip(inputs.iter()) {
            assert_eq!(interner.resolve(*id), expected);
        }

        assert_eq!(interner.len(), inputs.len());
    }

    #[test]
    fn ids_are_dense_and_stable() {
        let interner = StrInterner::with_capacity(16);

        let a = interner.intern("a");
        let b = interner.intern("b");
        let c = interner.intern("c");

        assert_eq!(a, 0);
        assert_eq!(b, 1);
        assert_eq!(c, 2);

        // Re-interning does not change IDs
        assert_eq!(interner.intern("a"), a);
        assert_eq!(interner.intern("b"), b);
        assert_eq!(interner.len(), 3);
    }
}
