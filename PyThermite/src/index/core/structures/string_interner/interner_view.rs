use std::sync::Arc;

use arc_swap::Guard;

use crate::index::{core::structures::string_interner::{ImmutableInterner, StrInterner}, types::StrId};



pub struct StrInternerView<'a> {
    interner: &'a StrInterner,
    snapshot: Guard<Arc<ImmutableInterner>>,
}

impl<'a> StrInternerView<'a> {
    pub fn new(interner: &'a StrInterner) -> Self {
        Self {
            interner,
            snapshot: interner.snapshot.load(),
        }
    }

    pub fn resolve(&self, id: StrId) -> &str {
        self.snapshot.resolve(id)
    }

    pub fn intern(&mut self, s: &str) -> StrId {
        if let Some(id) = self.snapshot.get(s) {
            return id;
        }

        let id = self.interner.intern(s);
        self.snapshot = self.interner.snapshot.load();

        id
    }

    pub fn len(&self) -> usize {
        self.snapshot.len()
    }
}