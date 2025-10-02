use std::sync::Mutex;

use once_cell::sync::Lazy;
use rustc_hash::FxHashMap;
use smol_str::SmolStr;

use crate::index::value::PyValue;

pub struct SmolStrPyvalueHashMapPool {
    pool: Vec<FxHashMap<SmolStr, PyValue>>,
}

impl SmolStrPyvalueHashMapPool {

    pub fn new() -> Self {
        Self {
            pool: SmolStrPyvalueHashMapPool::alloc_inner().into()
        }
    }

    fn alloc_inner() -> Vec<FxHashMap<SmolStr, PyValue>> {
        let mut v = Vec::with_capacity(64);
        for _ in 0..64 {
            v.push(FxHashMap::with_capacity_and_hasher(16, Default::default()));
        }
        v
    }

    pub fn get(&mut self) -> FxHashMap<SmolStr, PyValue> {
        if let Some(res) = self.pool.pop() {
            return res;
        } else {
            self.pool.extend(Self::alloc_inner());
            self.pool.pop().unwrap()
        }
    }
}