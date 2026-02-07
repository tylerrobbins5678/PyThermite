use rustc_hash::FxHashMap;
use smallvec::SmallVec;
use std::{borrow::Borrow, hash::Hash, mem::MaybeUninit, ptr};

use crate::index::value;
const SMALL_SIZE: usize = 8;

#[derive(Debug)]
struct SmallKVMap<K, V> {
    keys: [MaybeUninit<K>; SMALL_SIZE],
    values: [MaybeUninit<V>; SMALL_SIZE],
    len: usize,
}

impl<K: PartialEq, V> SmallKVMap<K, V> {

    pub fn new() -> Self {
        Self {
            keys: [const { MaybeUninit::uninit() }; SMALL_SIZE],
            values: [const { MaybeUninit::uninit() }; SMALL_SIZE],
            len: 0,
        }
    }

    fn iter_mut(&mut self) -> impl Iterator<Item = (&K, &mut V)> {
        let len = self.len as usize;
        self.keys[..len]
            .iter()
            .map(|k| unsafe { k.assume_init_ref() })
            .zip(self.values[..len].iter_mut()
            .map(|v| unsafe { v.assume_init_mut() }))
    }

    fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        let len = self.len as usize;
        self.keys[..len]
            .iter()
            .map(|k| unsafe { k.assume_init_ref() })
            .zip(self.values[..len].iter()
            .map(|v| unsafe { v.assume_init_ref() }))
    }

    fn get<Q: ?Sized>(&self, key: &Q) -> Option<&V>
        where
            K: Borrow<Q>,
            Q: Eq + Hash,
        {
            let len = self.len;

            for i in 0..len {
                unsafe {
                    let k_ref: &K = self.keys[i].assume_init_ref();
                    if k_ref.borrow() == key {
                        return Some(self.values[i].assume_init_ref());
                    }
                }
            }

            None
        }

    fn len(&self) -> usize {
        self.len
    }

    fn push(&mut self, key: K, val: V) {
        self.keys[self.len].write(key);
        self.values[self.len as usize].write(val);
        self.len += 1;
    }

    fn drain(self) -> Drain<K, V> {
        Drain { small: self, idx: 0 }
    }
}

impl<K, V> Drop for SmallKVMap<K, V> {
    fn drop(&mut self) {
        let len = self.len as usize;
        for i in 0..len {
            unsafe {
                self.values[i].assume_init_drop();
            }
        }
    }
}

struct Drain<K, V> {
    small: SmallKVMap<K, V>,
    idx: usize,
}

impl<K, V> Iterator for Drain<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.small.len {
            return None;
        }

        let i = self.idx;
        self.idx += 1;

        unsafe {
            Some((
                self.small.keys[i].assume_init_read(),
                self.small.values[i].assume_init_read(),
            ))
        }
    }
}

#[derive(Debug)]
pub enum HybridHashmap<K, V> {
    Small(SmallKVMap<K, V>),
    Map(FxHashMap<K, V>),
}

impl<K, V> HybridHashmap<K, V>
where
    K: Eq + Hash,
{
    pub fn new() -> Self {
        HybridHashmap::Small(SmallKVMap::new())
    }

    #[inline(always)]
    pub fn insert(&mut self, key: K, value: V) {
        match self {
            HybridHashmap::Small(vec) => {
                for (k, v) in vec.iter_mut() {
                    if *k == key {
                        *v = value;
                        return;
                    }
                }
                if vec.len() < SMALL_SIZE {
                    vec.push(key, value);
                } else {
                    let mut map = FxHashMap::with_capacity_and_hasher(16, Default::default());
                    unsafe {
                        // drain owned small and reassign to enum variant to make this "large"
                        let small = ptr::read(vec);
                        for (k,v) in small.drain(){
                            map.insert(k, v);
                        }
                        map.insert(key, value);
                        *self = HybridHashmap::Map(map);
                    }
                }
            }
            HybridHashmap::Map(map) => { map.insert(key, value); }
        }
    }

    #[inline(always)]
    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        match self {
            HybridHashmap::Small(vec) => vec.get(key),
            HybridHashmap::Map(map) => map.get(key),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            HybridHashmap::Small(vec) => vec.len(),
            HybridHashmap::Map(map) => map.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = (&K, &V)> + '_> {
        match self {
            HybridHashmap::Small(vec) => Box::new(vec.iter().map(|(k, v)| (k, v))),
            HybridHashmap::Map(map) => Box::new(map.iter()),
        }
    }

    pub fn keys(&self) -> Box<dyn Iterator<Item = &K> + '_> {
        match self {
            HybridHashmap::Small(vec) => Box::new(vec.iter().map(|(k, _)| k)),
            HybridHashmap::Map(map) => Box::new(map.keys()),
        }
    }
}
