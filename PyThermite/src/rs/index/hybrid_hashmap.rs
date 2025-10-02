use rustc_hash::FxHashMap;
use smallvec::SmallVec;
use smol_str::SmolStr;
use std::{borrow::Borrow, hash::Hash};

#[derive(Clone, Debug)]
pub enum HybridHashmap<K, V> {
    Small(SmallVec<[(K, V); 8]>),
    Map(FxHashMap<K, V>),
}

impl<K, V> HybridHashmap<K, V>
where
    K: Eq + Hash,
{
    pub fn insert(&mut self, key: K, value: V) {
        match self {
            HybridHashmap::Small(vec) => {
                for (k, v) in vec.iter_mut() {
                    if *k == key {
                        *v = value;
                        return;
                    }
                }
                if vec.len() < 8 {
                    vec.push((key, value));
                } else {
                    let mut map = FxHashMap::with_capacity_and_hasher(16, Default::default());
                    for (k, v) in vec.drain(..) {
                        map.insert(k, v);
                    }
                    map.insert(key, value);
                    *self = HybridHashmap::Map(map);
                }
            }
            HybridHashmap::Map(map) => { map.insert(key, value); }
        }
    }

    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        match self {
            HybridHashmap::Small(vec) => vec.iter()
                .find(|(k, _)| k.borrow() == key)
                .map(|(_, v)| v),
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