use std::{collections::HashMap, hash::{BuildHasher, Hash, Hasher}, sync::{RwLock, RwLockWriteGuard}};

use rustc_hash::FxBuildHasher;


pub struct ShardedHashMap<K, V> {
    shards: Box<[RwLock<HashMap<K, V>>]>,
    mask: usize,
}


impl<K, V> ShardedHashMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone
{
    pub fn with_shard_count(shard_count: usize) -> Self {
        assert!(shard_count.is_power_of_two());

        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            let map: HashMap<K, V> = HashMap::new();
            shards.push(RwLock::new(map));
        }

        Self {
            shards: shards.into_boxed_slice(),
            mask: shard_count - 1,
        }
    }
}


impl<K, V> ShardedHashMap<K, V>
where
    K: Hash,
{
    #[inline]
    fn shard_for(&self, key: &K) -> usize {
        FxBuildHasher::default().build_hasher();
        let mut h = FxBuildHasher::default().build_hasher();
        key.hash(&mut h);
        (h.finish() as usize) & self.mask
    }
}


impl<K, V> ShardedHashMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let shard_idx = self.shard_for(&key);
        let mut guard = self.shards[shard_idx].write().unwrap();
        guard.insert(key, value)
    }

    pub fn get(&self, key: &K) -> Option<V> {
        let shard_idx = self.shard_for(key);
        let guard = self.shards[shard_idx].read().unwrap();
        guard.get(key).cloned()
    }

    pub fn get_shard(&self, key: &K) -> RwLockWriteGuard<HashMap<K, V>> {
        let shard_idx = self.shard_for(key);
        let guard = self.shards[shard_idx].write().unwrap();
        guard
    }

    pub fn get_mut(&self, key: &K) -> Option<RwLockWriteGuard<HashMap<K, V>>> {
        let shard_idx = self.shard_for(key);
        let guard = self.shards[shard_idx].write().unwrap();

        if guard.contains_key(key) {
            Some(guard) // return the whole write guard
        } else {
            None
        }
    }

    pub fn for_each<F: FnMut(&K, &V)>(&self, mut f: F) {
        for shard in &self.shards {
            let guard = shard.read().unwrap();
            for (k, v) in guard.iter() {
                f(k, v);
            }
        }
    }

    pub fn for_each_mut<F: FnMut(&K, &mut V)>(&self, mut f: F) {
        for shard in &self.shards {
            let mut guard = shard.write().unwrap();
            for (k, v) in guard.iter_mut() { // <-- iter_mut() gives &mut V
                f(k, v);
            }
        }
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        let shard_idx = self.shard_for(key);
        let mut guard = self.shards[shard_idx].write().unwrap();
        guard.remove(key)
    }

    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|shard| shard.read().unwrap().is_empty())
    }

}

impl<K, V> Default for ShardedHashMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone
{
    fn default() -> Self {
        // Pick a reasonable default shard count, must be a power of two
        Self::with_shard_count(16)
    }
}


#[cfg(test)]
mod tests {
    use super::ShardedHashMap;
    use std::collections::HashSet;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn basic_operations() {
        let map = ShardedHashMap::with_shard_count(4);

        // insert
        assert_eq!(map.insert("a", 1), None);
        assert_eq!(map.insert("b", 2), None);
        assert_eq!(map.insert("a", 3), Some(1)); // replacing existing

        // get
        assert_eq!(map.get(&"a"), Some(3));
        assert_eq!(map.get(&"b"), Some(2));
        assert_eq!(map.get(&"c"), None);

        // remove
        assert_eq!(map.remove(&"a"), Some(3));
        assert_eq!(map.get(&"a"), None);
    }

    #[test]
    fn concurrent_insert_get() {
        let map = Arc::new(ShardedHashMap::with_shard_count(8));
        let threads = 4;
        let barrier = Arc::new(Barrier::new(threads));

        let mut handles = Vec::new();

        for i in 0..threads {
            let map = Arc::clone(&map);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait(); // synchronize start
                for j in 0..100 {
                    let key = format!("key-{}", i * 100 + j);
                    map.insert(key.clone(), i * 100 + j);
                    assert_eq!(map.get(&key), Some(i * 100 + j));
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Check total keys
        let mut values = HashSet::new();
        for i in 0..threads * 100 {
            let key = format!("key-{}", i);
            values.insert(map.get(&key).unwrap());
        }
        assert_eq!(values.len(), threads * 100);
    }
}
