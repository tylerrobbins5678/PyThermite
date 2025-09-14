use std::{cell::UnsafeCell};
use crate::index::value::PyValue;
use rustc_hash::FxHashMap;

pub struct UnsafePyValues {
    map: UnsafeCell<FxHashMap<String, PyValue>>,
}

unsafe impl Sync for UnsafePyValues {}

impl UnsafePyValues {
    pub fn new(default: FxHashMap<String, PyValue>) -> Self {
        Self { map: UnsafeCell::new(default) }
    }

    /// Safe API for external Python access (with lock)
    pub fn lock_and_insert(&self, key: String, val: PyValue, lock: &std::sync::Mutex<()>) {
        let _guard = lock.lock().unwrap();
        unsafe { (*self.map.get()).insert(key, val); }
    }

    pub fn lock_and_get(&self, key: &str, lock: &std::sync::Mutex<()>) -> Option<PyValue> {
        let _guard = lock.lock().unwrap();
        unsafe { (*self.map.get()).get(key).cloned() }
    }

    /// Internal API: clone whole map without locking (use responsibly!)
    pub unsafe fn clone_map(&self) -> FxHashMap<String, PyValue> {
        (*self.map.get()).clone()
    }

    /// Internal API: get reference (no cloning or locking)
    pub unsafe fn map_ref(&self) -> &FxHashMap<String, PyValue> {
        &*self.map.get()
    }

    /// Internal API: get mutable
    pub unsafe fn get_mut(&self) -> &mut FxHashMap<String, PyValue> {
        &mut *self.map.get()
    }

}