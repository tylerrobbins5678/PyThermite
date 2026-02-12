use std::sync::{Mutex, atomic::{AtomicU32, Ordering}};

use once_cell::sync::Lazy;



static GLOBAL_ID_COUNTER: AtomicU32 = AtomicU32::new(1);

static FREE_IDS: Lazy<Mutex<Vec<u32>>> = Lazy::new(|| Mutex::new(Vec::new()));


pub fn allocate_id() -> u32 {
    let mut free = FREE_IDS.lock().unwrap();

    if let Some(id) = free.pop() {
        id
    } else {
        GLOBAL_ID_COUNTER.fetch_add(1, Ordering::SeqCst)
    }
}

pub fn free_id(id: u32) {
    let mut free = FREE_IDS.lock().unwrap();
    free.push(id);
}
