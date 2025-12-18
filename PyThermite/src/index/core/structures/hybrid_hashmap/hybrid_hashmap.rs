use croaring::Bitmap;
use rustc_hash::FxHashMap;
use crate::index::{core::structures::hybrid_hashmap::RadixMap, value::PyValue};


pub enum HybridU32Hashmap {
    HashMap(FxHashMap<PyValue, Bitmap>),
    RadixMap(RadixMap<8>),
}