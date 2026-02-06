use std::sync::{Arc, RwLock, RwLockWriteGuard, Weak};

use ordered_float::OrderedFloat;
use pyo3::{Python, types::{PyListMethods, PySetMethods, PyTupleMethods}};

use crate::index::{core::{index::IndexAPI, query::{QueryMap, b_tree::Key}, stored_item::StoredItem, structures::{boolean_bitmap::BooleanBitmap, composite_key::CompositeKey128, hybrid_set::{HybridSet, HybridSetOps}, ordered_bitmap::NumericalBitmap, positional_bitmap::PositionalBitmap, shards::ShardedHashMap}}, types::StrId, value::{PyIterable, PyValue, RustCastValue, StoredIndexable}};



pub struct BulkQueryMapAdder<'a> {
    pub str_radix_map: RwLockWriteGuard<'a, PositionalBitmap>,
    pub num_ordered: RwLockWriteGuard<'a, NumericalBitmap>,
    pub bool_map: RwLockWriteGuard<'a, BooleanBitmap>,
    map: &'a QueryMap,
}

impl<'a> BulkQueryMapAdder<'a> {
    pub fn new(map: &'a QueryMap) -> Self {
        Self {
            str_radix_map: map.write_str_radix_map(),
            num_ordered: map.write_num_ordered(),
            bool_map: map.get_bool_map_writer(),
            map: map,
        }
    }

    #[inline(always)]
    pub fn insert(&mut self, value: &PyValue, obj_id: u32){
        // Insert into the right ordered map based on primitive type
        match &value.get_primitive() {
            RustCastValue::Int(i) => {
                //self.insert_exact(value, obj_id);
                self.insert_num_ordered(Key::Int(*i), obj_id);
            }
            RustCastValue::Float(f) => {
                //elf.insert_exact(value, obj_id);
                self.insert_num_ordered(Key::FloatOrdered(OrderedFloat(*f)), obj_id);
            }
            RustCastValue::Ind(index_obj) => {
                self.map.insert_exact(value, obj_id);
                self.map.insert_indexable(index_obj, obj_id);
            },
            RustCastValue::Iterable(py_iterable) => {
                self.map.insert_iterable(py_iterable, obj_id);
            }
            RustCastValue::Bool(b) => self.insert_bool(*b, obj_id),
            RustCastValue::Str(extracted_str) => {
                self.insert_str(extracted_str, obj_id);
                // self.insert_exact(value, obj_id);
            },
            RustCastValue::Unknown => {
                self.map.insert_exact(value, obj_id);
            },
        }
    }

    #[inline]
    fn insert_num_ordered(&mut self, key: Key, obj_id: u32){
        self.map.insert_delayed_num_ordered_from_guard(&mut self.num_ordered, key, obj_id);
    }

    #[inline]
    fn insert_str(&mut self, value: &str, obj_id: u32) {
        self.map.insert_str_from_guard(&mut self.str_radix_map, value, obj_id);
    }

    #[inline]
    fn insert_bool(&mut self, value: bool, obj_id: u32) {
        self.map.insert_bool_delayed_from_guard(&mut self.bool_map, value, obj_id);
    }
}

impl<'a> Drop for BulkQueryMapAdder<'a> {
    fn drop(&mut self) {
        // self.str_radix_map.flush();
        self.num_ordered.flush();
        self.bool_map.flush();
    }
}