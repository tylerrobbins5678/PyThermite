use std::sync::{Arc, RwLock, RwLockWriteGuard, Weak};

use croaring::Bitmap;
use ordered_float::OrderedFloat;
use pyo3::{Python, types::{PyListMethods, PySetMethods, PyTupleMethods}};
use rustc_hash::FxHashMap;

use crate::index::{core::{id_alloc::allocate_id, index::IndexAPI, query::{QueryMap, b_tree::Key}, stored_item::StoredItem, structures::{boolean_bitmap::BooleanBitmap, composite_key::CompositeKey128, hybrid_set::{HybridSet, HybridSetOps}, ordered_bitmap::NumericalBitmap, positional_bitmap::PositionalBitmap, shards::ShardedHashMap}}, types::StrId, value::{PyIterable, PyValue, RustCastValue, StoredIndexable}};



pub struct BulkQueryMapAdder<'a> {
    pub str_radix_map: RwLockWriteGuard<'a, PositionalBitmap>,
    pub num_ordered: RwLockWriteGuard<'a, NumericalBitmap>,
    pub bool_map: RwLockWriteGuard<'a, BooleanBitmap>,
    pub mapped_ids: RwLockWriteGuard<'a, FxHashMap<u32, u32>>,
    pub masked_ids: RwLockWriteGuard<'a, Bitmap>,
    map: &'a QueryMap,
}

impl<'a> BulkQueryMapAdder<'a> {
    pub fn new(map: &'a QueryMap) -> Self {
        Self {
            str_radix_map: map.write_str_radix_map(),
            num_ordered: map.write_num_ordered(),
            bool_map: map.get_bool_map_writer(),
            mapped_ids: map.get_mapped_ids_writer(),
            masked_ids: map.get_masked_ids_writer(),
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
                self.insert_iterable(py_iterable, obj_id);
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


    pub fn insert_iterable(&mut self, iterable: &PyIterable, obj_id: u32){
        Python::with_gil(|py| {
            match iterable {
                PyIterable::Dict(_) => {
//                    let dict = py_dict.bind(py);
//                    dict.iter().for_each(|(k, v)| {
//                        self.iterable.entry(k).or_insert(k)
//                    });
                },

                PyIterable::List(py_list) => {
                    for item in py_list.bind(py).iter(){
                        let index_id = allocate_id();
                        self.mapped_ids.insert(index_id, obj_id);
                        self.masked_ids.add(index_id);
                        self.insert(&PyValue::new(item), index_id);
                    }
                },
                PyIterable::Tuple(py_tuple) => {
                    for item in py_tuple.bind(py).iter(){
                        let index_id = allocate_id();
                        self.mapped_ids.insert(index_id, obj_id);
                        self.masked_ids.add(index_id);
                        self.insert(&PyValue::new(item), index_id);
                    }
                }
                PyIterable::Set(py_set) => {
                    for item in py_set.bind(py).iter(){
                        let index_id = allocate_id();
                        self.mapped_ids.insert(index_id, obj_id);
                        self.masked_ids.add(index_id);
                        self.insert(&PyValue::new(item), index_id);
                    }
                },
            }
        });
    }

    #[inline]
    fn insert_num_ordered(&mut self, key: Key, obj_id: u32){
        let composit_key = CompositeKey128::new(key, obj_id);
        self.num_ordered.add_delayed(composit_key.get_value_bits(), obj_id);
    }

    #[inline]
    fn insert_str(&mut self, value: &str, obj_id: u32) {
        self.str_radix_map.add_delayed(value, obj_id);
    }

    #[inline]
    fn insert_bool(&mut self, value: bool, obj_id: u32) {
        self.bool_map.add_delayed(value, obj_id);
    }
}

impl<'a> Drop for BulkQueryMapAdder<'a> {
    fn drop(&mut self) {
        // self.str_radix_map.flush();
        self.num_ordered.flush();
        self.bool_map.flush();
        self.str_radix_map.flush();
    }
}