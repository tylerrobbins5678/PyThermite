use std::sync::{Arc, RwLock, RwLockWriteGuard, Weak};

use ordered_float::OrderedFloat;
use pyo3::{Python, types::{PyListMethods, PySetMethods, PyTupleMethods}};

use crate::index::{core::{index::IndexAPI, query::{QueryMap, b_tree::Key}, stored_item::{StoredItem, StoredItemParent}, structures::{composite_key::CompositeKey128, hybrid_set::{HybridSet, HybridSetOps}, ordered_bitmap::NumericalBitmap, positional_bitmap::PositionalBitmap, shards::ShardedHashMap}}, types::StrId, value::{PyIterable, PyValue, RustCastValue, StoredIndexable}};



pub struct BulkQueryMapAdder<'a> {
    pub str_radix_map: RwLockWriteGuard<'a, PositionalBitmap>,
    pub num_ordered: RwLockWriteGuard<'a, NumericalBitmap>,

    pub exact: ShardedHashMap<PyValue, HybridSet>,
    pub parent: Weak<IndexAPI>,
    pub nested: Arc<IndexAPI>,
}

impl<'a> BulkQueryMapAdder<'a> {
    pub fn new(map: &'a QueryMap) -> Self {
        Self {
            str_radix_map: map.write_str_radix_map(),
            num_ordered: map.write_num_ordered(),
            exact: map.exact.clone(),
            parent: map.parent.clone(),
            nested: map.nested.clone(),
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
                self.insert_exact(value, obj_id);
                self.insert_indexable(index_obj, obj_id);
            },
            RustCastValue::Iterable(py_iterable) => {
                self.insert_iterable(py_iterable, obj_id);
            }
            RustCastValue::Bool(_) => self.insert_exact(value, obj_id),
            RustCastValue::Str(extracted_str) => {
                self.insert_str(extracted_str, obj_id);
                // self.insert_exact(value, obj_id);
            },
            RustCastValue::Unknown => {
                self.insert_exact(value, obj_id);
            },
        }
    }

    fn insert_num_ordered(&mut self, key: Key, obj_id: u32){
        let composit_key = CompositeKey128::new(key, obj_id);
        self.num_ordered.add_delayed(composit_key.get_value_bits(), obj_id);
    }

    #[inline]
    fn insert_str(&mut self, value: &str, obj_id: u32) {
        self.str_radix_map.add(value, obj_id);
    }

    pub(crate) fn insert_indexable(&self, index_obj: &StoredIndexable, obj_id: u32){
        let mut path = HybridSet::new();

        if let Some(parent) = self.parent.upgrade() {
            path = parent.get_parents_from_stored_item(obj_id as usize);
        }

        let id: u32 = index_obj.owned_handle.id;

        if path.contains(id){
            return;
        }

        // register the index in the object
        let weak_nested = Arc::downgrade(&self.nested);
        index_obj.owned_handle.add_index(weak_nested.clone());

        if self.nested.has_object_id(id) {
            self.nested.register_path(id, obj_id);
        } else {
            let mut hs = HybridSet::new();
            hs.add(obj_id);
            let stored_parent = StoredItemParent {
                ids: hs,
                path_to_root: path,
                index: weak_nested.clone(),
            };

            let stored_item = StoredItem::new(index_obj.python_handle.clone(), index_obj.owned_handle.clone(), Some(stored_parent));
            let py_values = index_obj.owned_handle.get_py_values();
            self.nested.add_object(weak_nested, id, stored_item, py_values);
        }
    }

    #[inline(always)]
    pub(crate) fn insert_exact(&self, value: &PyValue, obj_id: u32){
        let mut shard = self.exact.get_shard(&value);
        match shard.get_mut (value) {
            Some(hs) => {
                hs.add(obj_id);
            }
            None => {
                shard.insert(value.clone(), HybridSet::of(&[obj_id]));
            }
        }
    }

    fn insert_iterable(&mut self, iterable: &PyIterable, obj_id: u32){
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
                        self.insert(&PyValue::new(item), obj_id);
                    }
                },
                PyIterable::Tuple(py_tuple) => {
                    for item in py_tuple.bind(py).iter(){
                        self.insert(&PyValue::new(item), obj_id);
                    }
                }
                PyIterable::Set(py_set) => {
                    for item in py_set.bind(py).iter(){
                        self.insert(&PyValue::new(item), obj_id);
                    }
                },
            }
        });
    }
}

impl<'a> Drop for BulkQueryMapAdder<'a> {
    fn drop(&mut self) {
        // self.str_radix_map.flush();
        self.num_ordered.flush();
    }
}