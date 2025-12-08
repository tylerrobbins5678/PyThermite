use std::{collections::hash_map::Entry, ops::Deref, sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak}};

use rustc_hash::{FxBuildHasher, FxHashMap};
use croaring::Bitmap;
use ordered_float::OrderedFloat;
use pyo3::{Py, Python, types::{PyListMethods, PySetMethods, PyTupleMethods}};
use smallvec::SmallVec;
use smol_str::SmolStr;

const QUERY_DEPTH_LEN: usize = 12;

use crate::index::{Indexable, core::{query::{attr_parts, b_tree::{composite_key::CompositeKey128, ranged_b_tree::BitMapBTreeIter}}, structures::{hybrid_set::{HybridSet, HybridSetOps}, shards::ShardedHashMap}}, value::{PyIterable, PyValue, RustCastValue}};
use crate::index::core::index::IndexAPI;
use crate::index::core::stored_item::{StoredItem, StoredItemParent};
use crate::index::core::query::b_tree::{BitMapBTree, Key};

#[derive(Default)]
pub struct QueryMap {
    pub exact: ShardedHashMap<PyValue, HybridSet>,
    pub parent: Weak<IndexAPI>,
    pub num_ordered: RwLock<BitMapBTree>,
    pub nested: Arc<IndexAPI>,
}

unsafe impl Send for QueryMap {}
unsafe impl Sync for QueryMap {}

impl QueryMap {
    pub fn new(parent: Weak<IndexAPI>) -> Self{
        Self{
            exact: ShardedHashMap::<PyValue, HybridSet>::with_shard_count(16),
            parent: parent.clone(),
            num_ordered: RwLock::new(BitMapBTree::new()),
            nested: Arc::new(IndexAPI::new(Some(parent))),
        }
    }

    fn insert_exact(&self, value: &PyValue, obj_id: u32){
        let mut shard = self.exact.get_shard(&value);
        let entry = shard.entry(value.clone());
        match entry {
            Entry::Occupied(mut o) => {
                o.get_mut().add(obj_id);
            }
            Entry::Vacant(v) => {
                v.insert(HybridSet::of(&[obj_id]));
            }
        }
    }

    fn insert_num_ordered(&self, key: Key, obj_id: u32){
        let mut writer = self.write_num_ordered();
        writer.insert(key, obj_id);
    }

    fn remove_num_ordered(&self, key: Key, obj_id: u32){
        let mut writer = self.write_num_ordered();
        writer.remove(key, obj_id);
    }

    fn insert_indexable(&self, index_obj: &Arc<Py<Indexable>>, obj_id: u32){
        let mut path = HybridSet::new();

        if let Some(parent) = self.parent.upgrade() {
            path = parent.get_parents_from_stored_item(obj_id as usize);
        }

        let res = Python::with_gil(|py| {
            let index_obj_ref = index_obj.try_borrow(py).expect("cannot borrow, owned by other object");
            let id: u32 = index_obj_ref.id;

            if path.contains(id){
                return None;
            }
            let py_values = index_obj_ref.get_py_values().clone();

            // register the index in the object
            let weak_nested = Arc::downgrade(&self.nested);
            index_obj_ref.add_index(weak_nested.clone());
            Some((id, py_values, weak_nested))
        });

        if let Some((id, py_values, weak_nested)) = res {
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

                let stored_item = StoredItem::new(index_obj.clone(), Some(stored_parent));
                self.nested.add_object(weak_nested, id, stored_item, py_values);
            }
        }
    }

    fn insert_iterable(&self, iterable: &PyIterable, obj_id: u32){
        Python::with_gil(|py| {
            match iterable {
                PyIterable::Dict(py_dict) => {
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

    pub fn insert(&self, value: &PyValue, obj_id: u32){
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
            
            RustCastValue::Str(_) => {
                self.insert_exact(value, obj_id);
            },
            RustCastValue::Unknown => {
                self.insert_exact(value, obj_id);
            },
        }
    }

    pub fn check_prune(&self, val: &PyValue) {
        let mut shard = self.exact.get_shard(&val);
        if let Some(ev) = shard.get(val) {
            if ev.is_empty() {
                shard.remove(val); // no clone needed
            }
        }
    }

    pub fn merge(&self, other: &Self) {
        // Iterate over all values in `self` mutably
        self.exact.for_each_mut(|key_self, bm_self| {
            // Try to get the corresponding value from `other`
            if let Some(bm_other) = other.exact.get(key_self) {
                bm_self.or_inplace(&bm_other);
            }
        });
    }

    pub fn is_empty(&self) -> bool {
        self.exact.is_empty()
    }

    pub fn get<'a>(
        &self,
        guard: &'a RwLockReadGuard<FxHashMap<PyValue, HybridSet>>,
        key: &PyValue,
    ) -> Option<&'a HybridSet> {
        guard.get(key)
    }

    pub fn get_mut<'a>(
        &self,
        guard: &'a mut RwLockWriteGuard<FxHashMap<PyValue, HybridSet>>,
        key: &PyValue,
    ) -> Option<&'a mut HybridSet> {
        guard.get_mut(key)
    }

    fn remove_exact(&self, py_value: &PyValue, idx: u32) {
        let mut shard = self.exact.get_shard(py_value);
        if let Some(hs) = shard.get_mut(py_value){
            hs.remove(idx);
        }
    }

    fn remove_iterable(&self, iterable: &PyIterable, obj_id: u32) {
        Python::with_gil(|py| {
            match iterable {
                PyIterable::Dict(py_dict) => {
    //                let dict = py_dict.bind(py);
    //                dict.iter().for_each(|(k, v)| {
    //                    self.iterable.entry(k).or_insert(k)
    //                });
                },

                PyIterable::List(py_list) => {
                    for item in py_list.bind(py).iter(){
                        self.remove_id(&PyValue::new(item), obj_id);
                    }
                },
                PyIterable::Tuple(py_tuple) => {
                    for item in py_tuple.bind(py).iter(){
                        self.remove_id(&PyValue::new(item), obj_id);
                    }
                }
                PyIterable::Set(py_set) => {
                    for item in py_set.bind(py).iter(){
                        self.remove_id(&PyValue::new(item), obj_id);
                    }
                },
            }
        })
    }

    pub fn remove_id(&self, py_value: &PyValue, idx: u32) {
        match &py_value.get_primitive(){
            RustCastValue::Int(i) => {
                // self.remove_exact(py_value, idx);
                self.remove_num_ordered(Key::Int(*i), idx);
            }
            RustCastValue::Float(f) => {
                // self.remove_exact(py_value, idx);
                self.remove_num_ordered(Key::FloatOrdered(OrderedFloat(*f)), idx);
            }
            RustCastValue::Str(_) => {
                self.remove_exact(py_value, idx);
            },
            RustCastValue::Ind(indexable) => {
                self.remove_exact(py_value, idx);
                Python::with_gil(| py | {
                    let to_remove = indexable.borrow(py);
                    self.nested.remove(to_remove.deref(), idx);
                });
            },
            RustCastValue::Iterable(py_iterable) => {
                self.remove_iterable(py_iterable, idx);
            },
            RustCastValue::Unknown => {
                self.remove_exact(py_value, idx);
            },
        };
    }

    pub fn remove(&self, filter_bm: &HybridSet) {
        self.exact.for_each_mut(|_, bm| {
            bm.and_inplace(filter_bm);
        });
    }

    pub fn group_by(&self, sub_query: SmolStr) -> Option<SmallVec<[(PyValue, HybridSet); QUERY_DEPTH_LEN]>> {
        let (_, parts) = attr_parts(sub_query);
        match parts {
            Some(rest) => {
                let groups = self.nested.group_by(rest);
                if let Some(r) = groups {
                    
                    let mut res: SmallVec<[(PyValue, HybridSet); QUERY_DEPTH_LEN]> = SmallVec::new();
                    for (py_value, allowed) in r {
                        let allowed_parents = self.get_allowed_parents(&allowed.as_bitmap());
                        res.push((py_value, allowed_parents));
                    }
                    Some(res)
                } else {
                    None
                }
            },
            None => {
                let mut res:SmallVec<[(PyValue, HybridSet); QUERY_DEPTH_LEN]> = SmallVec::new();
                self.exact.for_each(|k, v| {
                    res.push((k.clone(), v.clone()));
                });

                let iter_guard = &self.read_num_ordered();
                let bitmap_iter = BitMapBTreeIter::new(iter_guard);

                let mut current_val: Option<u128> = None;
                let mut current_bitmap: Bitmap = Bitmap::new();

                for composite_key in bitmap_iter {
                    let id = composite_key.get_id();
                    let key_val = composite_key.get_value_bits();

                    if Some(key_val) != current_val {
                        // push previous group if exists
                        if let Some(cv) = current_val {
                            let pyval = PyValue::from_primitave(RustCastValue::Float(CompositeKey128::decode_float96(cv)));
                            let hset = HybridSet::Large(current_bitmap.clone());
                            res.push((pyval, hset));
                            current_bitmap.clear();
                        }
                        current_val = Some(key_val);
                    }

                    current_bitmap.add(id);
                }

                // push last group
                if let Some(cv) = current_val {
                    let pyval = PyValue::from_primitave(RustCastValue::Float(CompositeKey128::decode_float96(cv)));
                    let hset = HybridSet::Large(current_bitmap);
                    res.push((pyval, hset));
                }

                Some(res)
            },
        }
    }

    pub fn get_allowed_parents(&self, child_bm: &Bitmap) -> HybridSet {
        self.nested.get_direct_parents(child_bm)
    }

}


impl QueryMap {
    pub fn read_num_ordered(&self) -> std::sync::RwLockReadGuard<'_, BitMapBTree> {
        self.num_ordered.read().unwrap()
    }
    pub fn write_num_ordered(&self) -> std::sync::RwLockWriteGuard<'_, BitMapBTree> {
        self.num_ordered.write().unwrap()
    }
}