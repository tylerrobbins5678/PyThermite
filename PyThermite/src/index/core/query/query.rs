use std::{ops::Deref, sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak}};

use rustc_hash::FxHashMap;
use croaring::Bitmap;
use ordered_float::OrderedFloat;
use pyo3::{Py, Python, types::{PyListMethods, PySetMethods, PyTupleMethods}};
use smallvec::SmallVec;
use smol_str::SmolStr;

const QUERY_DEPTH_LEN: usize = 12;

use crate::index::{HybridSet, Indexable, core::query::attr_parts, value::{PyIterable, PyValue, RustCastValue}};
use crate::index::core::index::IndexAPI;
use crate::index::core::stored_item::{StoredItem, StoredItemParent};
use crate::index::core::query::b_tree::{BitMapBTree, Key};

#[derive(Default)]
pub struct QueryMap {
    pub exact: RwLock<FxHashMap<PyValue, HybridSet>>,
    pub parent: Weak<IndexAPI>,
    pub num_ordered: RwLock<BitMapBTree>,
    pub nested: Arc<IndexAPI>,
}

unsafe impl Send for QueryMap {}
unsafe impl Sync for QueryMap {}

impl QueryMap {
    pub fn new(parent: Weak<IndexAPI>) -> Self{
        Self{
            exact: RwLock::new(FxHashMap::default()),
            parent: parent.clone(),
            num_ordered: RwLock::new(BitMapBTree::new()),
            nested: Arc::new(IndexAPI::new(Some(parent))),
        }
    }

    fn insert_exact(&self, value: &PyValue, obj_id: u32){
        let mut writer = self.write_exact();
        if let Some(existing) = writer.get_mut(&value) {
            existing.add(obj_id);
        } else {
            // lazily create only if needed
            let hybrid_set = HybridSet::of(&[obj_id]);
            writer.insert(value.clone(), hybrid_set);
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
                self.insert_exact(value, obj_id);
                self.insert_num_ordered(Key::Int(*i), obj_id);
            }
            RustCastValue::Float(f) => {
                self.insert_exact(value, obj_id);
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
        let reader = self.read_exact();
        if reader.contains_key(val) && reader[val].is_empty(){
            drop(reader);
            let mut writer = self.write_exact();
            writer.remove(val);
        }
    }

    pub fn merge(&self, other: &Self) {
        let mut writer = self.write_exact();
        let other_reader = other.read_exact();
        for (val, bm) in writer.iter_mut() {
            if let Some(other) = other.get(&other_reader, &val){
                bm.or_inplace(other);
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.read_exact().is_empty()
    }

    pub fn contains(&self, key: &PyValue) -> bool{
        self.read_exact().contains_key(key)
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
        let reader = self.read_exact();
        if let Some(_) = reader.get(py_value) {
            drop(reader);
            let mut writer = self.write_exact();
            let hybrid_set = writer.get_mut(py_value).unwrap();
            hybrid_set.remove(idx);
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
                self.remove_exact(py_value, idx);
                self.remove_num_ordered(Key::Int(*i), idx);
            }
            RustCastValue::Float(f) => {
                self.remove_exact(py_value, idx);
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

    pub fn remove(&self, filter_bm: &HybridSet){
        let mut writer = self.write_exact();
        for (_, bm) in writer.iter_mut() {
            bm.and_inplace(filter_bm);
        }
    }

    pub fn group_by(&self, sub_query: Option<SmolStr>) -> Option<SmallVec<[(PyValue, HybridSet); QUERY_DEPTH_LEN]>> {
        if let Some(sub_q) = sub_query {
            let (_, parts) = attr_parts(sub_q);
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
                    let reader = self.read_exact();
                    for (k, v) in reader.iter() {
                        res.push((k.clone(), v.clone()));
                    }
                    Some(res)
                },
            }
        } else{
            None
        }
    }

    pub fn get_allowed_parents(&self, child_bm: &Bitmap) -> HybridSet {
        self.nested.get_direct_parents(child_bm)
    }

}


impl QueryMap {
    pub fn read_exact(&self) -> std::sync::RwLockReadGuard<'_, FxHashMap<PyValue, HybridSet>> {
        self.exact.read().unwrap()
    }
    pub fn write_exact(&self) -> std::sync::RwLockWriteGuard<'_, FxHashMap<PyValue, HybridSet>> {
        self.exact.write().unwrap()
    }
    pub fn read_num_ordered(&self) -> std::sync::RwLockReadGuard<'_, BitMapBTree> {
        self.num_ordered.read().unwrap()
    }
    pub fn write_num_ordered(&self) -> std::sync::RwLockWriteGuard<'_, BitMapBTree> {
        self.num_ordered.write().unwrap()
    }
}