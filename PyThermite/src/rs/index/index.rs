
use std::{fmt, ops::Deref, sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak}, time::Instant, vec};
use croaring::Bitmap;
use pyo3::prelude::*;
use rand::seq::IndexedRandom;
use rustc_hash::FxHashMap;
use smol_str::SmolStr;

use crate::index::{filtered_index::FilteredIndex, query::{evaluate_query, filter_index_by_hashes, kwargs_to_hash_query, PyQueryExpr, QueryMap}, stored_item, HybridHashmap, HybridSet, Indexable};

use super::stored_item::StoredItem;
use super::value::PyValue;


#[pyclass]
pub struct Index {
    pub inner: Arc<IndexAPI>
}

#[pymethods]
impl Index {
    #[new]
    pub fn new() -> Self {
        let index = IndexAPI::new(None);
        Self {
            inner: Arc::new(index)
        }
    }

    pub fn collect(&self, py: Python) -> PyResult<Vec<Py<Indexable>>> {
        self.inner.collect(py)
    }

    #[pyo3(signature = (**kwargs))]
    pub fn reduced<'py>(
        &self,
        py: Python,
        kwargs: Option<FxHashMap<String, pyo3::Bound<'py, PyAny>>>,
    ) -> PyResult<FilteredIndex> {
        let query = kwargs_to_hash_query(kwargs.unwrap_or_default())?;
        py.allow_threads(move || {
            Ok(self.inner.reduced(query))
        })
    }

    #[pyo3(signature = (**kwargs))]
    pub fn reduce<'py>(
        &mut self,
        py: Python,
        kwargs: Option<FxHashMap<String, pyo3::Bound<'py, PyAny>>>,
    ) -> PyResult<()> {
        self.inner.reduce(Arc::downgrade(&self.inner), py, kwargs)
    }

    #[pyo3(signature = (**kwargs))]
    pub fn get_by_attribute<'py>(
        &self,
        py: Python,
        kwargs: Option<FxHashMap<String, pyo3::Bound<'py, PyAny>>>,
    ) -> PyResult<Vec<Py<Indexable>>> {
        let query = kwargs_to_hash_query(kwargs.unwrap_or_default())?;
        let allowed = self.inner.get_by_attribute(query);
        Ok(self.inner.get_from_indexes(py, allowed)?)
    }

    pub fn add_object_many(&mut self, py: Python, objs: Vec<Py<Indexable>>) -> PyResult<()> {
        
        let weak_self = Arc::downgrade(&self.inner);
        self.inner.store_items(py, weak_self, &objs)?;

        let py_handled_refs: Vec<PyRef<Indexable>> = objs.iter()
            .map(| obj | {
                obj.borrow(py)
            })
            .collect();

        let raw_objs: Vec<&Indexable> = py_handled_refs.iter()
            .map(| obj | {
                obj.deref()
            })
            .collect();

        py.allow_threads(|| {
            let weak_index = Arc::downgrade(&self.inner);
            self.inner.add_object_many(weak_index, raw_objs);
        });

        Ok(())

    }

    pub fn add_object(&mut self, py: Python, py_ref: PyRef<Indexable>) -> PyResult<()> {

        let py_val_hashmap = py_ref.get_py_values().clone();
        let idx = py_ref.id;
        let py_obj: Py<Indexable> = py_ref.into_pyobject(py)?.unbind();
        let py_obj_arc = Arc::new(py_obj);

        py.allow_threads(||{
            let stored_item = StoredItem::new(py_obj_arc.clone(), None);
            let weak_index = Arc::downgrade(&self.inner);
            self.inner.add_object(weak_index, idx, stored_item, py_val_hashmap);
        });

        py_obj_arc.extract::<PyRef<Indexable>>(py)?.add_index(Arc::downgrade(&self.inner));

        Ok(())
    }

    pub fn reduced_query(
        &self,
        py: Python,
        query: PyQueryExpr,
    ) -> PyResult<FilteredIndex> {
        py.allow_threads(move || {
            Ok(self.inner.reduced_query(query))
        })
    }

    pub fn union_with(&mut self, py: Python, other: &Index) -> PyResult<()>{
        py.allow_threads(|| {
            self.inner.union_with(&other.inner)
        })
    }

}

#[derive(Clone, Default)]
pub struct IndexAPI{
    pub index: Arc<RwLock<FxHashMap<SmolStr, Box<QueryMap>>>>,
    pub items: Arc<RwLock<Vec<Option<StoredItem>>>>,
    pub allowed_items: Arc<RwLock<Bitmap>>,
    pub parent_index: Option<Weak<IndexAPI>>,
}

impl IndexAPI{

    pub fn new(parent_index: Option<Weak<IndexAPI>>) -> Self {
        Self {
            index: Arc::new(RwLock::new(FxHashMap::default())),
            items: Arc::new(RwLock::new(vec![])),
            allowed_items: Arc::new(RwLock::new(Bitmap::new())),
            parent_index: parent_index,
        }
    }

    pub fn collect(&self, py:Python) -> PyResult<Vec<Py<Indexable>>> {
        let mut result = vec![];
        let items_reader = self.get_items_reader();
        for item_opt in items_reader.iter(){
            if let Some(item) = item_opt{
                result.push(item);
            }
        }
        Ok(result.iter().map(|arc| (**arc).get_py_ref(py)).collect())
    }

    pub fn get_direct_parents(&self, to_get: &Bitmap) -> HybridSet {
        let items_reader = self.get_items_reader();
        let mut result = HybridSet::new();

        for idx in to_get.iter(){
            if let Some(item) = items_reader[idx as usize].as_ref(){
                if let Some(parent_id) = item.get_parent_id() {
                    result.add(parent_id as u32);
                }
            }
        }

        result
    }

    pub fn get_ids_to_root(&self, idx: u32) -> HybridSet {
        let mut res = HybridSet::new();
        if let Some(weak_parent) = &self.parent_index {
            if let Some(parent) = weak_parent.upgrade(){
                res.or_inplace(&parent.get_parents_from_stored_item(idx as usize));
            }
        }
        res
    }

    pub fn get_parents_from_stored_item(&self, idx: usize) -> HybridSet {
        let guard = self.get_items_reader();
        if let Some(stored_item) = &guard[idx] {
            stored_item.get_path_to_root()
        } else {
            HybridSet::new()
        }
    }

    pub fn store_items(
        &self,
        py: Python,
        weak_self: Weak<Self>,
        raw_objs: &Vec<Py<Indexable>>
    ) -> PyResult<()>{

        let mut items_writer: std::sync::RwLockWriteGuard<'_, Vec<Option<StoredItem>>> = self.get_items_writer();

        for py_ref in raw_objs{
            let idx: usize;
            {
                let borrowed_ref = py_ref.borrow(py);
                idx = borrowed_ref.id as usize;
                borrowed_ref.add_index(weak_self.clone());
            }

            let py_obj_arc = Arc::new(py_ref.clone_ref(py));
            let stored_item = StoredItem::new(py_obj_arc.clone(), None);
            
            if items_writer.len() <= idx{
                items_writer.resize(idx + 1, None);
            }
            
            items_writer[idx] = Some(stored_item);
        }
        Ok(())
    }

    pub fn add_object_many(
        &self,
        weak_self: Weak<Self>,
        raw_objs: Vec<&Indexable>
    ) {

        let mut index = self.get_index_writer();
        let mut allowed_writer = self.get_allowed_items_writer();

        for py_ref in raw_objs {
            allowed_writer.add(py_ref.id);
            for (key, value) in (*py_ref.get_py_values()).iter(){
                if key.starts_with("_"){continue;}
                _add_index(&mut index, weak_self.clone(), py_ref.id, key.clone(), value);
            }
        }
    }

    pub fn add_object(
        &self,
        weak_self: Weak<IndexAPI>,
        idx: u32,
        stored_item: StoredItem,
        py_val_hashmap: Arc<HybridHashmap<SmolStr, PyValue>>
    ) {

        self.get_allowed_items_writer().add(idx);
        {
            let mut items_writer: std::sync::RwLockWriteGuard<'_, Vec<Option<StoredItem>>> = self.get_items_writer();
            if items_writer.len() <= idx as usize{
                items_writer.resize(idx as usize + 1, None);
            }

            items_writer[idx as usize] = Some(stored_item);
        }

        let mut index= self.get_index_writer();
        for (key, value) in py_val_hashmap.iter() {
            if key.starts_with("_"){continue;}
            _add_index(&mut index, weak_self.clone(), idx, key.clone(), value);
        }
    }

    pub fn remove(&self, item: &Indexable) {

        let mut index = self.get_index_writer();
        let item_id = item.id;

        for (key, value) in (*item.get_py_values()).iter(){
            if key.starts_with("_"){continue;}
            _remove_index(&mut index, item_id, &key, value);
        }

        self.get_allowed_items_writer().remove(item_id);
        let mut writer = self.get_items_writer();
        writer[item_id as usize] = None;

    }

    pub fn reduce<'py>(
        &self,
        self_arc: Weak<Self>,
        py: Python,
        kwargs: Option<FxHashMap<String, pyo3::Bound<'py, PyAny>>>,
    ) -> PyResult<()> {
        let query = kwargs_to_hash_query(kwargs.unwrap_or_default())?;
        let mut index = self.get_index_writer();

        let survivors = filter_index_by_hashes(&index, &query);
        let survivors = HybridSet::Large(survivors);

        // Step 1: Remove items not in survivors
        for attr_map in index.values_mut() {
            attr_map.remove(&survivors);
        }

        // Step 2: Add any missing entries for survivors
        for idx in survivors.iter() {

            let reader = self.get_items_reader();
            let item = reader.get(idx as usize).unwrap().clone().unwrap();

            let py_item = item.borrow_py_ref(py);
            
            for (attr, val) in (*py_item.get_py_values()).iter() {
                if attr.starts_with("_") {
                    continue;
                }
                index
                    .entry(attr.clone())
                    .or_insert_with(|| Box::new(QueryMap::new(self_arc.clone())))
                    .insert(&val, idx);
            }
        }

        // Optional: clean up empty submaps
//            index.retain(|_, val_map| {
//                val_map.retain(|_, set| !set.is_empty());
//                !val_map.is_empty()
//            });
        Ok(())
    }


    pub fn reduced(
        &self,
        query: std::collections::HashMap<SmolStr, std::collections::HashSet<PyValue>, rustc_hash::FxBuildHasher>
    ) -> FilteredIndex {
        let index = self.get_index_reader();
        self.filter_from_bitmap(
            filter_index_by_hashes(&index, &query)
        )
    }

    pub fn reduced_query(
        &self,
        query: PyQueryExpr,
    ) -> FilteredIndex {
        let index = self.get_index_reader();
        let allowed = self.get_allowed_items_reader();
        self.filter_from_bitmap(
            evaluate_query(&index, &allowed, &query.inner)
        )
    }

    pub fn get_by_attribute(
        &self,
        query: std::collections::HashMap<SmolStr, std::collections::HashSet<PyValue>, rustc_hash::FxBuildHasher>
    ) -> Bitmap {
        let index = self.get_index_reader();
        filter_index_by_hashes(&index, &query)
    }

    pub fn union_with(&self, other: &IndexAPI) -> PyResult<()>{
        union_with(&self, other)
    }

//    pub fn group_by(&self, py:Python, attr: &str) -> FxHashMap<PyValue, HashSet<StoredItem>> {
//        py.allow_threads(||{
//            let index = self.get_index_reader();
//            let attr_map = match index.get(attr){
//                Some(map) => map,
//                None => &FxHashMap::new(),
//            };
//            let mut result: FxHashMap<PyValue, HashSet<StoredItem>> = FxHashMap::new();
//    
//            for (py_val, items) in attr_map {
//                let obj_set = (*items).iter().map(|arc| (**arc).clone()).collect();
//                result.insert(py_val.clone(), obj_set);
//            }
//            result
//        })
//    }

//    fn group_by_count(&self, py:Python, attr: &str) -> FxHashMap<PyValue, usize> {
//        py.allow_threads(||{
//            let index = self.get_index_reader();
//            let mut result: FxHashMap<PyValue, usize> = FxHashMap::new();
//            if let Some(attr_index) = index.get(attr) {
//                for (value, items) in attr_index {
//                    result.insert(value.clone(), items.len());
//                }
//                result
//            } else {
//                FxHashMap::new()
//            }
//        })
//    }

    pub fn update_index(
        &self,
        mut index: std::sync::RwLockWriteGuard<'_, FxHashMap<SmolStr, Box<QueryMap>>>,
        weak_self: Weak<IndexAPI>,
        attr: SmolStr, 
        old_pv: Option<&PyValue>,
        new_pv: &PyValue,
        item_id: u32,
    ) {
        if attr.starts_with("_") {
            return;
        }
        
        if let Some(old_val) = old_pv {
            _remove_index(&mut index, item_id, &attr, old_val);
        }
        _add_index(&mut index, weak_self, item_id, attr, &new_pv);
    }

    fn get_from_indexes(&self, py: Python, indexes: Bitmap) -> PyResult<Vec<Py<Indexable>>>{
        let items_read = self.get_items_reader();
        let results: Vec<Py<Indexable>> = indexes.iter()
            .map(|arc| items_read.get(arc as usize).unwrap().as_ref().unwrap().get_py_ref(py))
            .collect();

        Ok(results)
    }

    fn filter_from_bitmap(&self, bm: Bitmap) -> FilteredIndex {
        FilteredIndex {
            index: self.index.clone(),
            items: self.items.clone(),
            allowed_items: bm
        }
    }

    fn get_items_writer(&self) -> RwLockWriteGuard<Vec<Option<StoredItem>>> {
        self.items.write().unwrap()
        //self.items.try_write().expect("items writer deadlock")
    }

    fn get_items_reader(&self) -> RwLockReadGuard<Vec<Option<StoredItem>>> {
        self.items.read().unwrap()
        //self.items.try_read().expect("cannot read from items")
    }

    pub fn get_index_writer(&self) -> RwLockWriteGuard<FxHashMap<SmolStr, Box<QueryMap>>> {
        self.index.write().unwrap()
        //self.index.try_write().expect("index writer deadlock")
    }

    pub fn get_index_reader(&self) -> RwLockReadGuard<FxHashMap<SmolStr, Box<QueryMap>>> {
        self.index.read().unwrap()
        //self.index.try_read().expect("cannot read from index")
    }

    fn get_allowed_items_writer(&self) -> RwLockWriteGuard<Bitmap> {
        self.allowed_items.write().unwrap()
        //self.allowed_items.try_write().expect("index writer deadlock")
    }

    fn get_allowed_items_reader(&self) -> RwLockReadGuard<Bitmap> {
        self.allowed_items.read().unwrap()
        //self.allowed_items.try_read().expect("cannot read from index")
    }
}

impl fmt::Debug for IndexAPI {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let index = self.get_index_reader();
        let items = self.get_items_reader();
        let allowed_items = self.get_allowed_items_reader();

        f.debug_struct("IndexAPI")
            .field("index_len", &index.len())
            .field("items_len", &items.len())
            .field("allowed_items_cardinality", &allowed_items.cardinality())
            .finish()
    }
}

fn union_with(index: &IndexAPI, other: &IndexAPI) -> PyResult<()> {
    let mut self_index = index.index.write().unwrap();
    let other_index = other.index.read().unwrap();

    for (attr, val_map) in other_index.iter() {
        let self_val_map = self_index.entry(attr.clone()).or_default();
        self_val_map.merge(val_map);
    }

    Ok(())
}


pub fn _add_index(
    index: &mut std::sync::RwLockWriteGuard<'_, FxHashMap<SmolStr, Box<QueryMap>>>, 
    weak_self: Weak<IndexAPI>,
    obj_id: u32,
    attr: SmolStr,
    value: &PyValue
){
    if let Some(qmap) = index.get_mut(&attr) {
        qmap.insert(value, obj_id);
    } else {
        let mut qmap = QueryMap::new(weak_self);
        qmap.insert(value, obj_id);
        index.insert(attr, Box::new(qmap));
    }
}

fn _remove_index(
    index: &mut std::sync::RwLockWriteGuard<'_, FxHashMap<SmolStr, Box<QueryMap>>>,
    idx: u32,
    attr: &str, 
    py_value: &PyValue
){

    if index.contains_key(attr){
        if let Some(val) = index.get_mut(attr) { 
            if val.contains(py_value){
                val.remove_id(py_value, idx);
                val.check_prune(py_value);
            }
        };

        if index[attr].is_empty(){
            index.remove(attr);
        }
    }
}
