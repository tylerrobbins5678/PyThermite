
use std::{ops::Deref, sync::{Arc, RwLock}, time::Instant, vec};
use croaring::Bitmap;
use pyo3::prelude::*;
use rustc_hash::FxHashMap;
use smol_str::SmolStr;

use crate::index::{filtered_index::FilteredIndex, query::{evaluate_query, filter_index_by_hashes, kwargs_to_hash_query, PyQueryExpr, QueryMap}, HybridSet, Indexable};

use super::stored_item::StoredItem;
use super::value::PyValue;


#[pyclass]
#[derive(Clone)]
pub struct Index{
    pub index: Arc<RwLock<FxHashMap<SmolStr, Box<QueryMap>>>>,
    pub items: Arc<RwLock<Vec<Option<StoredItem>>>>,
    pub allowed_items: Bitmap,
}

#[pymethods]
impl Index{

    #[new]
    pub fn new() -> Self {
        Self {
            index: Arc::new(RwLock::new(FxHashMap::default())),
            items: Arc::new(RwLock::new(vec![])),
            allowed_items: Bitmap::new()
        }
    }

    pub fn collect(&self, py:Python) -> PyResult<Vec<Py<Indexable>>> {
        let mut result = vec![];
        let items_reader = self.items.read().unwrap();
        for item_opt in items_reader.iter(){
            if let Some(item) = item_opt{
                result.push(item);
            }
        }
        Ok(result.iter().map(|arc| (**arc).py_item.clone_ref(py)).collect())
    }

    pub fn add_object_many(&mut self, py: Python, objs: Vec<PyRefMut<Indexable>>) -> PyResult<()> {

        let start = Instant::now();

        let raw_objs: Vec<&Indexable> = objs.iter()
            .map(| obj | {
                obj.deref()
            })
            .collect();

        py.allow_threads(|| {
            for py_ref in raw_objs {
                self.allowed_items.add(py_ref.id);
                for (key, value) in py_ref.py_values.iter(){
                    if key.starts_with("_"){continue;}
                    let mut index = self.index.write().unwrap();
                    _add_index(&mut index, py_ref.id, key.clone(), value);                    
                }
            }
        });

        let duration = start.elapsed();
        println!("add to index: {:.6} seconds", duration.as_secs_f64());

        let start = Instant::now();

        let mut items_writer: std::sync::RwLockWriteGuard<'_, Vec<Option<StoredItem>>> = self.items.write().unwrap();
        let slf = Arc::new(self.clone());
        for mut py_ref in objs{
            let idx = py_ref.id as usize;
            py_ref.add_index(Arc::downgrade(&slf.clone()));
            //let py_obj: Py<Indexable> = py_ref.clone().into_pyobject(py)?.unbind();
            let py_obj = py_ref.into_pyobject(py)?.unbind();
            let py_obj_arc = Arc::new(py_obj);
            let stored_item = StoredItem::new(py_obj_arc.clone());
            
            if items_writer.len() <= idx{
                items_writer.resize(idx + 1, None);
            }
            
            items_writer[idx] = Some(stored_item);
        }

        let duration = start.elapsed();
        println!("add to list: {:.6} seconds", duration.as_secs_f64());

        Ok(())
    }

    pub fn add_object(&mut self, py: Python, py_ref: PyRef<Indexable>) -> PyResult<()> {

        let mut py_val_FxHashMap: FxHashMap<SmolStr, PyValue> = FxHashMap::default();
        for (key, value) in py_ref.py_values.iter(){
            if key.starts_with("_"){continue;}
            py_val_FxHashMap.insert(key.clone(), value.clone());
        }

        let idx = py_ref.id;

        let py_obj: Py<Indexable> = py_ref.into_pyobject(py)?.unbind();
        let py_obj_arc = Arc::new(py_obj);
        let stored_item = StoredItem::new(py_obj_arc.clone());
        

        py.allow_threads(||{
            self.allowed_items.add(idx);
            {
                let mut items_writer: std::sync::RwLockWriteGuard<'_, Vec<Option<StoredItem>>> = self.items.write().unwrap();
                if items_writer.len() <= idx as usize{
                    items_writer.resize(idx as usize + 1, None);
                }
                
                items_writer[idx as usize] = Some(stored_item);
            }

            let mut index= self.index.write().unwrap();
            for (key, value) in py_val_FxHashMap.iter() {
                if key.starts_with("_"){continue;}
                _add_index(&mut index, idx, key.clone(), value);
            }

        });

        py_obj_arc.extract::<PyRefMut<Indexable>>(py)?.add_index(Arc::downgrade(&Arc::new(self.clone())));

        Ok(())
    }

    #[pyo3(signature = (**kwargs))]
    pub fn reduce<'py>(
        &mut self,
        py: Python,
        kwargs: Option<FxHashMap<String, pyo3::Bound<'py, PyAny>>>,
    ) -> PyResult<()> {
        let query = kwargs_to_hash_query(kwargs.unwrap_or_default())?;
        let mut index = self.index.write().unwrap();

        let survivors = filter_index_by_hashes(&index, &query);
        let survivors = HybridSet::Large(survivors);

        // Step 1: Remove items not in survivors
        for attr_map in index.values_mut() {
            attr_map.remove(&survivors);
        }

        // Step 2: Add any missing entries for survivors
        for idx in survivors.iter() {

            let reader = self.items.read().unwrap();
            let item = reader.get(idx as usize).unwrap().clone().unwrap();

            let py_item = item.py_item.bind(py).borrow();
            
            for (attr, val) in py_item.py_values.iter() {
                if attr.starts_with("_") {
                    continue;
                }
                index
                    .entry(attr.clone())
                    .or_insert_with(|| Box::new(QueryMap::new()))
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

    #[pyo3(signature = (**kwargs))]
    pub fn reduced<'py>(
        &self,
        py: Python,
        kwargs: Option<FxHashMap<String, pyo3::Bound<'py, PyAny>>>,
    ) -> PyResult<FilteredIndex> {
        let query = kwargs_to_hash_query(kwargs.unwrap_or_default())?;
        py.allow_threads(move || {
            let index = self.index.read().unwrap();
            Ok(FilteredIndex {
                index: self.index.clone(),
                items: self.items.clone(),
                allowed_items: filter_index_by_hashes(&index, &query)
            })
        })
    }

    pub fn reduced_query(
        &self,
        py: Python,
        query: PyQueryExpr,
    ) -> PyResult<FilteredIndex> {
        py.allow_threads(move || {
            let index = self.index.read().unwrap();
            Ok(FilteredIndex {
                index: self.index.clone(),
                items: self.items.clone(),
                allowed_items: evaluate_query(&index, &self.allowed_items, &query.inner)
            })
        })
    }

    #[pyo3(signature = (**kwargs))]
    pub fn get_by_attribute<'py>(
        &self,
        py: Python,
        kwargs: Option<FxHashMap<String, pyo3::Bound<'py, PyAny>>>,
    ) -> PyResult<Vec<Py<Indexable>>> {

        let query = kwargs_to_hash_query(kwargs.unwrap_or_default())?;
        let index = self.index.read().unwrap();
        let indexes: Bitmap = filter_index_by_hashes(&index, &query);
    
        Ok(self.get_from_indexes(py, indexes)?)
    }

    pub fn union_with(&mut self, py: Python, other: &Index) -> PyResult<()>{
        py.allow_threads(|| {
            union_with(&self, other)
        })
    }

//    pub fn group_by(&self, py:Python, attr: &str) -> FxHashMap<PyValue, HashSet<StoredItem>> {
//        py.allow_threads(||{
//            let index = self.index.read().unwrap();
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
//            let index = self.index.read().unwrap();
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
}

impl Index{
    pub fn update_index(
        &self,
        mut index: std::sync::RwLockWriteGuard<'_, FxHashMap<SmolStr, Box<QueryMap>>>,
        attr: SmolStr, 
        old_pv: &PyValue,
        new_pv: &PyValue,
        item_id: u32,
    ) {
        if attr.starts_with("_") {
            return;
        }

        _remove_index(&mut index, item_id, &attr, old_pv.clone());
        _add_index(&mut index, item_id, attr, &new_pv);
    }

    fn get_from_indexes(&self, py: Python, indexes: Bitmap) -> PyResult<Vec<Py<Indexable>>>{
        let items_read = self.items.read().unwrap();
        let results: Vec<Py<Indexable>> = indexes.iter()
            .map(|arc| items_read.get(arc as usize).unwrap().clone().unwrap().py_item.clone_ref(py))
            .collect();

        Ok(results)
    }
}

fn union_with(index: &Index, other: &Index) -> PyResult<()> {
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
    obj_id: u32,
    attr: SmolStr,
    value: &PyValue
){
    if let Some(qmap) = index.get_mut(&attr) {
        qmap.insert(value, obj_id);
    } else {
        let mut qmap = QueryMap::new();
        qmap.insert(value, obj_id);
        index.insert(attr, Box::new(qmap));
    }
}

fn _remove_index(
    index: &mut std::sync::RwLockWriteGuard<'_, FxHashMap<SmolStr, Box<QueryMap>>>,
    idx: u32,
    attr: &str, 
    py_value: PyValue
){

    if index.contains_key(attr){
        if let Some(val) = index.get_mut(attr) { 
            if val.contains(&py_value){
                val.remove_id(&py_value, idx);
                val.check_prune(&py_value);
            }
        };

        if index[attr].is_empty(){
            index.remove(attr);
        }
    }
}
