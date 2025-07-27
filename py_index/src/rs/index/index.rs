
use std::{collections::HashMap, sync::{Arc, RwLock}, time::Instant, vec};
use croaring::Bitmap;
use pyo3::prelude::*;

use crate::index::{filtered_index::FilteredIndex, query::{evaluate_query, filter_index_by_hashes, kwargs_to_hash_query, PyQueryExpr, QueryMap}, Indexable};

use super::stored_item::StoredItem;
use super::value::PyValue;


#[pyclass]
#[derive(Clone)]
pub struct Index{
    pub index: Arc<RwLock<HashMap<String, QueryMap>>>,
    pub items: Arc<RwLock<Vec<Option<Arc<StoredItem>>>>>,
    pub allowed_items: Bitmap,
}

#[pymethods]
impl Index{

    #[new]
    pub fn new() -> Self {
        Self {
            index: Arc::new(RwLock::new(HashMap::new())),
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

    pub fn add_object_many(&mut self, py: Python, objs: Vec<Indexable>) -> PyResult<()> {

        let start = Instant::now();

        py.allow_threads(||{
            let mut index= self.index.write().unwrap();
            for py_ref in &objs{
                self.allowed_items.add(py_ref.id);
                for (key, value) in unsafe { py_ref.py_values.map_ref() }.iter(){
                    if key.starts_with("_"){continue;}
                    _add_index(&mut index, py_ref.id , key.to_string(), value.clone());
                }
            }
        });

        eprintln!("Time to build index {:?}", start.elapsed());

        let start = Instant::now();

        let mut items_writer: std::sync::RwLockWriteGuard<'_, Vec<Option<Arc<StoredItem>>>> = self.items.write().unwrap();
        let slf = Arc::new(self.clone());
        for py_ref in objs{
            let idx = py_ref.id as usize;
            py_ref.add_index(slf.clone());
            let py_obj: Py<Indexable> = py_ref.clone().into_pyobject(py)?.unbind();
            let py_obj_arc = Arc::new(py_obj);
            let stored_item = Arc::new(StoredItem::new(py_ref.clone(), py_obj_arc.clone()));
            
            if items_writer.len() <= idx{
                items_writer.resize(idx + 1, None);
            }
            
            items_writer[idx] = Some(Arc::clone(&stored_item));
        }

        eprintln!("Time to extract py objects {:?}", start.elapsed());

        Ok(())
    }

    pub fn add_object(&mut self, py: Python, py_ref: PyRef<Indexable>) -> PyResult<()> {

        let py_obj: Py<Indexable> = py_ref.clone().into_pyobject(py)?.unbind();
        let py_obj_arc = Arc::new(py_obj);

        let mut py_val_hashmap: HashMap<String, PyValue> = HashMap::new();
        for (key, value) in unsafe { py_ref.py_values.map_ref() }.iter(){
            if key.starts_with("_"){continue;}
            py_val_hashmap.insert(key.clone(), value.clone());
        }

        let idx = py_ref.id;
        let stored_item = Arc::new(StoredItem::new(py_ref.clone(), py_obj_arc.clone()));
        
        py.allow_threads(||{
            self.allowed_items.add(idx);
            {
                let mut items_writer: std::sync::RwLockWriteGuard<'_, Vec<Option<Arc<StoredItem>>>> = self.items.write().unwrap();
                if items_writer.len() <= idx as usize{
                    items_writer.resize(idx as usize + 1, None);
                }
                
                items_writer[idx as usize] = Some(Arc::clone(&stored_item));
            }

            let mut index= self.index.write().unwrap();
            for (key, value) in py_val_hashmap.iter() {
                if key.starts_with("_"){continue;}
                _add_index(&mut index, idx, key.clone(), value.clone());
            }

        });

        py_obj_arc.extract::<PyRefMut<Indexable>>(py)?.add_index(Arc::new(self.clone()));

        Ok(())
    }

    #[pyo3(signature = (**kwargs))]
    pub fn reduce(
        &mut self,
        py: Python,
        kwargs: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<()> {
        let query = kwargs_to_hash_query(py, &kwargs.unwrap_or_default())?;
        py.allow_threads(|| {
            let mut index = self.index.write().unwrap();

            let survivors = filter_index_by_hashes(&index, &query);

            // Step 1: Remove items not in survivors
            for attr_map in index.values_mut() {
                attr_map.remove_bitmap(&survivors);
            }

            // Step 2: Add any missing entries for survivors
            for idx in survivors.iter() {

                let reader = self.items.read().unwrap();
                let item = reader.get(idx as usize).unwrap().clone().unwrap();
                
                let attr_map = unsafe { item.item.py_values.map_ref() };
                for (attr, val) in attr_map.iter() {
                    if attr.starts_with("_") {
                        continue;
                    }
                    index
                        .entry(attr.clone())
                        .or_insert_with(QueryMap::new)
                        .insert(val.clone(), idx);
                }
            }

            // Optional: clean up empty submaps
//            index.retain(|_, val_map| {
//                val_map.retain(|_, set| !set.is_empty());
//                !val_map.is_empty()
//            });
            Ok(())
        })
    }

    #[pyo3(signature = (**kwargs))]
    pub fn reduced(
        &self,
        py: Python,
        kwargs: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<FilteredIndex> {
        let query = kwargs_to_hash_query(py, &kwargs.unwrap_or_default())?;
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
    pub fn get_by_attribute(
        &self,
        py: Python,
        kwargs: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Vec<Py<Indexable>>> {

        let query = kwargs_to_hash_query(py, &kwargs.unwrap_or_default())?;
        let index = self.index.read().unwrap();
        let indexes: Bitmap = filter_index_by_hashes(&index, &query);
    
        Ok(self.get_from_indexes(py, indexes)?)
    }

    pub fn union_with(&mut self, py: Python, other: &Index) -> PyResult<()>{
        py.allow_threads(|| {
            union_with(&self, other)
        })
    }

//    pub fn group_by(&self, py:Python, attr: &str) -> HashMap<PyValue, HashSet<StoredItem>> {
//        py.allow_threads(||{
//            let index = self.index.read().unwrap();
//            let attr_map = match index.get(attr){
//                Some(map) => map,
//                None => &HashMap::new(),
//            };
//            let mut result: HashMap<PyValue, HashSet<StoredItem>> = HashMap::new();
//    
//            for (py_val, items) in attr_map {
//                let obj_set = (*items).iter().map(|arc| (**arc).clone()).collect();
//                result.insert(py_val.clone(), obj_set);
//            }
//            result
//        })
//    }

//    fn group_by_count(&self, py:Python, attr: &str) -> HashMap<PyValue, usize> {
//        py.allow_threads(||{
//            let index = self.index.read().unwrap();
//            let mut result: HashMap<PyValue, usize> = HashMap::new();
//            if let Some(attr_index) = index.get(attr) {
//                for (value, items) in attr_index {
//                    result.insert(value.clone(), items.len());
//                }
//                result
//            } else {
//                HashMap::new()
//            }
//        })
//    }
}

impl Index{
    pub fn update_index(
        &self,
        mut index: std::sync::RwLockWriteGuard<'_, HashMap<String, QueryMap>>,
        attr: String, 
        old_pv: &PyValue,
        new_val: &PyObject,
        item_id: u32,
    ) -> PyResult<()> {
        if attr.starts_with("_") {
            return Ok(());
        }
        let new_pv = PyValue::new(new_val);

        _remove_index(&mut index, item_id, &attr, old_pv.clone());
        _add_index(&mut index, item_id, attr, new_pv);

        Ok(())
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
    index: &mut std::sync::RwLockWriteGuard<'_, HashMap<String, QueryMap>>, 
    obj_id: u32, 
    attr: String,
    value: PyValue
){
   
    let q_map: &mut QueryMap = index.entry(attr.clone())
        .or_insert_with(|| QueryMap::new());

    q_map.insert(value, obj_id);
}

fn _remove_index(
    index: &mut std::sync::RwLockWriteGuard<'_, HashMap<String, QueryMap>>, 
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
