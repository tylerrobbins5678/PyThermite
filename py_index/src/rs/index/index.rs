
use std::{collections::{HashMap, HashSet}, sync::{Arc, RwLock}};
use pyo3::{prelude::*, types::PyList};
use pyo3::types::PyString;

use crate::index::{stored_item, Indexable};

use super::stored_item::StoredItem;
use super::value::PyValue;
use std::time::Instant;


#[pyclass]
#[derive(Clone)]
pub struct Index{
    pub index: Arc<RwLock<HashMap<String, HashMap<PyValue, HashSet<Arc<StoredItem>>>>>>
}

#[pymethods]
impl Index{

    #[new]
    pub fn new() -> Self {
        Self {
            index: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn collect(&self, py:Python) -> PyResult<Vec<Py<Indexable>>> {
        let index = self.index.read().unwrap();
        let result = py.allow_threads(||{
            let mut result: HashSet<&Arc<StoredItem>> = HashSet::new();

            for attr_map in index.values() {
                for item_set in attr_map.values() {
                    for arc_item in item_set {
                        result.insert(arc_item);
                    }
                }
            }
            result
        });

        Ok(result.into_iter().map(|arc| (**arc).py_item.clone_ref(py)).collect())
    }

    pub fn add_object_many(&self, py: Python, objs: Vec<PyRef<Indexable>>) -> PyResult<()> {
        for obj in objs{
            self.add_object(py, obj)?;
        }
        Ok(())
    }

    pub fn add_object(&self, py: Python, py_ref: PyRef<Indexable>) -> PyResult<()> {

        let mut py_val_hashmap: HashMap<String, PyValue> = HashMap::new();
        for (key, value) in unsafe { py_ref.py_values.map_ref() }.iter(){
            if key.starts_with("_"){continue;}
            py_val_hashmap.insert(key.clone(), value.clone());
        }

        let py_obj: Py<Indexable> = py_ref.clone().into_pyobject(py)?.unbind();
        let py_obj_arc = Arc::new(py_obj);

        let mut attr_values = HashMap::new();
        let stored_obj = Arc::new(StoredItem::new(py_ref.clone(), py_obj_arc.clone()));

        let mut index= self.index.write().unwrap();
        
        for (key, value) in py_val_hashmap.iter() {
            if key.starts_with("_"){continue;}
            _add_index(&mut index, stored_obj.clone(), key, value.clone());
            attr_values.insert(key.clone(), value.clone());
        }

        py_obj_arc.extract::<PyRefMut<Indexable>>(py)?.add_index(Arc::new(self.clone()), stored_obj);

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
            _reduced_in_place(&mut index, &query);
            Ok(())
        })
    }

    #[pyo3(signature = (**kwargs))]
    pub fn reduced(
        &self,
        py: Python,
        kwargs: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Self> {
        let query = kwargs_to_hash_query(py, &kwargs.unwrap_or_default())?;
        py.allow_threads(|| {
            let index = self.index.read().unwrap();
            Ok(Self {
                index: Arc::new(RwLock::new(_reduced(&index, &query))),
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
        let items: HashSet<Arc<StoredItem>> = filter_index_by_hashes(&index, &query);
        Ok(items.into_iter().map(|arc| arc.py_item.clone_ref(py)).collect())
    }

    pub fn union_with(&mut self, py: Python, other: &Index) -> PyResult<()>{
        py.allow_threads(|| {
            union_with(&self, other)
        })
    }

    pub fn group_by(&self, py:Python, attr: &str) -> HashMap<PyValue, HashSet<StoredItem>> {
        py.allow_threads(||{
            let index = self.index.read().unwrap();
            let attr_map = match index.get(attr){
                Some(map) => map,
                None => &HashMap::new(),
            };
            let mut result: HashMap<PyValue, HashSet<StoredItem>> = HashMap::new();
    
            for (py_val, items) in attr_map {
                let obj_set = (*items).iter().map(|arc| (**arc).clone()).collect();
                result.insert(py_val.clone(), obj_set);
            }
            result
        })
    }

    fn group_by_count(&self, py:Python, attr: &str) -> HashMap<PyValue, usize> {
        py.allow_threads(||{
            let index = self.index.read().unwrap();
            let mut result: HashMap<PyValue, usize> = HashMap::new();
            if let Some(attr_index) = index.get(attr) {
                for (value, items) in attr_index {
                    result.insert(value.clone(), items.len());
                }
                result
            } else {
                HashMap::new()
            }
        })
    }
}

impl Index{
    pub fn update_index(
        &self, 
        attr: String, 
        old_pv: &PyValue,
        new_val: &PyObject,
        stored_item: Arc<StoredItem>,
    ) -> PyResult<()> {
        if attr.starts_with("_") {
            return Ok(());
        }
        let new_pv = PyValue::new(new_val)?;
        
        let attr_vals = unsafe { stored_item.item.py_values.get_mut() };
        attr_vals.insert(attr.clone(), new_pv.clone());

        let mut index = self.index.write().unwrap();

        _remove_index(&mut index, Arc::clone(&stored_item), &attr, old_pv.clone());
        _add_index(&mut index, Arc::clone(&stored_item), &attr, new_pv);

        Ok(())
    }
}

fn union_with(index: &Index, other: &Index) -> PyResult<()> {
    let mut self_index = index.index.write().unwrap();
    let other_index = other.index.read().unwrap();

    for (attr, val_map) in other_index.iter() {
        let self_val_map = self_index.entry(attr.clone()).or_default();
        for (hash, items) in val_map.iter() {
            self_val_map.entry(hash.clone())
                .or_default()
                .extend(items.iter().cloned());
        }
    }

    Ok(())
}

fn filter_index_by_hashes(
    index: &HashMap<String, HashMap<PyValue, HashSet<Arc<StoredItem>>>>,
    query: &HashMap<String, HashSet<PyValue>>,
) -> HashSet<Arc<StoredItem>> {
    let mut sets_iter: HashSet<Arc<StoredItem>> = HashSet::new();
    let mut first = true;
    let eh = HashMap::new();

    let mut sorted_query: Vec<_> = query.iter().collect();
    sorted_query.sort_by_key(|(attr, hashes)| {
        index.get(*attr)
            .map(|attr_map| {
                hashes.iter()
                    .map(|h| attr_map.get(h).map_or(0, |set| set.len()))
                    .sum::<usize>()
            })
            .unwrap_or(0)
    });
    
    for (attr, allowed_hashes) in sorted_query {
        
        let mut per_attr_match: HashSet<Arc<StoredItem>> = HashSet::new();

        let attr_map = index.get(attr).unwrap_or(&eh);
        
        for h in allowed_hashes {
            if let Some(matched) = attr_map.get(h) {
                if first {
                    per_attr_match.extend(matched.clone());
                } else {
                    if sets_iter.len() < matched.len(){
                        for item in &sets_iter {
                            if matched.contains(item) {
                                per_attr_match.insert(item.clone());
                            }
                        }
                    } else {
                        for item in matched {
                            if sets_iter.contains(item) {
                                per_attr_match.insert(item.clone());
                            }
                        }
                    }
                }
            }
        }

        if !first && sets_iter.is_empty() {
            return HashSet::new();
        }

        sets_iter = if first {
            per_attr_match
        } else {
            sets_iter.intersection(&per_attr_match).cloned().collect()
        };
        first = false;
    }

    sets_iter
}


fn kwargs_to_hash_query(
    py: Python,
    kwargs: &HashMap<String, Py<PyAny>>,
) -> PyResult<HashMap<String, HashSet<PyValue>>> {
    let mut query = HashMap::new();

    for (attr, py_val) in kwargs {
        let val_ref = py_val.clone_ref(py).into_bound(py);
        let mut hash_set = HashSet::new();

        // Detect if iterable but not string
        let is_str = val_ref.is_instance_of::<PyString>();

        if !is_str {
            match val_ref.try_iter() {
                Ok(iter) => {
                    for item in iter {
                        let lookup_item = PyValue::new(&item.unwrap().unbind())?;
                        hash_set.insert(lookup_item);
                    }
                }
                Err(_) => {
                    // Not iterable, treat as a single value
                    hash_set.insert(PyValue::new(py_val)?);
                }
            }
        } else {
            // Is a string, treat as a single value
            hash_set.insert(PyValue::new(py_val)?);
        }

        // Single value
        query.insert(attr.clone(), hash_set);
    }

    Ok(query)
}


pub fn _add_index(
    index: &mut std::sync::RwLockWriteGuard<'_, HashMap<String, HashMap<PyValue, HashSet<Arc<StoredItem>>>>>, 
    obj: Arc<StoredItem>, 
    attr: &str, 
    value: PyValue
){
   
    let attr_entry: &mut HashMap<PyValue, HashSet<Arc<StoredItem>>> = index.entry(attr.to_string())
        .or_insert_with(|| HashMap::<PyValue, HashSet<Arc<StoredItem>>>::new());
    
    let val_entry: &mut HashSet<Arc<StoredItem>> = attr_entry.entry(value)
        .or_insert_with(|| HashSet::<Arc<StoredItem>>::new());
    
    val_entry.insert(obj);
}

fn _reduced(
    index: &HashMap<String, HashMap<PyValue, HashSet<Arc<StoredItem>>>>,
    query: &HashMap<String, HashSet<PyValue>>
) -> HashMap<String, HashMap<PyValue, HashSet<Arc<StoredItem>>>> {
    let start = Instant::now();

    let survivors = filter_index_by_hashes(index, query);

    let duration = start.elapsed();
    eprintln!("Elapsed time to get survivors: {:.3?}", duration);

    let start = Instant::now();

    let mut new_index: HashMap<String, HashMap<PyValue, HashSet<Arc<StoredItem>>>> = HashMap::new();

    for item in survivors {
        let attr_map = unsafe { item.item.py_values.map_ref() };
        
        for (attr, val) in attr_map.iter() {
            new_index
                .entry(attr.clone())
                .or_insert_with(HashMap::new)
                .entry(val.clone())
                .or_insert_with(HashSet::new)
                .insert(item.clone());
        }
    }

    let duration = start.elapsed();
    eprintln!("Elapsed time to rebuild index: {:.3?}", duration);

    new_index
}

fn _reduced_in_place(
    index: &mut HashMap<String, HashMap<PyValue, HashSet<Arc<StoredItem>>>>,
    query: &HashMap<String, HashSet<PyValue>>,
) {
    let survivors = filter_index_by_hashes(index, query);

    // Step 1: Remove items not in survivors
    for attr_map in index.values_mut() {
        for items in attr_map.values_mut() {
            items.retain(|item| survivors.contains(item));
        }
    }

    // Step 2: Add any missing entries for survivors
    for item in survivors {
        let attr_map = unsafe { item.item.py_values.map_ref() };
        for (attr, val) in attr_map.iter() {
            if attr.starts_with("_") {
                continue;
            }

            index
                .entry(attr.clone())
                .or_insert_with(HashMap::new)
                .entry(val.clone())
                .or_insert_with(HashSet::new)
                .insert(item.clone());
        }
    }

    // Optional: clean up empty submaps
    index.retain(|_, val_map| {
        val_map.retain(|_, set| !set.is_empty());
        !val_map.is_empty()
    });
}

fn _remove_index(
    index: &mut std::sync::RwLockWriteGuard<'_, HashMap<String, HashMap<PyValue, HashSet<Arc<StoredItem>>>>>, 
    obj: Arc<StoredItem>, 
    attr: &str, 
    value_hash: PyValue
){

    if index.contains_key(attr){
        if let Some(val) = index.get_mut(attr) { 
            if val.contains_key(&value_hash){
                if let Some(val) = val.get_mut(&value_hash) { 
                    val.remove(&obj);
                };
                if val[&value_hash].is_empty(){
                    val.remove(&value_hash);
                }
            }
        };

        if index[attr].is_empty(){
            index.remove(attr);
        }
    }
}
