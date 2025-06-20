
use std::{collections::{HashMap, HashSet}, ffi::CString, sync::{Arc, RwLock}};
use pyo3::{prelude::*, types::PyCapsule};
use pyo3::types::PyString;
use rayon::prelude::*;

use super::stored_item::StoredItem;
use super::value::PyValue;


#[pyclass]
#[derive(Clone)]
pub struct Index{
    pub index: Arc<RwLock<HashMap<String, HashMap<PyValue, HashSet<StoredItem>>>>>
}

#[pymethods]
impl Index{

    #[new]
    pub fn new() -> Self {
        Self {
            index: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn collect(&mut self) -> PyResult<HashSet<StoredItem>> {
        let index = self.index.read().unwrap();
        let mut result = HashSet::new();

        for attr_map in index.values() {
            for item_set in attr_map.values() {
                result.extend(item_set.iter().cloned());
            }
        }

        Ok(result)
    }

    pub fn add_object(&mut self, py: Python, obj: PyObject) -> PyResult<()> {

        let py_index_obj = Py::new(py, self.clone())?;
        obj.call_method1(py, "add_index", (py_index_obj,))?;

        // Get the __dict__ attribute and values
        let py_object_dict: HashMap<String, PyObject> = obj.getattr(py, "__dict__")?.extract(py).unwrap();
        
        let mut py_val_hashmap: HashMap<String, PyValue> = HashMap::new();
        for (key, value) in py_object_dict.iter(){
            if key.starts_with("_"){continue;}
            py_val_hashmap.insert(key.clone(), PyValue::new(value.clone_ref(py))?);
        }
        
        let attr_values = Arc::new(RwLock::new(HashMap::new()));
        let stored_obj = StoredItem::new(py, &obj, attr_values.clone());
        
        let name = CString::new("StoredItem").unwrap();

        py.allow_threads( || {
            let mut index= self.index.write().unwrap();

            for (key, value) in py_val_hashmap.iter() {
                if key.starts_with("_"){continue;}
                _add_index(&mut index, &stored_obj, key, value.clone());
                attr_values.write().unwrap().insert(key.clone(), value.clone());
            }
        });

        let pyc = PyCapsule::new(py, stored_obj, Some(name))?;
        obj.setattr(py, "_rust_index_ptr", pyc)?;

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
        });
        Ok(())
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
    ) -> PyResult<HashSet<StoredItem>> {

        let index = self.index.read().unwrap();
        let query = kwargs_to_hash_query(py, &kwargs.unwrap_or_default())?;
        py.allow_threads(|| {
            Ok(filter_index_by_hashes(&index, &query))
        })
    }


    pub fn update_index(&mut self, py: Python, obj: Py<PyAny>, attr: String, old_val: Py<PyAny>) -> PyResult<()>{

        if attr.starts_with("_"){return Ok(());}
        let new_val = obj.getattr(py, &attr)?;

        let capsule: Py<PyCapsule> = obj.getattr(py, "_rust_index_ptr")?.extract(py)?;
        let capsule = capsule.bind(py);
        let ptr = capsule.pointer() as *mut StoredItem;
        let old_stored_obj: &mut StoredItem = unsafe { &mut *ptr };
        let stored_obj = StoredItem::new(py, &obj, old_stored_obj.attr_values.clone());

        py.allow_threads(|| {
            let new_pv = PyValue::new(new_val)?;
    
            stored_obj.attr_values.write().unwrap().remove(&attr);
            stored_obj.attr_values.write().unwrap().insert(attr.clone(), new_pv.clone());
    
            let mut index= self.index.write().unwrap();
            
            _remove_index(&mut index, &stored_obj, &attr, PyValue::new(old_val)?);
            _add_index(&mut index, &stored_obj, &attr, new_pv);
    
            Ok(())
        })

    }

    pub fn union_with(&mut self, py: Python, other: &Index) -> PyResult<()>{
        py.allow_threads(|| {
            union_with(&self, other)
        })
    }

    pub fn group_by(&self, attr: &str) -> HashMap<PyValue, HashSet<StoredItem>> {
        let index = self.index.read().unwrap();
        let attr_map = match index.get(attr){
            Some(map) => map,
            None => &HashMap::new(),
        };
        let mut result: HashMap<PyValue, HashSet<StoredItem>> = HashMap::new();

        for (py_val, items) in attr_map {
            let obj_set = items.clone();
            result.insert(py_val.clone(), obj_set);
        }
        result
    }

    fn group_by_count(&self, attr: &str) -> HashMap<PyValue, usize> {
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
    index: &HashMap<String, HashMap<PyValue, HashSet<StoredItem>>>,
    query: &HashMap<String, HashSet<PyValue>>,
) -> HashSet<StoredItem> {
    let mut sets_iter: HashSet<&StoredItem> = HashSet::new();
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
        
        let mut per_attr_match: HashSet<&StoredItem> = HashSet::new();

        let attr_map = index.get(attr).unwrap_or(&eh);
        
        for h in allowed_hashes {
            if let Some(matched) = attr_map.get(h) {
                if first {
                    per_attr_match.extend(matched);
                } else {
                    if sets_iter.len() < matched.len(){
                        for item in &sets_iter {
                            if matched.contains(*item) {
                                per_attr_match.insert(item);
                            }
                        }
                    } else {
                        for item in matched {
                            if sets_iter.contains(item) {
                                per_attr_match.insert(item);
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

    sets_iter.into_iter().cloned().collect()
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
                        let lookup_item = PyValue::new(item?.into())?;
                        hash_set.insert(lookup_item);
                    }
                }
                Err(_) => {
                    // Not iterable, treat as a single value
                    hash_set.insert(PyValue::new(py_val.clone_ref(py))?);
                }
            }
        } else {
            // Is a string, treat as a single value
            hash_set.insert(PyValue::new(py_val.clone_ref(py))?);
        }

        // Single value
        query.insert(attr.clone(), hash_set);
    }

    Ok(query)
}


pub fn _add_index(
    index: &mut std::sync::RwLockWriteGuard<'_, HashMap<String, HashMap<PyValue, HashSet<StoredItem>>>>, 
    obj: &StoredItem, 
    attr: &str, 
    value: PyValue
){
   
    let attr_entry: &mut HashMap<PyValue, HashSet<StoredItem>> = index.entry(attr.to_string())
        .or_insert_with(|| HashMap::<PyValue, HashSet<StoredItem>>::new());
    
    let val_entry: &mut HashSet<StoredItem> = attr_entry.entry(value)
        .or_insert_with(|| HashSet::<StoredItem>::new());
    
    val_entry.insert(obj.clone());
}

fn _reduced(
    index: &HashMap<String, HashMap<PyValue, HashSet<StoredItem>>>,
    query: &HashMap<String, HashSet<PyValue>>
) -> HashMap<String, HashMap<PyValue, HashSet<StoredItem>>> {

    let survivors = filter_index_by_hashes(index, query);

    let mut new_index = HashMap::new();

    for item in survivors {
        let attr_map = item.attr_values.read().unwrap();
        
        for (attr, val) in attr_map.iter() {
            if attr.starts_with("_") {
                continue;
            }
            new_index
                .entry(attr.clone())
                .or_insert_with(HashMap::new)
                .entry(val.clone())
                .or_insert_with(HashSet::new)
                .insert(item.clone());
        }
    }

    new_index
}

fn _reduced_in_place(
    index: &mut HashMap<String, HashMap<PyValue, HashSet<StoredItem>>>,
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
        let attr_map = item.attr_values.read().unwrap();
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
    index: &mut std::sync::RwLockWriteGuard<'_, HashMap<String, HashMap<PyValue, HashSet<StoredItem>>>>, 
    obj: &StoredItem, 
    attr: &str, 
    value_hash: PyValue
){

    if index.contains_key(attr){
        if let Some(val) = index.get_mut(attr) { 
            if val.contains_key(&value_hash){
                if let Some(val) = val.get_mut(&value_hash) { 
                    val.remove(obj); 
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
