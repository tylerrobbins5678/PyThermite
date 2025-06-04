
use std::{collections::{HashMap, HashSet}, sync::{Arc, Mutex}};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyString;


use super::stored_item::StoredItem;


#[pyclass]
#[derive(Clone)]
pub struct Index{
    pub index: Arc<Mutex<HashMap<String, HashMap<u64, HashSet<StoredItem>>>>>
}

#[pymethods]
impl Index{

    #[new]
    pub fn new() -> Self {
        Self {
            index: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn add_object(&mut self, py: Python, obj: PyObject) -> PyResult<()> {

        let py_index_obj = Py::new(py, self.clone())?;
        obj.call_method1(py, "add_index", (py_index_obj,))?;

        // Get the __dict__ attribute and values
        let dict: HashMap<String, PyObject> = obj.getattr(py, "__dict__")?.extract(py).unwrap();
        let mut index: std::sync::MutexGuard<'_, HashMap<String, HashMap<u64, HashSet<StoredItem>>>> = self.index.lock().unwrap();
        for (key, value) in dict.iter() {
            if key.starts_with("_"){continue;}
            let value_hash = hash_pyobject(py, &value)?;
            let stored_obj = StoredItem::new(py, &obj);
            _add_index(&mut index, &stored_obj, key, value_hash);
        }
        Ok(())
    }

    #[pyo3(signature = (**kwargs))]
    pub fn get_by_attribute(
        &self,
        py: Python,
        kwargs: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<HashSet<StoredItem>> {
        let mut sets_iter = HashSet::new();
        let index = self.index.lock().unwrap();

        for (attr, value) in kwargs.unwrap_or(HashMap::new()).iter() {

            let value_ref = value.into_pyobject(py)?;

            // Skip strings from being treated as iterables
            let is_str = value_ref.is_instance_of::<PyString>();
            let maybe_iter = if is_str {
                None
            } else {
                value_ref.try_iter().ok()
            };

            let mut matching_set: HashSet<&StoredItem> = HashSet::new();

            if let Some(iter) = maybe_iter {
                // Iterable: collect sets for each value
                let mut found_any = false;
                for item in iter {
                    let hash = hash_pyobject(py, &item?.as_ref())?;

                    if let Some(val_map) = index.get(attr) {
                        if let Some(items) = val_map.get(&hash) {
                            matching_set.extend(items.iter());
                            found_any = true;
                        }
                    }
                }

                if !found_any {
                    return Ok(HashSet::new());
                }
            } else {
                // Not iterable or string: treat as single value
                let hash = hash_pyobject(py, &value)?;

                if let Some(val_map) = index.get(attr) {
                    if let Some(items) = val_map.get(&hash) {
                        matching_set.extend(items.iter());
                    } else {
                        return Ok(HashSet::new());
                    }
                } else {
                    return Ok(HashSet::new());
                }
            }
            if sets_iter.is_empty(){
                sets_iter = matching_set;
            } else {
                sets_iter.retain(|item| matching_set.contains(item));
            }
        }
        Ok(sets_iter.iter().cloned().map(|arc| (*arc).clone()).collect())
    }

    pub fn update_index(&mut self, py: Python, obj: Py<PyAny>, attr: String, old_val: Py<PyAny>) -> PyResult<()>{

        if attr.starts_with("_"){return Ok(());}

        let stored_obj = StoredItem::new(py, &obj);
        let new_val = obj.getattr(py, &attr)?;

        let mut index: std::sync::MutexGuard<'_, HashMap<String, HashMap<u64, HashSet<StoredItem>>>> = self.index.lock().unwrap();
        _remove_index(&mut index, &stored_obj, &attr, hash_pyobject(py, &old_val)?);
        _add_index(&mut index, &stored_obj, &attr, hash_pyobject(py, &new_val)?);

        Ok(())
    }

}

pub fn _add_index(
    index: &mut std::sync::MutexGuard<'_, HashMap<String, HashMap<u64, HashSet<StoredItem>>>>, 
    obj: &StoredItem, 
    attr: &str, 
    value_hash: u64
){
   
    let attr_entry: &mut HashMap<u64, HashSet<StoredItem>> = index.entry(attr.to_string())
        .or_insert_with(|| HashMap::<u64, HashSet<StoredItem>>::new());
    
    let val_entry: &mut HashSet<StoredItem> = attr_entry.entry(value_hash)
        .or_insert_with(|| HashSet::<StoredItem>::new());
    
    val_entry.insert(obj.clone());

}

fn _remove_index(
    index: &mut std::sync::MutexGuard<'_, HashMap<String, HashMap<u64, HashSet<StoredItem>>>>, 
    obj: &StoredItem, 
    attr: &str, 
    value_hash: u64
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


fn hash_pyobject(py: Python, object: &Py<PyAny>) -> PyResult<u64> {
    let object = object.into_pyobject(py)?;

    // apparently this is the fastest way to hash a python object?
    match object.hash() {
        Ok(r) => Ok(r as u64),
        Err(_) => Err(PyTypeError::new_err("Unhashable type for attribute")),
    }
}