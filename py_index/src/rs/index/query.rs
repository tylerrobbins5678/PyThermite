use std::{collections::{HashMap, HashSet}, sync::Arc, time::Instant};

use pyo3::{types::{PyAnyMethods, PyString}, Py, PyAny, PyResult, Python};

use crate::index::{stored_item::StoredItem, value::PyValue};




pub fn _reduced(
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



pub fn filter_index_by_hashes(
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
                    per_attr_match = matched.clone();
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

        if first {
            sets_iter = per_attr_match;
        } else {
            sets_iter.retain(|item| per_attr_match.contains(item));
        }
        first = false;
    }

    sets_iter
}

pub fn kwargs_to_hash_query(
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