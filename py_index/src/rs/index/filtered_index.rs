use std::{collections::{HashMap, HashSet}, sync::{Arc, RwLock}, time::Instant};

use pyo3::{pyclass, pymethods, Py, PyAny, PyResult, Python};

use crate::index::{query::{filter_index_by_hashes, kwargs_to_hash_query}, stored_item::StoredItem, value::PyValue, Index, Indexable};



#[pyclass]
#[derive(Clone)]
pub struct FilteredIndex {
    pub index: Arc<RwLock<HashMap<String, HashMap<PyValue, HashSet<Arc<StoredItem>>>>>>,       // Shared with Index
    pub allowed_items: HashSet<Arc<StoredItem>>,
}


#[pymethods]
impl FilteredIndex{

    #[pyo3(signature = (**kwargs))]
    pub fn reduced(
        &self,
        py: Python,
        kwargs: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<FilteredIndex> {
        let query = kwargs_to_hash_query(py, &kwargs.unwrap_or_default())?;
        py.allow_threads(|| {
            let index = self.index.read().unwrap();
            Ok(FilteredIndex {
                index: self.index.clone(),
                allowed_items: filter_index_by_hashes(&index, &query).intersection(&self.allowed_items).cloned().collect()
            })
        })
    }

    pub fn collect(&self, py:Python) -> PyResult<Vec<Py<Indexable>>> {
        Ok(self.allowed_items.iter().map(|arc| (*arc).py_item.clone_ref(py)).collect())
    }

    pub fn rebase(&self) -> PyResult<Index> {
        let start = Instant::now();

        let mut new_index: HashMap<String, HashMap<PyValue, HashSet<Arc<StoredItem>>>> = HashMap::new();

        for item in &self.allowed_items {
            let attr_map = unsafe { item.item.py_values.map_ref() };
            
            for (attr, val) in attr_map.iter() {
                new_index
                    .entry(attr.to_string())
                    .or_insert_with(HashMap::new)
                    .entry(val.clone())
                    .or_insert_with(HashSet::new)
                    .insert(item.clone());
            }
        }

        let duration = start.elapsed();
        eprintln!("Elapsed time to rebase index: {:.3?}", duration);

        Ok(Index{
            index: Arc::new(RwLock::new(new_index))
        })
    }
}