use std::{sync::{Arc, RwLock}};

use croaring::Bitmap;
use pyo3::{pyclass, pymethods, Bound, IntoPyObject, Py, PyAny, PyResult, Python};
use rustc_hash::FxHashMap;

use crate::index::{query::{filter_index_by_hashes, kwargs_to_hash_query, QueryMap}, stored_item::StoredItem, value::PyValue, Index, Indexable};



#[pyclass]
#[derive(Clone)]
pub struct FilteredIndex {
    pub index: Arc<RwLock<FxHashMap<String, Box<QueryMap>>>>,
    pub items: Arc<RwLock<Vec<Option<Arc<StoredItem>>>>>,
    pub allowed_items: Bitmap,
}


#[pymethods]
impl FilteredIndex{

    #[pyo3(signature = (**kwargs))]
    pub fn reduced<'py>(
        &self,
        py: Python,
        kwargs: Option<FxHashMap<String, pyo3::Bound<'py, PyAny>>>,
    ) -> PyResult<FilteredIndex> {
        let query = kwargs_to_hash_query(kwargs.unwrap_or_default())?;
        py.allow_threads(|| {
            let index = self.index.read().unwrap();
            Ok(FilteredIndex {
                index: self.index.clone(),
                items: self.items.clone(),
                allowed_items: filter_index_by_hashes(&index, &query).and(&self.allowed_items)
            })
        })
    }

    pub fn collect(&self, py:Python) -> PyResult<Vec<Py<Indexable>>> {
        self.get_from_indexes(py, &self.allowed_items)
    }

    pub fn rebase(&self, py: Python) -> PyResult<Index> {

        let items = self.items.read().unwrap();
        let max_size = self.allowed_items.maximum().unwrap_or(0);

        let res_index = Index{
            index: Arc::new(RwLock::new(FxHashMap::default())),
            items: Arc::new(RwLock::new(Vec::with_capacity(max_size as usize))),
            allowed_items: self.allowed_items.clone()
        };

        let mut new_index = res_index.index.write().unwrap();
        let mut new_items = res_index.items.write().unwrap();

        let res_index_arc = Arc::new(res_index.clone());
        new_items.resize(max_size as usize + 1, None);
        
        for idx in self.allowed_items.iter() {
            let item = items[idx as usize].as_ref().unwrap();

            let mut py_item = item.py_item.bind(py).borrow_mut();
            py_item.add_index(res_index_arc.clone());

            new_items[idx as usize] = Some(item.clone());
            
            for (attr, val) in py_item.py_values.iter() {
                new_index
                    .entry(attr.clone())
                    .or_insert_with(|| Box::new(QueryMap::new()))
                    .insert(val, idx);
            }
        }

        drop(new_index);
        drop(new_items);

        Ok(res_index)
    }
}

impl FilteredIndex{

    fn get_from_indexes(&self, py: Python, indexes: &Bitmap) -> PyResult<Vec<Py<Indexable>>>{
        let items = self.items.read().unwrap();
        let results: Vec<Py<Indexable>> = indexes.iter()
            .map(|arc| items.get(arc as usize).unwrap().clone().unwrap().py_item.clone_ref(py))
            .collect();
        Ok(results)
    }

}