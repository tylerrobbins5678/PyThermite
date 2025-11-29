use std::{sync::{Arc, RwLock}};

use croaring::Bitmap;
use pyo3::{pyclass, pymethods, Py, PyAny, PyResult, Python};
use rustc_hash::FxHashMap;
use smol_str::SmolStr;

use crate::index::{Index, Indexable, PyQueryExpr};
use crate::index::core::stored_item::StoredItem;
use crate::index::core::index::IndexAPI;
use crate::index::core::query::{evaluate_query, filter_index_by_hashes, kwargs_to_hash_query, QueryMap};

#[pyclass]
#[derive(Clone)]
pub struct FilteredIndex {
    pub index: Arc<RwLock<FxHashMap<SmolStr, Box<QueryMap>>>>,
    pub items: Arc<RwLock<Vec<Option<StoredItem>>>>,
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

    pub fn reduced_query(
        &self,
        query: PyQueryExpr,
    ) -> FilteredIndex {
        let index = self.index.read().unwrap();
        let allowed = &self.allowed_items;
        self.filter_from_bitmap(
            evaluate_query(&index, &allowed, &query.inner)
        )
    }

    pub fn collect(&self, py:Python) -> PyResult<Vec<Py<Indexable>>> {
        self.get_from_indexes(py, &self.allowed_items)
    }

    pub fn rebase(&self, py: Python) -> PyResult<Index> {

        let items = self.items.read().unwrap();
        let max_size = self.allowed_items.maximum().unwrap_or(0);
        let index_api = IndexAPI {
            index: Arc::new(RwLock::new(FxHashMap::default())),
            items: Arc::new(RwLock::new(Vec::with_capacity(max_size as usize))),
            allowed_items: Arc::new(RwLock::new(self.allowed_items.clone())),
            parent_index: None,
        };

        let mut new_index = index_api.index.write().unwrap();
        let mut new_items = index_api.items.write().unwrap();
        
        let res_index_arc = Arc::downgrade(&Arc::new(index_api.clone()));

        new_items.resize(max_size as usize + 1, None);
        
        for idx in self.allowed_items.iter() {
            let item = items[idx as usize].as_ref().unwrap();

            let py_item = item.borrow_py_ref(py);
            py_item.add_index(res_index_arc.clone());

            new_items[idx as usize] = Some(item.clone());
            
            for (attr, val) in py_item.get_py_values().iter() {
                new_index
                    .entry(SmolStr::new(attr))
                    .or_insert_with(|| Box::new(QueryMap::new(res_index_arc.clone())))
                    .insert(val, idx);
            }
        }

        drop(new_index);
        drop(new_items);

        let res_index = Index {
            inner: Arc::new(index_api)
        };

        Ok(res_index)
    }
}