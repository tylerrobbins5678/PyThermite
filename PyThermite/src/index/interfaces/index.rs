
use std::{ops::Deref, sync::{Arc, Weak}};
use pyo3::prelude::*;
use rustc_hash::FxHashMap;
use smol_str::SmolStr;

use crate::index::{Indexable, PyQueryExpr, core::{query::kwargs_to_hash_query, structures::hybrid_set::HybridSetOps}};
use crate::index::interfaces::filtered_index::FilteredIndex;
use crate::index::core::index::IndexAPI;
use crate::index::core::stored_item::StoredItem;
use crate::index::value::PyValue;

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
        &self,
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

    pub fn add_object_many(&self, py: Python, objs: Vec<PyRef<Indexable>>) -> PyResult<()> {
        
        let raw_objs: Vec<(Indexable, Py<Indexable>)> = objs.into_iter().map(|obj| {
            (
                Indexable::from_py_ref(&obj, py),
                obj.into_pyobject(py).unwrap().unbind()
            )
        }).collect();

        py.allow_threads(|| {
            let weak_index = Arc::downgrade(&self.inner);
            self.inner.add_object_many(weak_index, raw_objs);
        });

        Ok(())

    }

    pub fn add_object(&self, py: Python, py_ref: PyRef<Indexable>) -> PyResult<()> {

        let rust_handle = Arc::new(Indexable::from_py_ref(&py_ref, py));
        let py_handle = Arc::new(py_ref.into_pyobject(py)?.unbind());

        py.allow_threads(||{
            let weak_index = Arc::downgrade(&self.inner);
            rust_handle.add_index(weak_index.clone());
            let stored_item = StoredItem::new(py_handle, rust_handle.clone(), None);
            // i dont like this clone - need to remove
            let py_val_hashmap = rust_handle.get_py_values();
            self.inner.add_object(weak_index, rust_handle.id, stored_item, py_val_hashmap);
        });


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

    pub fn group_by(
        &self,
        py: Python,
        attr: &str
    ) -> PyResult<FxHashMap<PyValue, FilteredIndex>> {
        py.allow_threads( move || {
            let groups = self.inner.group_by(SmolStr::new(attr));
            let mut res = FxHashMap::default();

            match groups {
                Some(r) => {
                    for (py_vals, allowed) in r {
                        res.insert(py_vals, 
                            self.inner.filter_from_bitmap(allowed.as_bitmap())
                        );
                    }
                    Ok(res)
                },
                None => Ok(res)
            }
        })
    }

    pub fn union_with(&self, py: Python, other: &Index) -> PyResult<()>{
        py.allow_threads(|| {
            self.inner.union_with(&other.inner)
        })
    }

}