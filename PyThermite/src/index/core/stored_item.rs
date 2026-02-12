use pyo3::{Bound, IntoPyObject, Py, PyAny, PyRef, Python};
use std::{hash::{Hash, Hasher}, sync::{Arc, Weak}};

use crate::index::{core::{index::IndexAPI, structures::hybrid_set::{HybridSet, HybridSetOps}}, types::{DEFAULT_INDEXABLE_ARC, DEFAULT_PY_INDEXABLE_ARC, StrId}, value::PyValue};
use crate::index::Indexable;


#[derive(Clone, Debug)]
pub struct StoredItem{
    // these two are the same object, one is a rust handle and the other is a python handle
    py_item: Arc<Py<Indexable>>,
    owned_py_item: Arc<Indexable>,
}

impl<'py> StoredItem {
    pub fn new(
        py_handle: Arc<Py<Indexable>>,
        rust_handle: Arc<Indexable>,
    ) -> Self {
        Self {
            py_item: py_handle,
            owned_py_item: rust_handle,
        }
    }

    pub fn with_attr_id<F, R>(&self, str_id: StrId, f: F) -> Option<R>
    where
        F: FnOnce(&PyValue) -> R,
    {
        self.owned_py_item.with_attr_id(str_id, f)
    }

    pub fn get_owned_handle(&self) -> &Arc<Indexable> {
        &self.owned_py_item
    }

    pub fn get_py_ref(&self, py: Python) -> Py<Indexable> {
        self.py_item.clone_ref(py)
    }

    pub fn borrow_py_ref(&self, py: Python<'py>) -> PyRef<'py, Indexable> {
        self.py_item.bind(py).borrow()
    }
}

impl Default for StoredItem {
    fn default() -> Self {
        Self {
            py_item: DEFAULT_PY_INDEXABLE_ARC.clone(),
            owned_py_item: DEFAULT_INDEXABLE_ARC.clone(),
        }
    }
}

impl PartialEq for StoredItem {
    fn eq(&self, other: &Self) -> bool {
        self.py_item.as_ptr() == other.py_item.as_ptr()
    }
}

impl Eq for StoredItem {}

impl Hash for StoredItem {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Better:
        let py_id = self.py_item.as_ptr();
        py_id.hash(state);

    }
}

impl<'py> IntoPyObject<'py> for StoredItem {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = std::convert::Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(self.py_item.clone_ref(py).into_pyobject(py)?.into_any())
    }
}