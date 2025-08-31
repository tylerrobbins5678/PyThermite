use pyo3::{types::PyAnyMethods, Bound, IntoPyObject, Py, PyAny, PyRef, Python};
use std::{hash::{Hash, Hasher}, sync::Arc};

use crate::index::Indexable;

#[derive(Clone)]
pub struct StoredItem{
    pub py_item: Arc<Py<Indexable>>,
}

impl StoredItem {
    pub fn new(py_item: Arc<Py<Indexable>>) -> Self {
        Self {
            py_item: py_item.clone(),
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