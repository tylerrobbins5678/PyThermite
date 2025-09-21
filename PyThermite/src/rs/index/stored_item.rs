use pyo3::{types::PyAnyMethods, Bound, IntoPyObject, Py, PyAny, PyRef, PyRefMut, Python};
use std::{hash::{Hash, Hasher}, sync::Arc};

use crate::index::Indexable;

#[derive(Clone, Debug)]
pub struct StoredItem{
    py_item: Arc<Py<Indexable>>,
}

impl<'py> StoredItem {
    pub fn new(py_item: Arc<Py<Indexable>>) -> Self {
        Self {
            py_item: py_item.clone(),
        }
    }

    pub fn get_py_ref(&self, py: Python) -> Py<Indexable> {
        self.py_item.clone_ref(py)
    }

    pub fn borrow_py_ref(&self, py: Python<'py>) -> PyRef<'py, Indexable> {
        self.py_item.bind(py).borrow()
    }

    pub fn borrow_py_ref_mut(&self, py: Python<'py>) -> PyRefMut<'py, Indexable> {
        self.py_item.bind(py).borrow_mut()
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