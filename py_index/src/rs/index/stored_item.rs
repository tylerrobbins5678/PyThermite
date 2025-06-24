use pyo3::{basic::CompareOp, types::PyAnyMethods, Bound, BoundObject, IntoPyObject, Py, PyAny, PyObject, Python};
use std::{collections::HashMap, hash::{Hash, Hasher}, sync::{Arc, RwLock}};

use crate::index::{py_dict_values::UnsafePyValues, value::PyValue, Indexable};

pub struct StoredItem{
    pub item: Indexable,
    pub py_item: Arc<Py<Indexable>>,
}

impl StoredItem {
    pub fn new(item: Indexable, py_item: Arc<Py<Indexable>>) -> Self {
        Self {
            item: item.clone(),
            py_item: py_item.clone()
        }
    }
}

impl Clone for StoredItem {
    fn clone(&self) -> Self {
        StoredItem {
            item: self.item.clone(),
            py_item: self.py_item.clone()
        }
    }
}

impl PartialEq for StoredItem {
    fn eq(&self, other: &Self) -> bool {
        self.item == other.item
    }
}

impl Eq for StoredItem {}

impl Hash for StoredItem {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.item.hash(state)
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