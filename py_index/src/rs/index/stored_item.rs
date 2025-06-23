use pyo3::{basic::CompareOp, types::PyAnyMethods, Bound, BoundObject, IntoPyObject, Py, PyAny, PyObject, Python};
use std::{collections::HashMap, hash::{Hash, Hasher}, sync::{Arc, RwLock}};

use crate::index::{value::PyValue, Indexable};

#[derive(Debug)]
pub struct StoredItem{
    pub item: Indexable,
    pub attr_values: Arc<RwLock<HashMap<String, PyValue>>>,
}

impl StoredItem {
    pub fn new(item: &Indexable, attr_values: Arc<RwLock<HashMap<String, PyValue>>>) -> Self {
        Self {
            item: item.clone(),
            attr_values: attr_values
        }
    }
}

impl Clone for StoredItem {
    fn clone(&self) -> Self {
        StoredItem {
            item: self.item.clone(),
            attr_values: self.attr_values.clone()
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

        let pycell: Py<Indexable> = Py::new(py, self.item.clone()).unwrap();
        Ok(pycell.into_pyobject(py)?.into_any())
    }
}