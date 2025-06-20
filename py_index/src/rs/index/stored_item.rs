use pyo3::{basic::CompareOp, types::PyAnyMethods, Bound, IntoPyObject, PyAny, PyObject, Python};
use std::{collections::HashMap, hash::{Hash, Hasher}, sync::{Arc, RwLock}};

use crate::index::value::PyValue;


pub struct StoredItem{
    pub item: PyObject,
    pub id_hash: usize,
    pub attr_values: Arc<RwLock<HashMap<String, PyValue>>>,
}

impl StoredItem {
    pub fn new(py: Python, item: &PyObject, attr_values: Arc<RwLock<HashMap<String, PyValue>>>) -> Self {
        let id_hash = item.as_ptr() as usize;
        Self {
            item: item.clone_ref(py),
            id_hash: id_hash,
            attr_values: attr_values
        }
    }
}

impl Clone for StoredItem {
    fn clone(&self) -> Self {
        Python::with_gil(|py| {
            StoredItem {
                item: self.item.clone_ref(py),
                id_hash: self.id_hash,
                attr_values: self.attr_values.clone()
            }
        })
    }
}

impl PartialEq for StoredItem {
    fn eq(&self, other: &Self) -> bool {
        Python::with_gil(|py| {
            let a = self.item.clone_ref(py).into_bound(py);
            let b = other.item.clone_ref(py).into_bound(py);
            match a.rich_compare(b, CompareOp::Eq) {
                Ok(result) => {
                    // Extract bool from Python object
                    match result.extract::<bool>() {
                        Ok(value) => value,
                        Err(_) => false,
                    }
                }
                Err(_) => false,
            }
        })
    }
}

impl Eq for StoredItem {}

impl Hash for StoredItem {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Use the precomputed hash to feed the Hasher
        state.write_usize(self.id_hash);
    }
}

impl<'py> IntoPyObject<'py> for StoredItem {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = std::convert::Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(self.item.into_bound(py))
    }
}