use pyo3::{Bound, IntoPyObject, PyAny, PyObject, Python};
use std::hash::{Hash, Hasher};


pub struct StoredItem{
    pub item: PyObject,
    pub id_hash: usize,
}

impl StoredItem {
    pub fn new(py: Python, item: &PyObject) -> Self {
        let id_hash = item.as_ptr() as usize;
        Self {
            item: item.clone_ref(py),
            id_hash: id_hash
        }
    }
}

impl Clone for StoredItem {
    fn clone(&self) -> Self {
        Python::with_gil(|py| {
            StoredItem {
                item: self.item.clone_ref(py),
                id_hash: self.id_hash,
            }
        })
    }
}

impl PartialEq for StoredItem {
    fn eq(&self, other: &Self) -> bool {
        // Ideally, compare Python-level equality, or fallback to hash equality:
        self.id_hash == other.id_hash
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