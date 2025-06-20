use pyo3::prelude::*;
use pyo3::types::PyAny;
use std::hash::{Hash, Hasher};

pub struct PyValue {
    obj: Py<PyAny>,
    hash: u64,
}

impl PyValue {
    pub fn new(obj: Py<PyAny>) -> PyResult<Self> {
        Python::with_gil( | py | {
            let hash = obj.clone_ref(py).into_bound(py).hash()? as u64;
            Ok(Self {
                obj: obj,
                hash,
            })
        })
    }

    pub fn get_obj(&self) -> &Py<PyAny> {
        &self.obj
    }
}

impl PartialEq for PyValue {
    fn eq(&self, other: &Self) -> bool {
        Python::with_gil(|py| {
            let a = self.obj.clone_ref(py).into_bound(py);
            let b = other.obj.clone_ref(py).into_bound(py);
            a.eq(b).unwrap_or(false)
        })
    }
}

impl Clone for PyValue {
    fn clone(&self) -> Self {
        Python::with_gil(|py| {
            Self {
                obj: self.obj.clone_ref(py),
                hash: self.hash,
            }
        })
    }
}

impl Eq for PyValue {}

impl Hash for PyValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl<'py> IntoPyObject<'py> for PyValue {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = std::convert::Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(self.obj.into_bound(py))
    }
}