use pyo3::prelude::*;
use pyo3::types::PyAny;
use std::{hash::{Hash, Hasher}, sync::Arc};

#[derive(Clone, Debug)]
pub enum RustCastValue {
    Int(i64),
    Float(f64),
    Str(String),
    Unknown,
}

#[derive(Debug)]
pub struct PyValue {
    obj: Arc<Py<PyAny>>,
    primitave: RustCastValue,
    hash: u64,
}

impl PyValue {
    pub fn new(obj: &Py<PyAny>) -> Self {
        Python::with_gil(|py| {
            let bound = obj.clone_ref(py).into_bound(py);

            let primitave = if let Ok(v) = bound.extract::<i64>() {
                RustCastValue::Int(v)
            } else if let Ok(v) = bound.extract::<f64>() {
                RustCastValue::Float(v)
            } else if let Ok(v) = bound.extract::<String>() {
                RustCastValue::Str(v)
            } else {
                RustCastValue::Unknown
            };

            let hash = match bound.hash() {
                Ok(i) => i as u64,
                Err(_) => 0,
            };

            Self {
                obj: Arc::new(obj.clone_ref(py)),
                primitave,
                hash,
            }
        })
    }

    pub fn get_primitive(&self) -> &RustCastValue {
        &self.primitave
    }

    pub fn get_obj(&self) -> &Py<PyAny> {
        &self.obj
    }
}

impl PartialEq for PyValue {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Clone for PyValue {
    fn clone(&self) -> Self {
        Self {
            obj: self.obj.clone(),
            primitave: self.primitave.clone(),
            hash: self.hash,
        }
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
        Ok(self.obj.clone_ref(py).into_bound(py))
    }
}