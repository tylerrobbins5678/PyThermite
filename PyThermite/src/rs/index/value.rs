use pyo3::{prelude::*, PyTypeInfo};
use pyo3::types::PyAny;
use rustc_hash::FxHashMap;
use smol_str::SmolStr;
use std::ops::Deref;
use std::sync::Arc;
use std::{hash::{Hash, Hasher}};

use crate::index::stored_item::StoredItem;
use crate::index::{types, Indexable};

#[derive(Clone, Debug)]
pub enum RustCastValue {
    Int(i64),
    Float(f64),
    Str(String),
    Ind(Arc<Py<Indexable>>),
    Unknown,
}

#[derive(Debug)]
pub struct PyValue {
    obj: Arc<Py<PyAny>>,
    primitave: RustCastValue,
    hash: u64,
}

impl PyValue {
    pub fn new<'py>(obj: Bound<'py, PyAny>) -> Self {

        let py_type = obj.get_type();

        let primitave = if py_type.is(pyo3::types::PyInt::type_object(obj.py())) {
            RustCastValue::Int(obj.extract::<i64>().unwrap())
        } else if py_type.is(pyo3::types::PyFloat::type_object(obj.py())) {
            RustCastValue::Float(obj.extract::<f64>().unwrap())
        } else if py_type.is(pyo3::types::PyString::type_object(obj.py())) {
            RustCastValue::Str(obj.extract::<String>().unwrap())
        } else if py_type.is_subclass(types::indexable_type().bind(obj.py())).unwrap_or(false) {
            RustCastValue::Ind(Arc::new(obj.extract::<Py<Indexable>>().unwrap()))
        } else {
            RustCastValue::Unknown
        };

        let hash = match obj.hash() {
            Ok(i) => i as u64,
            Err(_) => 0,
        };

        Self {
            obj: Arc::new(obj.into()),
            primitave,
            hash,
        }
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