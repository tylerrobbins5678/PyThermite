use pyo3::{prelude::*, PyTypeInfo};
use pyo3::types::{PyAny, PyDict, PyList, PySet, PyTuple};
use std::sync::Arc;
use std::{hash::{Hash, Hasher}};

use crate::index::{types, Indexable};

#[derive(Debug)]
pub enum PyIterable {
    List(Py<PyList>),
    Dict(Py<PyDict>),
    Tuple(Py<PyTuple>),
    Set(Py<PySet>)
}

impl Clone for PyIterable {
    fn clone(&self) -> Self {
        Python::with_gil(|py| {
            match self {
                Self::List(arg0) => Self::List(arg0.clone_ref(py)),
                Self::Dict(arg0) => Self::Dict(arg0.clone_ref(py)),
                Self::Tuple(arg0) => Self::Tuple(arg0.clone_ref(py)),
                Self::Set(arg0) => Self::Set(arg0.clone_ref(py)),
            }
        })
    }
}

#[derive(Clone, Debug)]
pub enum RustCastValue {
    Int(i64),
    Float(f64),
    Str(String),
    Iterable(PyIterable),
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
        let py = obj.py();

        let primitave = if py_type.is(pyo3::types::PyInt::type_object(py)) {
            RustCastValue::Int(obj.extract::<i64>().expect("type checked"))
        } else if py_type.is(pyo3::types::PyFloat::type_object(py)) {
            RustCastValue::Float(obj.extract::<f64>().expect("type checked"))
        } else if py_type.is(pyo3::types::PyString::type_object(py)) {
            RustCastValue::Str(obj.extract::<String>().expect("type checked"))
        } else if py_type.is_subclass(types::indexable_type().bind(py)).unwrap_or(false) {
            RustCastValue::Ind(Arc::new(obj.extract::<Py<Indexable>>().expect("type checked")))
        } else if py_type.is(pyo3::types::PyList::type_object(py)) {
            RustCastValue::Iterable(PyIterable::List(obj.extract::<Py<PyList>>().expect("type checked")))
        } else if py_type.is(pyo3::types::PyTuple::type_object(py)) {
            RustCastValue::Iterable(PyIterable::Tuple(obj.extract::<Py<PyTuple>>().expect("type checked")))
        } else if py_type.is(pyo3::types::PyDict::type_object(py)) {
            RustCastValue::Iterable(PyIterable::Dict(obj.extract::<Py<PyDict>>().expect("type checked")))
        } else if py_type.is(pyo3::types::PySet::type_object(py)) {
            RustCastValue::Iterable(PyIterable::Set(obj.extract::<Py<PySet>>().expect("type checked")))
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

    pub fn new_iter<'py>(obj: Bound<'py, PyAny>) -> Box<dyn Iterator<Item = PyValue> + 'py> {
        let py_type = obj.get_type();
        let py = obj.py();

        // Only iterate over native Python containers
        let is_container = py_type.is(pyo3::types::PyList::type_object(py))
            || py_type.is(pyo3::types::PyTuple::type_object(py))
            || py_type.is(pyo3::types::PyDict::type_object(py))
            || py_type.is(pyo3::types::PySet::type_object(py));
        
        if is_container {
            if let Ok(iter) = obj.try_iter() {
                return Box::new(iter.filter_map(|item| item.ok().map(PyValue::new)));
            }
        }

        Box::new(std::iter::once(PyValue::new(obj)))
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