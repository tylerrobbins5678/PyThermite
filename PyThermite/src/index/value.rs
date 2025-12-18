use arc_swap::AsRaw;
use pyo3::{IntoPyObjectExt, PyTypeInfo, prelude::*};
use pyo3::types::{PyAny, PyDict, PyList, PySet, PyTuple};
use rustc_hash::FxHasher;
use smol_str::SmolStr;
use std::ops::Deref;
use std::primitive;
use std::ptr::NonNull;
use std::sync::Arc;
use std::{hash::{Hash, Hasher}};
use pyo3::conversion::IntoPyObject;

use crate::index::types::{bool_type_ptrs, float_type_ptrs, int_type_ptrs, str_type_ptrs};
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
pub struct StoredIndexable {
    pub python_handle: Arc<Py<Indexable>>,
    pub owned_handle: Arc<Indexable>
}

impl StoredIndexable {
    pub fn from_py_ref(py_ref: PyRef<Indexable>, py: Python) -> Self {
        Self {
            owned_handle: Arc::new(Indexable::from_py_ref(&py_ref, py)),
            python_handle: Arc::new(py_ref.into_pyobject(py).unwrap().unbind())
        }
    }
}


#[derive(Clone, Debug)]
pub enum RustCastValue {
    Int(i64),
    Float(f64),
    Str(SmolStr),
    Bool(bool),
    Iterable(PyIterable),
    Ind(StoredIndexable),
    Unknown,
}

#[derive(Debug)]
pub struct PyValue {
    obj: Option<Arc<Py<PyAny>>>,
    primitave: RustCastValue,
    hash: u64,
}

impl PyValue {
    pub fn new<'py>(obj: Bound<'py, PyAny>) -> Self {

        let py_type = obj.get_type();
        let py = obj.py();

        // primitave types - check first
        let primitave = 
        if int_type_ptrs(py).contains(&py_type.as_ptr()) {
            RustCastValue::Int(obj.extract::<i64>().expect("type checked"))
        } else if float_type_ptrs(py).contains(&py_type.as_ptr()) {
            RustCastValue::Float(obj.extract::<f64>().expect("type checked"))
        } else if str_type_ptrs(py).contains(&py_type.as_ptr()) {
            RustCastValue::Str(SmolStr::new(obj.extract::<&str>().expect("type checked")))
        } else if bool_type_ptrs(py).contains(&py_type.as_ptr()) {
            RustCastValue::Bool(obj.extract::<bool>().expect("type checked"))

        // complex types - pointer based equality
        } else if py_type.is_subclass(types::indexable_type().bind(py)).unwrap_or(false) {
            let py_ref = obj.extract::<PyRef<Indexable>>().expect("type checked");
            RustCastValue::Ind(StoredIndexable::from_py_ref(py_ref, py))
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

        let hash = Self::hash_primitave(&primitave);

        Self {
            obj: Some(Arc::new(obj.into())),
            primitave,
            hash,
        }
    }

    fn hash_primitave(primitave: &RustCastValue) -> u64 {
        let mut hasher = FxHasher::default();
        match &primitave {
            RustCastValue::Int(i) => {
                hasher.write_u64(i.cast_unsigned())
            },
            RustCastValue::Float(f) => {
                hasher.write_u64(f.to_bits())
            },
            RustCastValue::Bool(b) => {
                hasher.write_u64(*b as u64)
            },
            RustCastValue::Str(s) => {
                s.hash(&mut hasher);
            },
            RustCastValue::Ind(ind) => {
                hasher.write_u64(ind.python_handle.as_ptr() as u64)
            },
            RustCastValue::Iterable(itr) => {
                hasher.write_u64(itr as *const _ as u64)
            },
            RustCastValue::Unknown => hasher.write_u64(0u64),
        };
        hasher.write_u8({
            match &primitave {
                RustCastValue::Int(_) => 1,
                RustCastValue::Float(_) => 2,
                RustCastValue::Str(_) => 3,
                RustCastValue::Bool(_) => 4,
                RustCastValue::Iterable(_) => 5,
                RustCastValue::Ind(_) => 6,
                RustCastValue::Unknown => 7
            }
        });
        hasher.finish()
    }

    pub fn from_primitave(prim: RustCastValue) -> Self {
        let hash = Self::hash_primitave(&prim);

        Self {
            obj: None,
            primitave: prim,
            hash,
        }
    }

    pub fn get_primitive(&self) -> &RustCastValue {
        &self.primitave
    }

    pub fn get_hash(&self) -> u64 {
        self.hash
    }

    pub fn get_obj(&self, py: Python) -> Py<PyAny> {
        match &self.primitave {
            RustCastValue::Int(v) => v.into_py_any(py).unwrap(),
            RustCastValue::Float(v) => v.into_py_any(py).unwrap(),
            RustCastValue::Bool(v) => v.into_py_any(py).unwrap(),
            RustCastValue::Str(v) => v.into_py_any(py).unwrap(),
            _ => self.obj.as_ref().unwrap().clone_ref(py)
        }
    }
}

impl PartialEq for PyValue {
    fn eq(&self, other: &Self) -> bool {
        if self.hash != other.hash {
            return false;
        }
        match (&self.primitave, &other.primitave) {
            (RustCastValue::Int(a), RustCastValue::Int(b)) => a == b,
            (RustCastValue::Float(a), RustCastValue::Float(b)) => a == b,
            (RustCastValue::Int(a), RustCastValue::Float(b)) => (*a as f64) == *b,
            (RustCastValue::Float(a), RustCastValue::Int(b)) => *a == (*b as f64),
            (RustCastValue::Bool(a), RustCastValue::Int(b)) => (*a as i64) == *b,
            (RustCastValue::Int(a), RustCastValue::Bool(b)) => *a == (*b as i64),
            (RustCastValue::Bool(a), RustCastValue::Bool(b)) => a == b,
            (RustCastValue::Str(a), RustCastValue::Str(b)) => a == b,
            // fallback to pointer identity
            (RustCastValue::Ind(a), RustCastValue::Ind(b)) => a.python_handle.as_ptr() == b.python_handle.as_ptr(),
            (RustCastValue::Iterable(a), RustCastValue::Iterable(b)) => {
                std::ptr::eq(a as *const _, b as *const _)
            },
            _ => false,
        }
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
        Ok(self.get_obj(py).into_bound(py))
    }
}