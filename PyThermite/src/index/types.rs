use std::sync::{Arc, RwLock};

use once_cell::sync::{Lazy, OnceCell};
use pyo3::{Py, PyAny, PyTypeInfo, Python, types::{PyAnyMethods, PyType}};
use smallvec::SmallVec;

use crate::index::{HybridHashmap, Indexable, core::{index::IndexAPI, query::QueryMap}};

pub type StrId = u32;
pub type IndexTree = Arc<RwLock<Vec<QueryMap>>>;

// devaults
pub static DEFAULT_INDEX_ARC: Lazy<Arc<IndexAPI>> = Lazy::new(|| Arc::new(IndexAPI::new(None)));
pub static DEFAULT_INDEXABLE: Lazy<Indexable> = Lazy::new(|| Indexable::default());

pub static DEFAULT_INDEXABLE_ARC: Lazy<Arc<Indexable>> = Lazy::new(|| {
    Arc::new(Indexable::default())
});
pub static DEFAULT_PY_INDEXABLE_ARC: Lazy<Arc<Py<Indexable>>> = Lazy::new(|| {
    Python::with_gil(|py| {
        Arc::new(Py::new(py, Indexable::default()).unwrap())
    })
});
pub static DEFAULT_PY_NONE_ARC: Lazy<Arc<Py<PyAny>>> = Lazy::new(|| {
    Python::with_gil(|py| {
        Arc::new(py.None())
    })
});


// types
static INDEXABLE_TYPE: OnceCell<Py<PyType>> = OnceCell::new();

pub fn indexable_type() -> &'static Py<PyType> {
    INDEXABLE_TYPE
    .get_or_init(|| {
            Python::with_gil(| py | {
                py.get_type::<Indexable>().into()
            })
        }
    ) // runs once
}

macro_rules! cached_py_type {
    // Built-in type
    ($fn_name:ident, $static_name:ident, $builtin:ident) => {
        static $static_name: OnceCell<Py<PyType>> = OnceCell::new();

        pub fn $fn_name(py: Python<'_>) -> &Py<PyType> {
            $static_name.get_or_init(|| {
                pyo3::types::$builtin::type_object(py).unbind()
            })
        }
    };
    // Module type
    ($fn_name:ident, $static_name:ident, $module:literal, $attr:literal) => {
        static $static_name: OnceCell<Option<Py<PyType>>> = OnceCell::new();

        pub fn $fn_name(py: Python<'_>) -> Option<&Py<PyType>> {
            $static_name.get_or_init(|| {
                let module = py.import($module).ok()?;
                let attr = module.getattr($attr).ok()?;
                let ty = attr.downcast_into::<PyType>().ok()?;
                Some(ty.unbind())
            }).as_ref()
        }
    };
}

macro_rules! cached_type_ptrs {
    ($fn_name:ident, $type_fn:ident, $len:expr) => {
        pub fn $fn_name(py: Python<'_>) -> SmallVec<[*mut pyo3::ffi::PyObject; $len]> {
            $type_fn(py).iter().map(|t| t.as_ptr()).collect()
        }
    };
}

// Int

cached_py_type!(py_int_type, PY_INT_TYPE_CELL, PyInt); // built-in int
cached_py_type!(np_int64_type, NP_INT64_CELL, "numpy", "int64"); // module type
static INT_TYPES: OnceCell<SmallVec<[Py<PyType>; 2]>> = OnceCell::new();

pub fn int_types(py: Python<'_>) -> &'_ [Py<PyType>] {
    INT_TYPES.get_or_init(|| {
        let mut types = SmallVec::<[_; 2]>::new();
        // Python int
        types.push(py_int_type(py).clone_ref(py));
        // Optional numpy int64
        if let Some(np64) = np_int64_type(py) {
            types.push(np64.clone_ref(py));
        }
        types
    })
}

cached_type_ptrs!(int_type_ptrs, int_types, 2);

// Float types

cached_py_type!(py_float_type, PY_FLOAT_TYPE_CELL, PyFloat); // built-in int
cached_py_type!(np_float64_type, NP_FLOAT64_CELL, "numpy", "float64"); // module type
static FLOAT_TYPES: OnceCell<SmallVec<[Py<PyType>; 2]>> = OnceCell::new();

pub fn float_types(py: Python<'_>) -> &'_ [Py<PyType>] {
    FLOAT_TYPES.get_or_init(|| {
        let mut types = SmallVec::<[_; 2]>::new();
        // Python float
        types.push(py_float_type(py).clone_ref(py));
        // Optional numpy float64
        if let Some(np64) = np_float64_type(py) {
            types.push(np64.clone_ref(py));
        }
        types
    })
}

cached_type_ptrs!(float_type_ptrs, float_types, 2);

// Bool types

cached_py_type!(py_bool_type, PY_BOOL_TYPE_CELL, PyBool); // built-in int
cached_py_type!(np_bool_type, NP_BOOL_CELL, "numpy", "bool"); // module type
static BOOL_TYPES: OnceCell<SmallVec<[Py<PyType>; 2]>> = OnceCell::new();

pub fn bool_types(py: Python<'_>) -> &'_ [Py<PyType>] {
    BOOL_TYPES.get_or_init(|| {
        let mut types = SmallVec::<[_; 2]>::new();
        // Python float
        types.push(py_bool_type(py).clone_ref(py));
        // Optional numpy float64
        if let Some(bool_) = np_bool_type(py) {
            types.push(bool_.clone_ref(py));
        }
        types
    })
}

cached_type_ptrs!(bool_type_ptrs, bool_types, 2);


// str types

cached_py_type!(py_str_type, PY_STR_TYPE_CELL, PyString); // built-in int
cached_py_type!(np_str_type, NP_STR_CELL, "numpy", "str_"); // module type
static STR_TYPES: OnceCell<SmallVec<[Py<PyType>; 2]>> = OnceCell::new();

pub fn str_types(py: Python<'_>) -> &'_ [Py<PyType>] {
    STR_TYPES.get_or_init(|| {
        let mut types = SmallVec::<[_; 2]>::new();
        // Python float
        types.push(py_str_type(py).clone_ref(py));
        // Optional numpy float64
        if let Some(str_) = np_str_type(py) {
            types.push(str_.clone_ref(py));
        }
        types
    })
}

cached_type_ptrs!(str_type_ptrs, str_types, 2);