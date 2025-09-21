use ahash::HashMapExt;
use pyo3::exceptions::PyAttributeError;
use pyo3::types::PyDictMethods;
use pyo3::types::PyStringMethods;
use pyo3::{ffi, IntoPyObjectExt, PyErr, PyRef};

use rustc_hash::FxHashMap;
use smallvec::SmallVec;
use once_cell::sync::Lazy;

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::hash::{Hash, Hasher};
use pyo3::{pyclass, pymethods, types::{PyAnyMethods, PyDict, PyList, PyString, PyTuple}, Bound, IntoPyObject, Py, PyAny, PyObject, PyResult, Python};

use smol_str::SmolStr;

use crate::index::py_dict_values::UnsafePyValues;
use crate::index::value::PyValue;
use crate::index::{stored_item::StoredItem, IndexAPI};

static GLOBAL_ID_COUNTER: AtomicU64 = AtomicU64::new(1);
static DEFAULT_INDEX_ARC: Lazy<Arc<IndexAPI>> = Lazy::new(|| Arc::new(IndexAPI::new()));

struct IndexMeta{
    index: Weak<IndexAPI>,
}

#[pyclass(subclass)]
pub struct Indexable{
    meta: SmallVec<[IndexMeta; 2]>,
    pub py_values: FxHashMap<SmolStr, PyValue>,
    pub id: u32
}


#[pymethods]
impl Indexable{

    #[new]
    #[pyo3(signature = (*_args, **kwargs))]
    #[inline(always)]
    fn new(
        _args: &Bound<'_, PyAny>, kwargs: Option<&Bound<'_, PyDict>>
    ) -> Self {

        let mut py_values: FxHashMap<SmolStr, PyValue>;

        if let Some(dict) = kwargs {
            let capacity = dict.len();
            py_values = FxHashMap::with_capacity_and_hasher(capacity, Default::default());
            for (key, value) in dict.iter() {
                if let Ok(key_str) = key.downcast::<PyString>() {
                    let key_string = SmolStr::new(key_str.to_str().unwrap_or(""));
                    py_values.insert(key_string, PyValue::new(value));
                }
            }
        } else {
            py_values = FxHashMap::new();
        }

        Self {
            meta: SmallVec::new(),
            id: GLOBAL_ID_COUNTER.fetch_add(1, Ordering::SeqCst) as u32,
            py_values
        }
    }

    #[inline(always)]
    fn __setattr__<'py>(&mut self, py: Python, name: &str, value: Bound<'py, PyAny>) -> PyResult<()> {

        let val: PyValue = PyValue::new(value);

        py.allow_threads(||{
            for ind in self.meta.iter() {
                if let Some(full_index) = ind.index.upgrade() {
                    let guard = full_index.index.write().unwrap();

                    if let Some(old_val) = self.py_values.get(name){
                        full_index.update_index(guard, SmolStr::new(name), Some(old_val), &val, self.id);
                    } else {
                        full_index.update_index(guard, SmolStr::new(name), None, &val, self.id);
                    }
                }
            }
        });

        // update value
        self.py_values.insert(SmolStr::new(name), val);

        Ok(())
    }

    #[inline(always)]
    fn __getattribute__(self_: Bound<'_, Self>, py: Python, name: Bound<'_, PyString>) -> PyResult<PyObject> {

        // aquire locks to prove nobody is writing

        let rust_self = self_.borrow();
        
        let arcs: SmallVec<[Arc<IndexAPI>;2]> = rust_self.meta
            .iter()
            .filter_map(|ind| ind.index.upgrade())
            .collect();

        let mut index_read_locks: SmallVec<[std::sync::RwLockReadGuard<'_, std::collections::HashMap<SmolStr, Box<super::query::QueryMap>, rustc_hash::FxBuildHasher>>; 2]> = SmallVec::new();
        
        for ind in arcs.iter() {
            let guard = ind.index.read().unwrap();
            index_read_locks.push(guard);
        }

        let name_str = match name.to_str() {
            Ok(s) => s,
            Err(_) => return Err(PyAttributeError::new_err("Invalid attribute name")),
        };
        
        if let Some(value) = rust_self.py_values.get(name_str) {
            Ok(value.get_obj().clone_ref(py))
        } else {

            let res = unsafe { ffi::PyObject_GenericGetAttr(self_.into_ptr(), name.into_ptr()) };
            if res.is_null() {
                Err(PyErr::fetch(py))
            } else {
                Ok(unsafe { Py::from_borrowed_ptr(py, res) })
            }
        }
    }

    fn __dir__(py_ref: PyRef<Self>, py: Python<'_>) -> PyResult<Py<PyList>> {
        let mut names: Vec<PyObject> = vec![];
        {
            for key in py_ref.py_values.keys() {
                names.push(PyString::new(py, key).into_py_any(py).unwrap());
            }
        }

        let py_self: Py<Self> = py_ref.into();
        let py_self_any: Py<PyAny> = py_self.into();

        let builtins = py.import("builtins")?;
        let object_type = builtins.getattr("object")?;
        let default_dir: Bound<'_, PyList> = object_type.call_method1("__dir__", (py_self_any,))?.extract()?;

        for d in default_dir.into_iter(){
            names.push(d.unbind());
        }

        Ok(PyList::new(py, names)?.into())
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("<MyClass with {} attributes>", self.py_values.len()))
    }
}

impl Indexable {

    pub fn trim_indexes(&mut self, remove: Arc<IndexAPI>){
        self.meta.retain(|m| {
            // Try to upgrade the Weak
            if let Some(arc) = m.index.upgrade() {
                if Arc::ptr_eq(&arc, &remove) {
                    return false
                } else {
                    !Arc::ptr_eq(&arc, &DEFAULT_INDEX_ARC)
                }
            } else {
                false
            }
        });
    }
    pub fn add_index(&mut self, index: Weak<IndexAPI>) {
        self.meta.push(IndexMeta {
            index: index,
        });

        self.trim_indexes(DEFAULT_INDEX_ARC.clone());
        self.meta.sort_by_key(|ind| Arc::as_ptr(&ind.index.upgrade().unwrap_or_else( || DEFAULT_INDEX_ARC.clone())) as usize);
    }

    pub fn remove_index(&mut self, index: Arc<IndexAPI>) {
        self.trim_indexes(index);
    }
}

impl Hash for Indexable {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self as *const _ as usize).hash(state);
    }
}

impl PartialEq for Indexable {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}

impl fmt::Debug for Indexable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexAPI")
            .field("id", &self.id)
            .field("attributes", &self.py_values)
            .finish()
    }
}