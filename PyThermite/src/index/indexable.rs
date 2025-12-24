use arc_swap::Guard;
use pyo3::exceptions::PyAttributeError;
use pyo3::types::PyDictMethods;
use pyo3::types::PyStringMethods;
use pyo3::{ffi, IntoPyObjectExt, PyErr, PyRef};

use smallvec::SmallVec;
use once_cell::sync::Lazy;
use arc_swap::ArcSwap;

use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::MutexGuard;
use std::sync::{Arc, Mutex, Weak};
use std::hash::{Hash, Hasher};
use pyo3::{pyclass, pymethods, types::{PyAnyMethods, PyDict, PyList, PyString}, Bound, Py, PyAny, PyObject, PyResult, Python};

use smol_str::SmolStr;

use crate::index::core::structures::string_interner::INTERNER;
use crate::index::core::structures::string_interner::StrInternerView;
use crate::index::types::DEFAULT_INDEX_ARC;
use crate::index::types::StrId;
use crate::index::value::PyValue;
use crate::index::HybridHashmap;
use crate::index::core::index::IndexAPI;

static GLOBAL_ID_COUNTER: AtomicU32 = AtomicU32::new(1);

static FREE_IDS: Lazy<Mutex<Vec<u32>>> = Lazy::new(|| Mutex::new(Vec::new()));


pub fn allocate_id() -> u32 {
    let mut free = FREE_IDS.lock().unwrap();

    if let Some(id) = free.pop() {
        id
    } else {
        GLOBAL_ID_COUNTER.fetch_add(1, Ordering::SeqCst)
    }
}

pub fn free_id(id: u32) {
    let mut free = FREE_IDS.lock().unwrap();
    free.push(id);
}


struct IndexMeta{
    index: Weak<IndexAPI>,
}

#[pyclass(subclass, freelist = 512)]
pub struct Indexable{
    meta: Arc<Mutex<SmallVec<[IndexMeta; 4]>>>,
    pub py_values: Arc<Mutex<HybridHashmap<StrId, PyValue>>>,
    pub id: u32,
    pub recycle_id_on_drop: bool
}


#[pymethods]
impl Indexable{

    #[new]
    #[pyo3(signature = (*_args, **kwargs))]
    fn new(
        _args: &Bound<'_, PyAny>, kwargs: Option<&Bound<'_, PyDict>>
    ) -> Self {

        let mut py_values: HybridHashmap<StrId, PyValue>;
        let mut interner = StrInternerView::new(&INTERNER);

        if let Some(dict) = kwargs {
            py_values = HybridHashmap::Small(SmallVec::new());
            for (key, value) in dict.iter() {
                if let Ok(key_str) = key.extract::<&str>() {
                    let key_id: StrId = interner.intern(key_str);
                    py_values.insert(key_id, PyValue::new(value));
                }
            }
        } else {
            py_values = HybridHashmap::Small(SmallVec::new());
        }

        Self {
            meta: Arc::new(Mutex::new(SmallVec::new())),
            id: allocate_id(),
            py_values: Arc::new(Mutex::new(py_values)),
            recycle_id_on_drop: true
        }
    }

    fn __setattr__<'py>(&self, py: Python, name: &str, value: Bound<'py, PyAny>) -> PyResult<()> {

        let val: PyValue = PyValue::new(value);

        py.allow_threads(||{
            let mut interner = StrInternerView::new(&INTERNER);
            for ind in self.meta.lock().unwrap().iter() {
                if let Some(full_index) = ind.index.upgrade() {
                    let name_id = interner.intern(name);
                    if let Some(old_val) = self.get_py_values().get(&name_id){
                        full_index.update_index(ind.index.clone(), name_id, Some(old_val), &val, self.id);
                    } else {
                        full_index.update_index(ind.index.clone(), name_id, None, &val, self.id);
                    }
                }
            }
        });

        // update value
        let str_id: StrId = INTERNER.intern(name);
        self.py_values.lock().unwrap().insert(str_id, val);
        Ok(())
    }

    fn __getattribute__(self_: PyRef<'_, Self>, py: Python, name: Bound<'_, PyString>) -> PyResult<PyObject> {

        let name_str = match name.to_str() {
            Ok(s) => s,
            Err(_) => return Err(PyAttributeError::new_err("Invalid attribute name")),
        };
        let py_values = self_.get_py_values();

        if let Some(value) = py_values.get(&INTERNER.intern(name_str)) {
            Ok(value.get_obj(py))
        } else {
            drop(py_values);
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
            let interner = StrInternerView::new(&INTERNER);
            for key_id in py_ref.get_py_values().keys() {
                names.push(PyString::new(py, interner.resolve(*key_id)).into_py_any(py).unwrap());
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
        Ok(format!("<Indexable with {} attributes>", self.get_py_values().len()))
    }
}

impl Indexable {

    pub fn from_py_ref(reference: &PyRef<Indexable>, _py: Python) -> Self {
        // `reference` is a GIL-bound borrow; we clone the Arc pointers for Rust ownership
        Self {
            meta: reference.meta.clone(),
            py_values: reference.py_values.clone(),
            id: reference.id,
            recycle_id_on_drop: false // ID authority is the Python handle
        }
    }

    fn trim_indexes(meta_lock: &mut MutexGuard<'_, SmallVec<[IndexMeta; 4]>>, remove: Arc<IndexAPI>){
        meta_lock.retain(|m| {
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

    #[inline(always)]
    pub fn add_index(&self, index: Weak<IndexAPI>) {
        let mut meta_lock: MutexGuard<'_, SmallVec<[IndexMeta; 4]>> = self.meta.lock().unwrap();
        meta_lock.push(IndexMeta {
            index: index,
        });

        Self::trim_indexes(&mut meta_lock, DEFAULT_INDEX_ARC.clone());
        meta_lock.sort_by_key(|ind| Arc::as_ptr(&ind.index.upgrade().unwrap_or_else( || DEFAULT_INDEX_ARC.clone())) as usize);
    }

    pub fn remove_index(&self, index: Arc<IndexAPI>) {
        let mut meta_lock: MutexGuard<'_, SmallVec<[IndexMeta; 4]>> = self.meta.lock().unwrap();
        Self::trim_indexes(&mut meta_lock, index);
    }

    pub fn get_py_values(&self) -> MutexGuard<'_, HybridHashmap<StrId, PyValue>>{
        self.py_values.lock().unwrap()
    }

    pub fn with_attr_id<F, R>(&self, str_id: StrId, f: F) -> Option<R>
    where
        F: FnOnce(&PyValue) -> R
    {
        let guard = self.get_py_values();
        guard.get(&str_id).map(f)
    }
}

impl Drop for Indexable {
    fn drop(&mut self) {
        if self.recycle_id_on_drop {
            free_id(self.id);
        }
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

impl Default for Indexable {
    fn default() -> Self {
        Self {
            meta: Arc::new(Mutex::new(SmallVec::new())),
            id: allocate_id(),
            py_values: Arc::new(Mutex::new(HybridHashmap::Small(SmallVec::new()))),
            recycle_id_on_drop: true
        }
    }
}