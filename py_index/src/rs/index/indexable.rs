use rustc_hash::FxHashMap;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::hash::{Hash, Hasher};
use pyo3::{pyclass, pymethods, types::{PyAnyMethods, PyDict, PyList, PyString, PyTuple}, Bound, IntoPyObject, Py, PyAny, PyObject, PyRef, PyResult, Python};


use crate::index::py_dict_values::UnsafePyValues;
use crate::index::value::PyValue;
use crate::index::{stored_item::StoredItem, Index};

static GLOBAL_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
struct IndexMeta{
    index: Arc<Index>,
}

#[pyclass(subclass)]
pub struct Indexable{
    meta: Vec<IndexMeta>,
    pub py_values: FxHashMap<String, PyValue>,
    pub id: u32
}


#[pymethods]
impl Indexable{

    #[new]
    #[pyo3(signature = (*_args, **_kwargs))]
    fn new(
        _args: &Bound<'_, PyTuple>, _kwargs: Option<&Bound<'_, PyDict>>
    ) -> Self {
        Self {
            meta: vec![],
            id: GLOBAL_ID_COUNTER.fetch_add(1, Ordering::Relaxed) as u32,
            py_values: FxHashMap::default()
        }
    }

    fn __setattr__<'py>(&mut self, py: Python, name: String, value: Bound<'py, PyAny>) -> PyResult<()> {

        let val: PyValue = PyValue::new(value);

        if let Some(old_val) = self.py_values.get(&name){

            py.allow_threads(||{
                // Acquire write locks and pair with original Arc
                for ind in self.meta.iter() {
                    let arc_index = ind.index.clone();
                    let guard = ind.index.index.write().unwrap();
                    arc_index.update_index(guard, &name, old_val, &val, self.id);
                }
            });
        }

        // update value
        self.py_values.insert(name, val);

        Ok(())
    }

    fn __getattr__(&self, py: Python, name: String) -> PyResult<&Py<PyAny>> {

        // should alreday be sorted
        // self.meta.sort_by_key(|ind| Arc::as_ptr(&ind.index) as usize);

        // aquire locks to prove nobody is writing
        py.allow_threads(||{
            let mut index_read_locks = Vec::with_capacity(self.meta.len());
            for ind in self.meta.iter() {
                let arc_index = ind.index.clone();
                let guard = ind.index.index.read().unwrap();
                index_read_locks.push((arc_index, guard));
            }

            match self.py_values.get(&name) {
                Some(value) => Ok(value.get_obj()),
                None => Err(pyo3::exceptions::PyAttributeError::new_err(format!(
                    "Attribute '{}' not found on RUST side",
                    name
                ))),
            }
        })
    }

    fn __dir__(py_ref: PyRef<Self>, py: Python<'_>) -> PyResult<Py<PyList>> {
        let mut names: Vec<PyObject> = vec![];
        {
            for key in py_ref.py_values.keys() {
                names.push(PyString::new(py, key).into_pyobject(py)?.unbind().into_any());
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
    pub fn add_index(&mut self, index: Arc<Index>) {
        self.meta.push(IndexMeta {
            index: index,
        });
        self.meta.sort_by_key(|ind| Arc::as_ptr(&ind.index) as usize);
    }

    pub fn remove_index(&mut self, index: Arc<Index>) {
        self.meta.retain(|m| !Arc::ptr_eq(&m.index, &index));
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