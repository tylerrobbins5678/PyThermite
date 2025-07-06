use std::collections::HashMap;
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
    meta: Arc<Mutex<Vec<IndexMeta>>>,
    pub py_values: Arc<UnsafePyValues>,
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
            meta: Arc::new(Mutex::new(vec![])),
            id: GLOBAL_ID_COUNTER.fetch_add(1, Ordering::Relaxed) as u32,
            py_values: Arc::new(UnsafePyValues::new(HashMap::new()))
        }
    }

    fn __setattr__(&mut self, py: Python, name: String, value: Py<PyAny>) -> PyResult<()> {

        let none = py.None(); // owns the Py<PyAny> until end of function
        py.allow_threads(||{

            // Step 1: Collect and sort index references
            let mut index_refs = self.meta.lock().unwrap();
            index_refs.sort_by_key(|ind| Arc::as_ptr(&ind.index) as usize);

            // Step 2: Acquire write locks and pair with original Arc
            let mut index_write_locks = Vec::with_capacity(index_refs.len());
            for ind in index_refs.iter() {
                let arc_index = ind.index.clone();
                let guard = ind.index.index.write().unwrap();
                index_write_locks.push((arc_index, guard));
            }

            // get value - can be assumes safe since lock is needed to mutate
            let val_map = unsafe { self.py_values.get_mut() };
            let old_val = match val_map.get(&name){
                Some(ov) => ov,
                _ => &PyValue::new(&none)
            };

            // update all indexes
            for (ind, guard) in index_write_locks{
                ind.update_index(guard, name.clone(), old_val, &value, self.id)?;
            }
            // update value
            val_map.insert(name.clone(), PyValue::new(&value));

            // locks are all released at end of scope

            Ok(())
        })
    }

    fn __getattr__(&self, py: Python, name: &str) -> PyResult<Py<PyAny>> {

        let mut index_refs = self.meta.lock().unwrap();
        index_refs.sort_by_key(|ind| Arc::as_ptr(&ind.index) as usize);

        // aquire locks to prove nobody is writing
        let mut index_read_locks = Vec::with_capacity(index_refs.len());
        for ind in index_refs.iter() {
            let arc_index = ind.index.clone();
            let guard = ind.index.index.read().unwrap();
            index_read_locks.push((arc_index, guard));
        }

        let val_map = unsafe { self.py_values.map_ref() };
        match val_map.get(name) {
            Some(value) => Ok(value.get_obj().clone_ref(py)),
            None => Err(pyo3::exceptions::PyAttributeError::new_err(format!(
                "Attribute '{}' not found on RUST side",
                name
            ))),
        }
    }

    fn __dir__(py_ref: PyRef<Self>, py: Python<'_>) -> PyResult<Py<PyList>> {
        let mut names: Vec<PyObject> = vec![];
        {
            let readlock = unsafe { py_ref.py_values.map_ref() };
            for key in readlock.keys() {
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
        let readlock = unsafe { self.py_values.map_ref() };
        Ok(format!("<MyClass with {} attributes>", readlock.len()))
    }
}

impl Indexable {
    pub fn add_index(&self, index: Arc<Index>) {
        self.meta.lock().unwrap().push(IndexMeta {
            index: index,
        });
    }

    pub fn remove_index(&mut self, index: Arc<Index>) {
        self.meta.lock().unwrap().retain(|m| !Arc::ptr_eq(&m.index, &index));
    }
}

impl Clone for Indexable {
    fn clone(&self) -> Self {
        Indexable {
            meta: self.meta.clone(),
            id: self.id,
            py_values: self.py_values.clone()
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