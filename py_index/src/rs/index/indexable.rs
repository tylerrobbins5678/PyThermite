use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::hash::{Hash, Hasher};
use pyo3::{pyclass, pymethods, types::{PyAnyMethods, PyDict, PyList, PyString, PyTuple}, Bound, IntoPyObject, Py, PyAny, PyObject, PyRef, PyResult, Python};


use crate::index::py_dict_values::UnsafePyValues;
use crate::index::value::PyValue;
use crate::index::{stored_item::StoredItem, Index};

#[derive(Clone)]
struct IndexMeta{
    index: Arc<Index>,
    stored_item: Weak<StoredItem>
}

#[pyclass(subclass)]
pub struct Indexable{
    meta: Arc<Mutex<Vec<IndexMeta>>>,
    pub py_values: Arc<UnsafePyValues>
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
            py_values: Arc::new(UnsafePyValues::new(HashMap::new()))
        }
    }

    fn __setattr__(&mut self, py: Python, name: String, value: Py<PyAny>) -> PyResult<()> {

        let none = py.None(); // owns the Py<PyAny> until end of function
        
        let result: PyResult<()> = py.allow_threads(||{
            let readlock = unsafe { self.py_values.map_ref() };
            let old_val = match readlock.get(&name){
                Some(ov) => ov,
                _ => &PyValue::new(&none).unwrap()
            };

            { 
                for ind in self.meta.lock().unwrap().iter() {
                    if let Some(stored_item) = ind.stored_item.upgrade() {
                        ind.index.update_index(name.clone(), old_val, &value, stored_item)?;
                    }
                }
            }
            Ok(())
        });

        let write_lock = unsafe { self.py_values.get_mut() };
        write_lock.insert(name.clone(), PyValue::new(&value)?);

        result
    }

    fn __getattr__(&self, py: Python, name: &str) -> PyResult<Py<PyAny>> {
        let readlock = unsafe { self.py_values.map_ref() };
        match readlock.get(name) {
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
    pub fn add_index(&mut self, index: Arc<Index>, stored_item: Arc<StoredItem>) {
        self.meta.lock().unwrap().push(IndexMeta { 
            index: index,
            stored_item: Arc::downgrade(&stored_item)
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