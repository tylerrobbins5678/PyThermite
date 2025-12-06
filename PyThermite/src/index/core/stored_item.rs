use pyo3::{Bound, IntoPyObject, Py, PyAny, PyRef, Python};
use std::{hash::{Hash, Hasher}, sync::{Arc, Weak}};

use crate::index::core::{structures::hybrid_set::{HybridSet, HybridSetOps}, index::IndexAPI};
use crate::index::Indexable;

#[derive(Clone, Debug)]
pub struct StoredItemParent {
    pub ids: HybridSet,
    pub path_to_root: HybridSet,
    pub index: Weak<IndexAPI>,
}

#[derive(Clone, Debug)]
pub struct StoredItem{
    py_item: Arc<Py<Indexable>>,
    parent: Option<StoredItemParent>, // parent index id
}

impl<'py> StoredItem {
    pub fn new(py_item: Arc<Py<Indexable>>, parent: Option<StoredItemParent>) -> Self {
        Self {
            py_item: py_item.clone(),
            parent: parent,
        }
    }

    pub fn is_orphaned(&self) -> bool {
        if let Some(ref p) = self.parent {
            p.ids.cardinality() == 0
        } else {
            false
        }
    }

    pub fn remove_parent(&mut self, parent_id: u32) {
        if let Some(ref mut p) = self.parent {
            p.ids.remove(parent_id)
        }
    }

    pub fn add_parent(&mut self, parent_id: u32) {
        if let Some(ref mut p) = self.parent {
            p.ids.add(parent_id)
        }
    }

    pub fn get_parent_ids(&self) -> &HybridSet {
        if let Some(parent) = &self.parent {
            &parent.ids
        } else {
            &HybridSet::Empty
        }
    }

    pub fn get_path_to_root(&self) -> HybridSet {
        let mut res = HybridSet::new();
        if let Some(parent) = &self.parent {
            if let Some(index) = parent.index.upgrade() {
                for id in parent.ids.iter() {
                    res.or_inplace(&index.get_ids_to_root(id));
                    res.add(id);
                }
                res
            } else {
                panic!("bad index upgrade");
            }
        } else {
            res
        }
    }

    pub fn get_py_ref(&self, py: Python) -> Py<Indexable> {
        self.py_item.clone_ref(py)
    }

    pub fn borrow_py_ref(&self, py: Python<'py>) -> PyRef<'py, Indexable> {
        self.py_item.bind(py).borrow()
    }
}

impl PartialEq for StoredItem {
    fn eq(&self, other: &Self) -> bool {
        self.py_item.as_ptr() == other.py_item.as_ptr()
    }
}

impl Eq for StoredItem {}

impl Hash for StoredItem {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Better:
        let py_id = self.py_item.as_ptr();
        py_id.hash(state);

    }
}

impl<'py> IntoPyObject<'py> for StoredItem {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = std::convert::Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(self.py_item.clone_ref(py).into_pyobject(py)?.into_any())
    }
}