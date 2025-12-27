use pyo3::{Bound, IntoPyObject, Py, PyAny, PyRef, Python};
use std::{hash::{Hash, Hasher}, sync::{Arc, Weak}};

use crate::index::{core::{index::IndexAPI, structures::hybrid_set::{HybridSet, HybridSetOps}}, types::{DEFAULT_INDEX_ARC, DEFAULT_INDEXABLE_ARC, DEFAULT_PY_INDEXABLE_ARC, DEFAULT_PY_NONE_ARC, StrId}, value::PyValue};
use crate::index::Indexable;

#[derive(Clone, Debug)]
pub struct StoredItemParent {
    pub ids: HybridSet,
    pub path_to_root: HybridSet,
    pub index: Weak<IndexAPI>,
}

#[derive(Clone, Debug)]
pub struct StoredItem{
    // these two are the same object, one is a rust handle and the other is a python handle
    py_item: Arc<Py<Indexable>>,
    owned_py_item: Arc<Indexable>,
    parent: Option<StoredItemParent>, // parent index id
}

impl<'py> StoredItem {
    pub fn new(
        py_handle: Arc<Py<Indexable>>,
        rust_handle: Arc<Indexable>,
        parent: Option<StoredItemParent>
    ) -> Self {
        Self {
            py_item: py_handle,
            owned_py_item: rust_handle,
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

    pub fn with_attr_id<F, R>(&self, str_id: StrId, f: F) -> Option<R>
    where
        F: FnOnce(&PyValue) -> R,
    {
        self.owned_py_item.with_attr_id(str_id, f)
    }

    pub fn get_owned_handle(&self) -> &Arc<Indexable> {
        &self.owned_py_item
    }

    pub fn get_py_ref(&self, py: Python) -> Py<Indexable> {
        self.py_item.clone_ref(py)
    }

    pub fn borrow_py_ref(&self, py: Python<'py>) -> PyRef<'py, Indexable> {
        self.py_item.bind(py).borrow()
    }
}

impl Default for StoredItem {
    fn default() -> Self {
        Self {
            py_item: DEFAULT_PY_INDEXABLE_ARC.clone(),
            owned_py_item: DEFAULT_INDEXABLE_ARC.clone(),
            parent: None,
        }
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