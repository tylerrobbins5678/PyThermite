
use croaring::Bitmap;
use pyo3::{Py, PyResult, Python};

use crate::index::{Indexable, interfaces::filtered_index::FilteredIndex};

impl FilteredIndex{

    pub fn get_from_indexes(&self, py: Python, indexes: &Bitmap) -> PyResult<Vec<Py<Indexable>>>{
        let items = self.items.read().unwrap();
        let results: Vec<Py<Indexable>> = indexes.iter()
            .map(|arc| items.get(arc as usize).unwrap().get_py_ref(py))
            .collect();
        Ok(results)
    }

    pub fn filter_from_bitmap(&self, mut bm: Bitmap) -> FilteredIndex {
        bm.and_inplace(&self.allowed_items);
        FilteredIndex {
            index: self.index.clone(),
            items: self.items.clone(),
            allowed_items: bm
        }
    }

}