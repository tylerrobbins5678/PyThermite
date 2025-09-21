use once_cell::sync::OnceCell;
use pyo3::{types::PyType, Bound, Py, Python};

use crate::index::Indexable;


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
