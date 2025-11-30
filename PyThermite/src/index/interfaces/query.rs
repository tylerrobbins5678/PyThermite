use pyo3::{PyAny, pyclass, pymethods};
use smol_str::SmolStr;

use crate::index::{core::query::query_ops::QueryExpr, value::PyValue};


#[pyclass]
#[derive(Clone)]
pub struct PyQueryExpr {
    pub inner: QueryExpr,
}

#[pymethods]
impl PyQueryExpr {
    #[staticmethod]
    pub fn eq<'py>(attr: String, value: pyo3::Bound<'py, PyAny>) -> Self {
        Self {
            inner: QueryExpr::Eq(SmolStr::new(attr), PyValue::new(value)),
        }
    }

    #[staticmethod]
    pub fn ne<'py>(attr: String, value: pyo3::Bound<'py, PyAny>) -> Self {
        Self {
            inner: QueryExpr::Ne(SmolStr::new(attr), PyValue::new(value)),
        }
    }

    #[staticmethod]
    pub fn gt<'py>(attr: String, value: pyo3::Bound<'py, PyAny>) -> Self {
        Self {
            inner: QueryExpr::Gt(SmolStr::new(attr), PyValue::new(value)),
        }
    }

    #[staticmethod]
    pub fn ge<'py>(attr: String, value: pyo3::Bound<'py, PyAny>) -> Self {
        Self {
            inner: QueryExpr::Ge(SmolStr::new(attr), PyValue::new(value)),
        }
    }

    #[staticmethod]
    pub fn le<'py>(attr: String, value: pyo3::Bound<'py, PyAny>) -> Self {
        Self {
            inner: QueryExpr::Le(SmolStr::new(attr), PyValue::new(value)),
        }
    }

    #[staticmethod]
    pub fn bt<'py>(attr: String, lower: pyo3::Bound<'py, PyAny>, upper: pyo3::Bound<'py, PyAny>) -> Self {
        Self {
            inner: QueryExpr::Bt(SmolStr::new(attr), PyValue::new(lower), PyValue::new(upper)),
        }
    }

    #[staticmethod]
    pub fn lt<'py>(attr: String, value: pyo3::Bound<'py, PyAny>) -> Self {
        Self {
            inner: QueryExpr::Lt(SmolStr::new(attr), PyValue::new(value)),
        }
    }

    #[staticmethod]
    pub fn in_<'py>(attr: String, values: Vec<pyo3::Bound<'py, PyAny>>) -> Self {
        let values = values.into_iter().map(|obj| PyValue::new(obj)).collect();
        Self {
            inner: QueryExpr::In(SmolStr::new(attr), values),
        }
    }

    #[staticmethod]
    #[pyo3(signature = (*exprs))]
    fn and_(exprs: Vec<Self>) -> Self {
        Self {
            inner: QueryExpr::And(exprs.iter().map( | i | i.inner.clone()).collect()),
        }
    }

    #[staticmethod]
    #[pyo3(signature = (*exprs))]
    fn or_(exprs: Vec<Self>) -> Self {
        Self {
            inner: QueryExpr::Or(exprs.iter().map( | i | i.inner.clone()).collect()),
        }
    }

    #[staticmethod]
    fn not_(exprs: Self) -> Self {
        Self {
            inner: QueryExpr::Not(Box::new(exprs.inner)),
        }
    }

    fn __repr__(&self) -> String {
        format!("<QueryExpr: {:?}>", self.inner)
    }
}