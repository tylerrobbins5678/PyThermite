use std::{cell::UnsafeCell, collections::{hash_map::{Iter, ValuesMut}, BTreeMap, HashMap, HashSet}, ops::{Bound, Range}, sync::{Arc, Weak}};

use croaring::{bitmap, Bitmap};
use ordered_float::OrderedFloat;
use pyo3::{pyclass, pymethods, types::{PyAnyMethods, PyString}, Py, PyAny, PyObject, PyResult, Python};

use crate::index::value::{PyValue, RustCastValue};

#[derive(Default)]
pub struct QueryMap {
    exact: HashMap<PyValue, Arc<UnsafeCell<Bitmap>>>,
    int_ordered: BTreeMap<i64, Arc<UnsafeCell<Bitmap>>>,
    float_ordered: BTreeMap<OrderedFloat<f64>, Arc<UnsafeCell<Bitmap>>>,
    str_ordered: BTreeMap<String, Arc<UnsafeCell<Bitmap>>>,
}

unsafe impl Send for QueryMap {}
unsafe impl Sync for QueryMap {}

impl QueryMap {
    pub fn new() -> Self{
        Self{
            exact: HashMap::new(),
            int_ordered: BTreeMap::new(),
            float_ordered: BTreeMap::new(),
            str_ordered: BTreeMap::new()
        }
    }

    pub fn insert(&mut self, value: PyValue, obj_id: u32){
        let val_entry =  self.exact.entry(value.clone())
            .or_insert_with(|| Arc::new(UnsafeCell::new(Bitmap::new())));

        unsafe { &mut *val_entry.get() }.add(obj_id);

        // Insert into the right ordered map based on primitive type
        match &value.get_primitive() {
            RustCastValue::Int(i) => {
                let entry = self.int_ordered.entry(*i)
                    .or_insert_with(|| val_entry.clone());
            }
            RustCastValue::Float(f) => {
                let entry = self.float_ordered.entry(OrderedFloat(*f))
                    .or_insert_with(|| val_entry.clone());
            }
            RustCastValue::Str(s) => {
                let entry = self.str_ordered.entry(s.clone())
                    .or_insert_with(|| val_entry.clone());
            }
            RustCastValue::Unknown => {
                // Optionally handle unknown types here or ignore
            }
        }
    }

    pub fn check_prune(&mut self, val: &PyValue) {
        if unsafe { &mut *self.exact[val].get() }.is_empty(){
            self.exact.remove(val);
        }
    }

    pub fn merge(&mut self, other: &Self) {
        for (val, bm) in &self.exact{
            if let Some(other) = other.get(&val){
                unsafe {
                    (&mut *bm.get()).or_inplace(other);
                }
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.exact.is_empty()
    }

    pub fn contains(&self, key: &PyValue) -> bool{
        self.exact.contains_key(key)
    }

    pub fn get(&self, key: &PyValue) -> Option<&Bitmap>{
        if let Some(cell) = self.exact.get(&key) {
            unsafe {
                Some(&*cell.get())
            }
        } else {
            None
        }
    }

    pub fn remove_id(&mut self, py_value: &PyValue, idx: u32) {
        if let Some(bm) = self.exact.get_mut(py_value) {
            unsafe { &mut *bm.get() } .remove(idx);
        }
    }

    pub fn remove_bitmap(&mut self, filter_bm: &Bitmap){
        for (_, bm) in self.exact.iter_mut() {
            unsafe { &mut *bm.get() }.and_inplace(filter_bm);
        }
    }

    pub fn iter(&self) -> QueryMapIter<'_> {
        QueryMapIter {
            exact_iter: self.exact.iter(),
        }
    }

}

impl QueryMap {

    fn get_range_number(
        &self,
        int_lower: Bound<i64>,
        int_upper: Bound<i64>,
        float_lower: Bound<OrderedFloat<f64>>,
        float_upper: Bound<OrderedFloat<f64>>,
    ) -> Bitmap {
        let mut result = Bitmap::new();

        for (_, bitmap) in self.int_ordered.range((int_lower, int_upper)) {
            result.or_inplace(unsafe { &*bitmap.get() });
        }

        for (_, bitmap) in self.float_ordered.range((float_lower, float_upper)) {
            result.or_inplace(unsafe { &*bitmap.get() });
        }

        result
    }

    pub fn gt(&self, val: &RustCastValue) -> Bitmap {
        // strictly greater than
        match val {
            RustCastValue::Int(i) => {
                self.get_range_number(
                    std::ops::Bound::Excluded(*i),
                    std::ops::Bound::Unbounded,
                    std::ops::Bound::Excluded(OrderedFloat(*i as f64)),
                    std::ops::Bound::Unbounded,
                )
            }
            RustCastValue::Float(f) => {
                self.get_range_number(
                    std::ops::Bound::Excluded(*f as i64),
                    std::ops::Bound::Unbounded,
                    std::ops::Bound::Excluded(OrderedFloat(*f)),
                    std::ops::Bound::Unbounded,
                )
            }
            RustCastValue::Str(f) => {
                let mut result = Bitmap::new();
                for (_, bitmap) in self.str_ordered
                    .range((std::ops::Bound::Excluded(f.clone()), std::ops::Bound::Unbounded)) {
                    result.or_inplace(unsafe { &*bitmap.get() });
                }
                result
            }
            RustCastValue::Unknown => {
                Bitmap::new()
            }
        }
    }

    pub fn ge(&self, val: &RustCastValue) -> Bitmap {
        // strictly greater than
        match val {
            RustCastValue::Int(i) => {
                self.get_range_number(
                    std::ops::Bound::Included(*i),
                    std::ops::Bound::Unbounded,
                    std::ops::Bound::Included(OrderedFloat(*i as f64)),
                    std::ops::Bound::Unbounded,
                )
            }
            RustCastValue::Float(f) => {
                let int_lower;
                if f.fract() == 0.0 {
                    int_lower = std::ops::Bound::Included(*f as i64);
                } else {
                    int_lower = std::ops::Bound::Excluded(*f as i64);
                }
                self.get_range_number(
                    int_lower,
                    std::ops::Bound::Unbounded,
                    std::ops::Bound::Included(OrderedFloat(*f)),
                    std::ops::Bound::Unbounded,
                )
            }
            RustCastValue::Str(f) => {
                let mut result = Bitmap::new();
                for (_, bitmap) in self.str_ordered
                    .range((std::ops::Bound::Included(f.clone()), std::ops::Bound::Unbounded)) {
                    result.or_inplace(unsafe { &*bitmap.get() });
                }
                result
            }
            RustCastValue::Unknown => {
                Bitmap::new()
            }
        }
    }

    pub fn lt(&self, val: &RustCastValue) -> Bitmap {
        match val {
            RustCastValue::Int(i) => {
                self.get_range_number(
                    std::ops::Bound::Unbounded,
                    std::ops::Bound::Excluded(*i),
                    std::ops::Bound::Unbounded,
                    std::ops::Bound::Excluded(OrderedFloat(*i as f64)),
                )
            }
            RustCastValue::Float(f) => {
                self.get_range_number(
                    std::ops::Bound::Unbounded,
                    std::ops::Bound::Excluded(*f as i64),
                    std::ops::Bound::Unbounded,
                    std::ops::Bound::Excluded(OrderedFloat(*f)),
                )
            }
            RustCastValue::Str(f) => {
                let mut result = Bitmap::new();
                for (_, bitmap) in self.str_ordered
                    .range((std::ops::Bound::Unbounded, std::ops::Bound::Excluded(f.clone()))) {
                    result.or_inplace(unsafe { &*bitmap.get() });
                }
                result
            }
            RustCastValue::Unknown => {
                Bitmap::new()
            }
        }
    }

    pub fn le(&self, val: &RustCastValue) -> Bitmap {
        // strictly greater than
        match val {
            RustCastValue::Int(i) => {
                self.get_range_number(
                    std::ops::Bound::Unbounded,
                    std::ops::Bound::Included(*i),
                    std::ops::Bound::Unbounded,
                    std::ops::Bound::Included(OrderedFloat(*i as f64)),
                )
            }
            RustCastValue::Float(f) => {
                let int_upper;
                if f.fract() == 0.0 {
                    int_upper = std::ops::Bound::Included(*f as i64);
                } else {
                    int_upper = std::ops::Bound::Excluded(*f as i64);
                }
                self.get_range_number(
                    std::ops::Bound::Unbounded,
                    int_upper,
                    std::ops::Bound::Unbounded,
                    std::ops::Bound::Included(OrderedFloat(*f)),
                )
            }
            RustCastValue::Str(f) => {
                let mut result = Bitmap::new();
                for (_, bitmap) in self.str_ordered
                    .range((std::ops::Bound::Unbounded, std::ops::Bound::Included(f.clone()))) {
                    result.or_inplace(unsafe { &*bitmap.get() });
                }
                result
            }
            RustCastValue::Unknown => {
                Bitmap::new()
            }
        }
    }

    pub fn eq(&self, val: &PyValue) -> Bitmap {
        if let Some(res) = self.exact.get(val){
            unsafe { (*res.get()).clone() }
        } else {
            Bitmap::new()
        }
    }

}

pub struct QueryMapIter<'a> {
    exact_iter: std::collections::hash_map::Iter<'a, PyValue, Arc<UnsafeCell<Bitmap>>>,
}

impl<'a> Iterator for QueryMapIter<'a> {
    type Item = (&'a PyValue, &'a Bitmap);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((k, v)) = self.exact_iter.next() {
            return Some((k, unsafe {&mut *v.get()}));
        }
        None
    }
}

pub fn filter_index_by_hashes(
    index: &HashMap<String, QueryMap>,
    query: &HashMap<String, HashSet<PyValue>>,
) -> Bitmap {
    let mut sets_iter: Bitmap = Bitmap::new();
    let mut first = true;
    let eq = QueryMap::new();

    let mut sorted_query: Vec<_> = query.iter().collect();
    sorted_query.sort_by_key(|(attr, hashes)| {
        index.get(*attr)
            .map(|attr_map| {
                hashes.iter()
                    .map(|h| attr_map.exact.get(h).map_or(0, |set| unsafe { &*set.get() }.cardinality()))
                    .sum::<u64>()
            })
            .unwrap_or(0)
    });
    
    let mut per_attr_match: Bitmap = Bitmap::new();

    for (attr, allowed_hashes) in sorted_query {
        per_attr_match.clear();

        let attr_map = index.get(attr).unwrap_or(&eq);
        
        for h in allowed_hashes {
            if let Some(matched) = attr_map.get(h) {
                per_attr_match |= matched;
            }
        }

        if !first && sets_iter.is_empty() {
            return Bitmap::new();
        }

        if first {
            sets_iter = per_attr_match.clone();
        } else {
            sets_iter &= &per_attr_match;
        }
        first = false;
    }

    sets_iter
}


#[derive(Clone, Debug)]
pub enum QueryExpr {
    Eq(String, PyValue),
    Ne(String, PyValue),
    Gt(String, PyValue),
    Ge(String, PyValue),
    Lt(String, PyValue),
    Le(String, PyValue),
    In(String, Vec<PyValue>),
    Not(Box<QueryExpr>),
    And(Vec<QueryExpr>),
    Or(Vec<QueryExpr>),
}

#[pyclass]
#[derive(Clone)]
pub struct PyQueryExpr {
    pub inner: QueryExpr,
}

#[pymethods]
impl PyQueryExpr {
    #[staticmethod]
    pub fn eq(attr: String, value: PyObject) -> Self {
        Self {
            inner: QueryExpr::Eq(attr, PyValue::new(&value)),
        }
    }

    #[staticmethod]
    pub fn ne(attr: String, value: PyObject) -> Self {
        Self {
            inner: QueryExpr::Ne(attr, PyValue::new(&value)),
        }
    }

    #[staticmethod]
    pub fn gt(attr: String, value: PyObject) -> Self {
        Self {
            inner: QueryExpr::Gt(attr, PyValue::new(&value)),
        }
    }

    #[staticmethod]
    pub fn ge(attr: String, value: PyObject) -> Self {
        Self {
            inner: QueryExpr::Ge(attr, PyValue::new(&value)),
        }
    }

    #[staticmethod]
    pub fn le(attr: String, value: PyObject) -> Self {
        Self {
            inner: QueryExpr::Le(attr, PyValue::new(&value)),
        }
    }

    #[staticmethod]
    pub fn lt(attr: String, value: PyObject) -> Self {
        Self {
            inner: QueryExpr::Lt(attr, PyValue::new(&value)),
        }
    }

    #[staticmethod]
    pub fn in_(attr: String, values: Vec<PyObject>) -> Self {
        let values = values.into_iter().map(|obj: pyo3::Py<pyo3::PyAny>| PyValue::new(&obj)).collect();
        Self {
            inner: QueryExpr::In(attr, values),
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

pub fn evaluate_query(
    index: &HashMap<String, QueryMap>,
    all_valid: Option<&Bitmap>,
    expr: &QueryExpr,
) -> Bitmap {
    match expr {
        QueryExpr::Eq(attr, value) => {
            if let Some(qm) = index.get(attr){
                qm.eq(value)
            } else {
                Bitmap::new()
            }
        }
        QueryExpr::Ne(attr, value ) => {
            evaluate_query(
                index,
                all_valid,
                &QueryExpr::Not(Box::new(QueryExpr::Eq(attr.clone(), value.clone())))
            )
        }
        QueryExpr::In(attr, values) => {
            let mut result = Bitmap::new();
            if let Some(qm) = index.get(attr) {
                for v in values {
                    if let Some(bm) = qm.get(v) {
                        result.or_inplace(bm);
                    }
                }
            }
            result
        }
        QueryExpr::Gt(attr, value) => {
            if let Some(qm) = index.get(attr) {
                qm.gt(value.get_primitive())
            } else {
                Bitmap::new()
            }
        }
        QueryExpr::Ge(attr, value) => {
            if let Some(qm) = index.get(attr) {
                qm.ge(value.get_primitive())
            } else {
                Bitmap::new()
            }
        }
        QueryExpr::Le(attr, value) => {
            if let Some(qm) = index.get(attr) {
                qm.le(value.get_primitive())
            } else {
                Bitmap::new()
            }
        }
        QueryExpr::Lt(attr, value) => {
            if let Some(qm) = index.get(attr) {
                qm.lt(value.get_primitive())
            } else {
                Bitmap::new()
            }
        }
        QueryExpr::Not(inner) => {
            let inner_bm = evaluate_query(index, all_valid, inner);
            match all_valid{
                Some(valid) => valid - &inner_bm,
                None => inner_bm,
            }
        }
        QueryExpr::And(exprs) => {
            let mut result = None;

            for expr in exprs {
                let bm = evaluate_query(index, result.as_ref().or(all_valid), expr);

                match &mut result {
                    Some(r) => {
                        r.and_inplace(&bm);
                        if r.is_empty() {
                            break; // early termination
                        }
                    }
                    None => result = Some(bm),
                }
            }

            result.unwrap_or_else(Bitmap::new)
        }
        QueryExpr::Or(exprs) => {
            let mut result = Bitmap::new();
            for e in exprs {
                result.or_inplace(&evaluate_query(index, all_valid, e));
            }
            result
        }
        _ => Bitmap::new(), // Ne/Ge/Le unimplemented in this stub
    }
}

pub fn kwargs_to_hash_query(
    py: Python,
    kwargs: &HashMap<String, Py<PyAny>>,
) -> PyResult<HashMap<String, HashSet<PyValue>>> {
    let mut query = HashMap::new();

    for (attr, py_val) in kwargs {
        let val_ref = py_val.clone_ref(py).into_bound(py);
        let mut hash_set = HashSet::new();

        // Detect if iterable but not string
        let is_str = val_ref.is_instance_of::<PyString>();

        if !is_str {
            match val_ref.try_iter() {
                Ok(iter) => {
                    for item in iter {
                        let lookup_item = PyValue::new(&item.unwrap().unbind());
                        hash_set.insert(lookup_item);
                    }
                }
                Err(_) => {
                    // Not iterable, treat as a single value
                    hash_set.insert(PyValue::new(py_val));
                }
            }
        } else {
            // Is a string, treat as a single value
            hash_set.insert(PyValue::new(py_val));
        }

        // Single value
        query.insert(attr.clone(), hash_set);
    }

    Ok(query)
}