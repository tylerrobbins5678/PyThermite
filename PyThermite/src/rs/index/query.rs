use std::{cell::UnsafeCell, collections::{hash_map::{Iter, ValuesMut}, BTreeMap, HashSet}, ops::{Bound, Deref, Range}, sync::{Arc, Weak}};

use rand::seq::index;
use rustc_hash::FxHashMap;
use croaring::Bitmap;
use ordered_float::OrderedFloat;
use pyo3::{pyclass, pymethods, types::{PyAnyMethods, PyString}, Py, PyAny, PyObject, PyResult, Python};
use smol_str::SmolStr;

use crate::index::{stored_item::{StoredItem, StoredItemParent}, value::{PyValue, RustCastValue}, BitMapBTree, HybridSet, Index, IndexAPI, Key};

#[derive(Default)]
pub struct QueryMap {
    exact: FxHashMap<PyValue, HybridSet>,
    parent: Weak<IndexAPI>,
    num_ordered: BitMapBTree,
    nested: Arc<IndexAPI>,
}

unsafe impl Send for QueryMap {}
unsafe impl Sync for QueryMap {}

impl QueryMap {
    pub fn new(parent: Weak<IndexAPI>) -> Self{
        Self{
            exact: FxHashMap::default(),
            parent: parent.clone(),
            num_ordered: BitMapBTree::new(),
            nested: Arc::new(IndexAPI::new(Some(parent))),
        }
    }

    pub fn insert(&mut self, value: &PyValue, obj_id: u32){

        if let Some(existing) = self.exact.get_mut(&value) {
            existing.add(obj_id);
        } else {
            // lazily create only if needed
            let hybrid_set = HybridSet::of(&[obj_id]);
            self.exact.insert(value.clone(), hybrid_set);
        }

        // Insert into the right ordered map based on primitive type
        match &value.get_primitive() {
            RustCastValue::Int(i) => {
                self.num_ordered.insert(Key::Int(*i), obj_id);
            }
            RustCastValue::Float(f) => {
                self.num_ordered.insert(Key::FloatOrdered(OrderedFloat(*f)), obj_id);
            }
            RustCastValue::Ind(index_obj) => {

                let mut path = HybridSet::new();

                if let Some(parent) = self.parent.upgrade() {
                    path = parent.get_parents_from_stored_item(obj_id as usize);
                }

                let res = Python::with_gil(|py| {
                    let index_obj_ref = index_obj.try_borrow(py).expect("cannot borrow, owned by other object");
                    let id: u32 = index_obj_ref.id;

                    if path.contains(id){
                        return None;
                    }
                    let py_values = index_obj_ref.get_py_values().clone();

                    // register the index in the object
                    let weak_nested = Arc::downgrade(&self.nested);
                    index_obj_ref.add_index(weak_nested.clone());
                    Some((id, py_values, weak_nested))
                });

                if let Some((id, py_values, weak_nested)) = res {
                    let stored_parent = StoredItemParent {
                        id: obj_id,
                        path_to_root: path,
                        index: weak_nested.clone(),
                    };
    
                    let stored_item = StoredItem::new(index_obj.clone(), Some(stored_parent));
                    self.nested.add_object(weak_nested, id, stored_item, py_values);
                }

            },
            RustCastValue::Unknown | RustCastValue::Str(_) => {
            }
        }
    }

    pub fn check_prune(&mut self, val: &PyValue) {
        if self.exact[val].is_empty(){
            self.exact.remove(val);
        }
    }

    pub fn merge(&mut self, other: &Self) {
        for (val, bm) in self.exact.iter_mut() {
            if let Some(other) = other.get(&val){
                bm.or_inplace(other);
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.exact.is_empty()
    }

    pub fn contains(&self, key: &PyValue) -> bool{
        self.exact.contains_key(key)
    }

    pub fn get(&self, key: &PyValue) -> Option<&HybridSet>{
        self.exact.get(&key)
    }

    pub fn get_mut(&mut self, key: &PyValue) -> Option<&mut HybridSet> {
        self.exact.get_mut(&key)
    }

    pub fn remove_id(&mut self, py_value: &PyValue, idx: u32) {
        if let Some(hybrid_set) = self.exact.get_mut(py_value) {
            hybrid_set.remove(idx);
        }
        match &py_value.get_primitive(){
            RustCastValue::Int(i) => {
                self.num_ordered.remove(Key::Int(*i), idx);
            }
            RustCastValue::Float(f) => {
                self.num_ordered.remove(Key::FloatOrdered(OrderedFloat(*f)), idx);
            }
            RustCastValue::Str(_) => return,
            RustCastValue::Ind(indexable) => {
                Python::with_gil(| py | {

                    let mut path = HybridSet::new();
                    if let Some(parent) = self.parent.upgrade() {
                        path = parent.get_parents_from_stored_item(idx as usize);
                    }

                    let to_insert = indexable.borrow(py);
                    if path.contains(to_insert.id){
                        return;
                    }

                    self.nested.remove(to_insert.deref());
                });
            },
            RustCastValue::Unknown => return,
        };
    }

    pub fn remove(&mut self, filter_bm: &HybridSet){
        for (_, bm) in self.exact.iter_mut() {
            bm.and_inplace(filter_bm);
        }
    }

    pub fn iter(&self) -> QueryMapIter<'_> {
        QueryMapIter {
            exact_iter: self.exact.iter(),
        }
    }

    pub fn get_allowed_parents(&self, child_bm: &Bitmap) -> HybridSet {
        self.nested.get_direct_parents(child_bm)
    }

}

impl QueryMap {

    pub fn gt(&self, val: &RustCastValue, all_valid: &Bitmap) -> Bitmap {
        // strictly greater than
        match val {
            RustCastValue::Int(i) => {
                self.num_ordered.range_query(
                    Bound::Excluded(&Key::Int(*i)),
                    Bound::Unbounded,
                    all_valid
                )
            }
            RustCastValue::Float(f) => {
                self.num_ordered.range_query(
                    Bound::Excluded(&Key::FloatOrdered(OrderedFloat(*f))),
                    Bound::Unbounded,
                    all_valid
                )
            }
            RustCastValue::Str(f) => {
                let mut result = Bitmap::new();
                result
            }
            RustCastValue::Ind(index_api) => todo!(),
            RustCastValue::Unknown => {
                Bitmap::new()
            }
        }
    }

    pub fn ge(&self, val: &RustCastValue, all_valid: &Bitmap) -> Bitmap {
        // strictly greater than
        match val {
            RustCastValue::Int(i) => {
                self.num_ordered.range_query(
                    Bound::Included(&Key::Int(*i)),
                    Bound::Unbounded,
                    all_valid
                )
            }
            RustCastValue::Float(f) => {
                self.num_ordered.range_query(
                    Bound::Included(&Key::FloatOrdered(OrderedFloat(*f))),
                    Bound::Unbounded,
                    all_valid
                )
            }
            RustCastValue::Str(f) => {
                let mut result = Bitmap::new();
                result
            }
            RustCastValue::Ind(index_api) => todo!(),
            RustCastValue::Unknown => {
                Bitmap::new()
            }
        }
    }

    pub fn lt(&self, val: &RustCastValue, all_valid: &Bitmap) -> Bitmap {
        match val {
            RustCastValue::Int(i) => {
                self.num_ordered.range_query(
                    Bound::Unbounded,
                    Bound::Excluded(&Key::Int(*i)),
                    all_valid
                )
            }
            RustCastValue::Float(f) => {
                self.num_ordered.range_query(
                    Bound::Unbounded,
                    Bound::Excluded(&Key::FloatOrdered(OrderedFloat(*f))),
                    all_valid
                )
            }
            RustCastValue::Str(f) => {
                let mut result = Bitmap::new();
                result
            }
            RustCastValue::Ind(index_api) => todo!(),
            RustCastValue::Unknown => {
                Bitmap::new()
            }
        }
    }

    pub fn le(&self, val: &RustCastValue, all_valid: &Bitmap) -> Bitmap {
        // strictly greater than
        match val {
            RustCastValue::Int(i) => {
                self.num_ordered.range_query(
                    Bound::Unbounded,
                    Bound::Included(&Key::Int(*i)),
                    all_valid
                )
            }
            RustCastValue::Float(f) => {
                self.num_ordered.range_query(
                    Bound::Unbounded,
                    Bound::Included(&Key::FloatOrdered(OrderedFloat(*f))),
                    all_valid
                )
            }
            RustCastValue::Str(f) => {
                let mut result = Bitmap::new();
                result
            }
            RustCastValue::Ind(index_api) => todo!(),
            RustCastValue::Unknown => {
                Bitmap::new()
            }
        }
    }

    pub fn bt(&self, lower: &RustCastValue, upper: &RustCastValue, all_valid: &Bitmap) -> Bitmap {
        let low_range = match lower {
            RustCastValue::Int(i) => Key::Int(*i),
            RustCastValue::Float(f) => Key::FloatOrdered(OrderedFloat(*f)),
            RustCastValue::Str(s) => todo!(),
            RustCastValue::Ind(index_api) => todo!(),
            RustCastValue::Unknown => todo!(),
        };

        let upper_range = match upper {
            RustCastValue::Int(i) => Key::Int(*i),
            RustCastValue::Float(f) => Key::FloatOrdered(OrderedFloat(*f)),
            RustCastValue::Str(s) => todo!(),
            RustCastValue::Ind(index_api) => todo!(),
            RustCastValue::Unknown => todo!(),
        };

        self.num_ordered.range_query(
            Bound::Included(&low_range),
            Bound::Included(&upper_range),
            all_valid
        )
    }

    pub fn eq(&self, val: &PyValue) -> Bitmap {
        if let Some(res) = self.exact.get(val){
            res.clone().as_bitmap()
        } else {
            Bitmap::new()
        }
    }

}

pub struct QueryMapIter<'a> {
    exact_iter: std::collections::hash_map::Iter<'a, PyValue, HybridSet>,
}

impl<'a> Iterator for QueryMapIter<'a> {
    type Item = (&'a PyValue, &'a HybridSet);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((k, v)) = self.exact_iter.next() {
            return Some((k, v));
        }
        None
    }
}

pub fn filter_index_by_hashes(
    index: &FxHashMap<SmolStr, Box<QueryMap>>,
    query: &FxHashMap<SmolStr, HashSet<PyValue>>,
) -> Bitmap {
    let mut sets_iter: Bitmap = Bitmap::new();
    let mut first = true;

    let mut sorted_query: Vec<_> = query.iter().collect();
    sorted_query.sort_by_key(|(attr, hashes)| {
        index.get(*attr)
            .map(|attr_map| {
                hashes.iter()
                    .map(|h| attr_map.exact.get(h).map_or(0, |set| set.cardinality()))
                    .sum::<u64>()
            })
            .unwrap_or(0)
    });
    
    let mut per_attr_match: Bitmap = Bitmap::new();

    for (attr, allowed_hashes) in sorted_query {
        per_attr_match.clear();


        if let None = index.get(attr) {
            return Bitmap::new();
        } 
        let attr_map = &index[attr];
        
        for h in allowed_hashes {
            if let Some(matched) = attr_map.get(h) {
                per_attr_match |= matched.clone().as_bitmap();
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
    Eq(SmolStr, PyValue),
    Ne(SmolStr, PyValue),
    Gt(SmolStr, PyValue),
    Ge(SmolStr, PyValue),
    Lt(SmolStr, PyValue),
    Le(SmolStr, PyValue),
    Bt(SmolStr, PyValue, PyValue),
    In(SmolStr, Vec<PyValue>),
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

fn attr_parts(attr: SmolStr) -> (SmolStr, Option<SmolStr>) {
    if let Some(pos) = attr.find('.') {
        let (base, rest) = attr.split_at(pos);
        let rest = &rest[1..];
        (SmolStr::new(base), Some(SmolStr::new(rest)))
    } else {
        (attr, None)
    }
}

pub fn evaluate_nested_query(
    nested_map: &Box<QueryMap>,
    expr: &QueryExpr,
) -> Bitmap {
    let wrapper = PyQueryExpr{inner: expr.clone()};
    let reduced = nested_map.nested.reduced_query(wrapper);
    nested_map.get_allowed_parents(&reduced.allowed_items).as_bitmap()
}

pub fn evaluate_query(
    index: &FxHashMap<SmolStr, Box<QueryMap>>,
    all_valid: &Bitmap,
    expr: &QueryExpr,
) -> Bitmap {
    match expr {
        QueryExpr::Eq(attr, value) => {
            let (base_attr, nested_attr) = attr_parts(attr.clone());
            if let Some(qm) = index.get(&base_attr){
                if let Some(nested_attr) = nested_attr {
                    let query = QueryExpr::Eq(nested_attr, value.clone());
                    evaluate_nested_query(qm, &query)
                } else {
                    qm.eq(value)
                }
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
            let (base_attr, nested_attr) = attr_parts(attr.clone());
            let mut result = Bitmap::new();
            if let Some(qm) = index.get(&base_attr) {

                if let Some(nested_attr) = nested_attr {
                    let query = QueryExpr::In(nested_attr, values.clone());
                    result = evaluate_nested_query(qm, &query);
                } else {
                    for v in values {
                        if let Some(bm) = qm.get(v) {
                            result.or_inplace(&bm.clone().as_bitmap());
                            result.and_inplace(all_valid);
                        }
                    }
                }

            }
            result
        }
        QueryExpr::Gt(attr, value) => {
            let (base_attr, nested_attr) = attr_parts(attr.clone());
            if let Some(qm) = index.get(&base_attr) {
                if let Some(nested_attr) = nested_attr {
                    let query = QueryExpr::Gt(nested_attr, value.clone());
                    evaluate_nested_query(qm, &query)
                } else {
                    qm.gt(value.get_primitive(), all_valid)
                }
            } else {
                Bitmap::new()
            }
        }
        QueryExpr::Ge(attr, value) => {
            let (base_attr, nested_attr) = attr_parts(attr.clone());
            if let Some(qm) = index.get(&base_attr) {
                if let Some(nested_attr) = nested_attr {
                    let query = QueryExpr::Ge(nested_attr, value.clone());
                    evaluate_nested_query(qm, &query)
                } else {
                    qm.ge(value.get_primitive(), all_valid)
                }
            } else {
                Bitmap::new()
            }
        }
        QueryExpr::Le(attr, value) => {
            let (base_attr, nested_attr) = attr_parts(attr.clone());
            if let Some(qm) = index.get(&base_attr) {
                if let Some(nested_attr) = nested_attr {
                    let query = QueryExpr::Le(nested_attr, value.clone());
                    evaluate_nested_query(qm, &query)
                } else {
                    qm.le(value.get_primitive(), all_valid)
                }
            } else {
                Bitmap::new()
            }
        }
        QueryExpr::Lt(attr, value) => {
            let (base_attr, nested_attr) = attr_parts(attr.clone());
            if let Some(qm) = index.get(&base_attr) {
                if let Some(nested_attr) = nested_attr {
                    let query = QueryExpr::Lt(nested_attr, value.clone());
                    evaluate_nested_query(qm, &query)
                } else {
                    qm.lt(value.get_primitive(), all_valid)
                }
            } else {
                Bitmap::new()
            }
        }
        QueryExpr::Bt(attr, lower, upper) => {
            let (base_attr, nested_attr) = attr_parts(attr.clone());
            if let Some(qm) = index.get(&base_attr) {
                if let Some(nested_attr) = nested_attr {
                    let query = QueryExpr::Bt(nested_attr, lower.clone(), upper.clone());
                    evaluate_nested_query(qm, &query)
                } else {
                    qm.bt(lower.get_primitive(), upper.get_primitive(), all_valid)
                }
            } else {
                Bitmap::new()
            }
        }
        QueryExpr::Not(inner) => {
            let inner_bm = evaluate_query(index, all_valid, inner);
                all_valid - &inner_bm
        }
        QueryExpr::And(exprs) => {
            let mut result = all_valid.clone();

            for expr in exprs {
                let bm = evaluate_query(index, &result, expr);
                result.and_inplace(&bm);
                if result.is_empty() {
                    break; // early termination
                }
            }
            result
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

pub fn kwargs_to_hash_query<'py>(
    kwargs: FxHashMap<String, pyo3::Bound<'py, PyAny>>,
) -> PyResult<FxHashMap<SmolStr, HashSet<PyValue>>> {
    let mut query = FxHashMap::default();

    for (attr, py_val) in kwargs {
        let mut hash_set = HashSet::new();

        // Detect if iterable but not string
        let is_str = py_val.is_instance_of::<PyString>();

        if !is_str {
            match py_val.try_iter() {
                Ok(iter) => {
                    for item in iter {
                        let lookup_item = PyValue::new(item.unwrap());
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
        query.insert(SmolStr::new(attr), hash_set);
    }

    Ok(query)
}