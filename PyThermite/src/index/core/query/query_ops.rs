
use std::{collections:: HashSet, ops::Bound};

use rustc_hash::FxHashMap;
use croaring::Bitmap;
use ordered_float::OrderedFloat;
use pyo3::{PyAny, PyResult, types::{PyAnyMethods, PyString}};
use smol_str::SmolStr;

use crate::index::{core::{query::QueryMap, structures::{composite_key::CompositeKey128, hybrid_set::HybridSetOps, string_interner::{INTERNER, StrInternerView}}}, interfaces::PyQueryExpr, value::{PyValue, RustCastValue}};

impl QueryMap {

    pub fn gt(&self, val: &RustCastValue, all_valid: &Bitmap) -> Bitmap {
        // strictly greater than
        let mut res = match val {
            RustCastValue::Int(i) => {
                let bits = CompositeKey128::encode_i64_to_float76(*i);
                self.read_num_ordered().get_gt_from_valid(bits, all_valid)
            }
            RustCastValue::Float(f) => {
                let bits = CompositeKey128::encode_f64_to_float76(OrderedFloat(*f));
                self.read_num_ordered().get_gt_from_valid(bits, all_valid)
            }
            _ => {
                Bitmap::new()
            }
        };
        self.unmask_ids(&mut res);
        res
    }

    pub fn ge(&self, val: &RustCastValue, all_valid: &Bitmap) -> Bitmap {
        // strictly greater than
        let mut res = match val {
            RustCastValue::Int(i) => {
                let bits = CompositeKey128::encode_i64_to_float76(*i);
                self.read_num_ordered().get_gte_from_valid(bits, all_valid)
            }
            RustCastValue::Float(f) => {
                let bits = CompositeKey128::encode_f64_to_float76(OrderedFloat(*f));
                self.read_num_ordered().get_gte_from_valid(bits, all_valid)
            }
            _ => {
                Bitmap::new()
            }
        };
        self.unmask_ids(&mut res);
        res
    }

    pub fn lt(&self, val: &RustCastValue, all_valid: &Bitmap) -> Bitmap {
        let mut res = match val {
            RustCastValue::Int(i) => {
                let bits = CompositeKey128::encode_i64_to_float76(*i);
                self.read_num_ordered().get_lt_from_valid(bits, all_valid)
            }
            RustCastValue::Float(f) => {
                let bits = CompositeKey128::encode_f64_to_float76(OrderedFloat(*f));
                self.read_num_ordered().get_lt_from_valid(bits, all_valid)
            }
            _ => {
                Bitmap::new()
            }
        };
        self.unmask_ids(&mut res);
        res
    }

    pub fn le(&self, val: &RustCastValue, all_valid: &Bitmap) -> Bitmap {
        // strictly greater than
        let mut res = match val {
            RustCastValue::Int(i) => {
                let bits = CompositeKey128::encode_i64_to_float76(*i);
                self.read_num_ordered().get_lte_from_valid(bits, all_valid)
            }
            RustCastValue::Float(f) => {
                let bits = CompositeKey128::encode_f64_to_float76(OrderedFloat(*f));
                self.read_num_ordered().get_lte_from_valid(bits, all_valid)
            }
            _ => {
                Bitmap::new()
            }
        };
        self.unmask_ids(&mut res);
        res
    }

    pub fn bt(&self, lower: &RustCastValue, upper: &RustCastValue, all_valid: &Bitmap) -> Bitmap {
        let low_range = match lower {
            RustCastValue::Int(i) => CompositeKey128::encode_i64_to_float76(*i),
            RustCastValue::Float(f) => CompositeKey128::encode_f64_to_float76(OrderedFloat(*f)),
            _ => todo!(),
        };

        let upper_range = match upper {
            RustCastValue::Int(i) => CompositeKey128::encode_i64_to_float76(*i),
            RustCastValue::Float(f) => CompositeKey128::encode_f64_to_float76(OrderedFloat(*f)),
            _ => todo!(),
        };

        let reader = self.read_num_ordered();
        let mut res = reader.get_bt_from_valid(low_range, upper_range, all_valid);
        self.unmask_ids(&mut res);
        res
    }

    pub fn eq(&self, val: &PyValue, all_valid: &Bitmap) -> Bitmap {

        let mut res = match val.get_primitive() {
            RustCastValue::Int(i) => {
                let bits = CompositeKey128::encode_i64_to_float76(*i);
                self.read_num_ordered().get_exact(bits)
            }
            RustCastValue::Float(f) => {
                let bits = CompositeKey128::encode_f64_to_float76(OrderedFloat(*f));
                self.read_num_ordered().get_exact(bits)
            }
            RustCastValue::Str(extracted_str) => {
                self.str_radix_map.read().unwrap().get_exact(extracted_str)
            }
            _ => {
                if let Some(res) = self.exact.get(val){
                    res.as_bitmap()
                } else {
                    Bitmap::new()
                }
            }
        };
        self.unmask_ids(&mut res);
        res
    }

    fn starts_with(&self, start: &RustCastValue, all_valid: &Bitmap) -> Bitmap {
        let mut res = match start {
            RustCastValue::Str(smol_str) => {
                let res = self.read_str_radix_map().starts_with(smol_str);
                res
            },
            _ => Bitmap::new(),
        };
        self.unmask_ids(&mut res);
        res
    }


    fn ends_with(&self, end: &RustCastValue, all_valid: &Bitmap) -> Bitmap {
       let mut res =  match end {
            RustCastValue::Str(smol_str) => {
                let res = self.read_str_radix_map().ends_with(smol_str);
                res
            },
            _ => Bitmap::new(),
        };
        self.unmask_ids(&mut res);
        res
    }

    fn contains(&self, inner: &RustCastValue, all_valid: &Bitmap) -> Bitmap {
        let mut res = match inner {
            RustCastValue::Str(smol_str) => {
                let res = self.read_str_radix_map().contains(smol_str);
                res
            },
            _ => Bitmap::new(),
        };
        self.unmask_ids(&mut res);
        res
    }

}

pub fn filter_index_by_hashes(
    index: &Vec<QueryMap>,
    query: &FxHashMap<SmolStr, HashSet<PyValue>>,
) -> Bitmap {
    let mut sets_iter: Bitmap = Bitmap::new();
    let mut first = true;
    
    let mut per_attr_match: Bitmap = Bitmap::new();
    let mut interner = StrInternerView::new(&INTERNER);

    for (attr, allowed_hashes) in query.iter() {
        let attr_id = interner.intern(attr) as usize;
        per_attr_match.clear();


        if let None = index.get(attr_id) {
            return Bitmap::new();
        } 
        let attr_map = &index[attr_id];
        
        for h in allowed_hashes {
            if let Some(matched) = attr_map.exact.get(h) {
                per_attr_match |= matched.as_bitmap();
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
    Not(Box<QueryExpr>),

    In(SmolStr, Vec<PyValue>),
    And(Vec<QueryExpr>),
    Or(Vec<QueryExpr>),
    // numeric ops
    Gt(SmolStr, PyValue),
    Ge(SmolStr, PyValue),
    Lt(SmolStr, PyValue),
    Le(SmolStr, PyValue),
    Bt(SmolStr, PyValue, PyValue),
    // string ops
    StartsWi(SmolStr, PyValue),
    EndsWi(SmolStr, PyValue),
    Contains(SmolStr, PyValue),
}

impl QueryExpr {
    pub fn estimated_cost(&self) -> u32 {
        match self {
            QueryExpr::Eq(_, _) => 0,
            QueryExpr::Ne(_, _) => 1,
            QueryExpr::Not(_) => 2,
            QueryExpr::In(_, _) => 3,
            QueryExpr::StartsWi(_, _) => 4,
            QueryExpr::EndsWi(_, _) => 5,
            QueryExpr::Contains(_, _) => 6,
            QueryExpr::And(_) => 7,
            QueryExpr::Or(_) => 8,
            QueryExpr::Lt(_, _) => 9,
            QueryExpr::Le(_, _) => 10,
            QueryExpr::Bt(_, _, _) => 11,
            QueryExpr::Gt(_, _) => 12,
            QueryExpr::Ge(_, _) => 13,
        }
    }
}

pub fn attr_parts(attr: SmolStr) -> (SmolStr, Option<SmolStr>) {
    if let Some(pos) = attr.find('.') {
        let (base, rest) = attr.split_at(pos);
        let rest = &rest[1..];
        (SmolStr::new(base), Some(SmolStr::new(rest)))
    } else {
        (attr, None)
    }
}

pub fn evaluate_nested_query(
    nested_map: &QueryMap,
    expr: &QueryExpr,
) -> Bitmap {
    let wrapper = PyQueryExpr{inner: expr.clone()};
    let reduced = nested_map.nested.reduced_query(wrapper);
    nested_map.get_allowed_parents(&reduced.allowed_items).as_bitmap()
}

pub fn evaluate_query(
    index: &Vec<QueryMap>,
    all_valid: &Bitmap,
    expr: &QueryExpr,
) -> Bitmap {
    match expr {
        QueryExpr::Eq(attr, value) => {
            let (base_attr, nested_attr) = attr_parts(attr.clone());
            let base_attr_id = INTERNER.intern(&base_attr) as usize;
            if let Some(qm) = index.get(base_attr_id){
                if let Some(nested_attr) = nested_attr {
                    let query = QueryExpr::Eq(nested_attr, value.clone());
                    evaluate_nested_query(qm, &query)
                } else {
                    qm.eq(value, all_valid)
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
            let mut result;
            let base_attr_id = INTERNER.intern(&base_attr) as usize;
            if let Some(qm) = index.get(base_attr_id) {
        
                if let Some(nested_attr) = nested_attr {
                    let query = QueryExpr::In(nested_attr, values.clone());
                    result = evaluate_nested_query(qm, &query);
                } else {
                    result = Bitmap::new();
                    for v in values {
                        let mut r = evaluate_query(
                            index,
                            all_valid,
                            &QueryExpr::Eq(attr.clone(), v.clone())
                        );
                        r.and_inplace(all_valid);
                        result.or_inplace(&r);
                    }
                }

            } else {
                result = Bitmap::new();
            }
            result
        }
        QueryExpr::Gt(attr, value) => {
            let (base_attr, nested_attr) = attr_parts(attr.clone());
            let base_attr_id = INTERNER.intern(&base_attr) as usize;
            if let Some(qm) = index.get(base_attr_id) {
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
            let base_attr_id = INTERNER.intern(&base_attr) as usize;
            if let Some(qm) = index.get(base_attr_id) {
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
            let base_attr_id = INTERNER.intern(&base_attr) as usize;
            if let Some(qm) = index.get(base_attr_id) {
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
            let base_attr_id = INTERNER.intern(&base_attr) as usize;
            if let Some(qm) = index.get(base_attr_id) {
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
            let base_attr_id = INTERNER.intern(&base_attr) as usize;
            if let Some(qm) = index.get(base_attr_id) {
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
            evaluate_and_queries_vec(index, all_valid, exprs)
            // Evaluate all queries in parallel
//            let mut bitmaps: Vec<Bitmap> = evaluate_queries_vec(index, all_valid, exprs);
//            bitmaps.sort_by_key(|bm| bm.cardinality());
//
//            // Reduce using AND in parallel
//            let result = bitmaps
//                .into_iter()
//                .reduce(|mut a, b| {
//                    a.and_inplace(&b); // mutate `a` in-place
//                    a
//                })
//                .unwrap_or_else(Bitmap::new); // handle empty exprs
//
//            result
        }
        QueryExpr::Or(exprs) => {
            evaluate_queries_vec(index, all_valid, exprs)
                .into_iter()
                .reduce(|mut a, b| {
                    a.or_inplace(&b); // mutate `a` in-place
                    a
                })
                .unwrap_or_else(Bitmap::new) // handle empty exprs
        }
        
        QueryExpr::StartsWi(attr, py_value) => {
            let (base_attr, nested_attr) = attr_parts(attr.clone());
            let base_attr_id = INTERNER.intern(&base_attr) as usize;

            if let Some(qm) = index.get(base_attr_id) {
                if let Some(nested_attr) = nested_attr {
                    let query = QueryExpr::StartsWi(nested_attr, py_value.clone());
                    evaluate_nested_query(qm, &query)
                } else {
                    qm.starts_with(py_value.get_primitive(), all_valid)
                }
            } else {
                Bitmap::new()
            }
        },
        QueryExpr::EndsWi(attr, py_value) => {
            let (base_attr, nested_attr) = attr_parts(attr.clone());
            let base_attr_id = INTERNER.intern(&base_attr) as usize;

            if let Some(qm) = index.get(base_attr_id) {
                if let Some(nested_attr) = nested_attr {
                    let query = QueryExpr::EndsWi(nested_attr, py_value.clone());
                    evaluate_nested_query(qm, &query)
                } else {
                    qm.ends_with(py_value.get_primitive(), all_valid)
                }
            } else {
                Bitmap::new()
            }
        },
        QueryExpr::Contains(attr, py_value) => {
            let (base_attr, nested_attr) = attr_parts(attr.clone());
            let base_attr_id = INTERNER.intern(&base_attr) as usize;

            if let Some(qm) = index.get(base_attr_id) {
                if let Some(nested_attr) = nested_attr {
                    let query = QueryExpr::Contains(nested_attr, py_value.clone());
                    evaluate_nested_query(qm, &query)
                } else {
                    qm.contains(py_value.get_primitive(), all_valid)
                }
            } else {
                Bitmap::new()
            }
        },
    }
}

pub fn evaluate_queries_vec(
    index: &Vec<QueryMap>,
    all_valid: &Bitmap,
    exprs: &Vec<QueryExpr>,
) -> Vec<Bitmap> {
    exprs
        .iter()
        .map(|expr| evaluate_query(index, &all_valid, expr))
        .collect()
}

pub fn evaluate_and_queries_vec(
    index: &Vec<QueryMap>,
    all_valid: &Bitmap,
    exprs: &Vec<QueryExpr>,
) -> Bitmap {
    let mut all_valid = all_valid.clone();

    let mut ordered: Vec<&QueryExpr> = exprs.iter().collect();
    ordered.sort_by_key(|expr| expr.estimated_cost());
    for o in ordered {
        all_valid.and_inplace(&evaluate_query(index, &all_valid, o));
    }
    all_valid
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