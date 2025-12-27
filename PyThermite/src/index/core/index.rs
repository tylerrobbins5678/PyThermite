
use std::{fmt, iter::Enumerate, ops::Deref, sync::{Arc, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak}, vec};
use croaring::Bitmap;
use pyo3::prelude::*;
use rustc_hash::FxHashMap;
use smallvec::SmallVec;
use smol_str::SmolStr;

use crate::index::{HybridHashmap, Indexable, PyQueryExpr, core::structures::{hybrid_set::{HybridSet, HybridSetOps}, string_interner::{INTERNER, StrInternerView}}, interfaces::filtered_index::FilteredIndex, types::{DEFAULT_INDEXABLE_ARC, IndexTree, StrId}};
use crate::index::core::query::{QueryMap, attr_parts, evaluate_query, filter_index_by_hashes, kwargs_to_hash_query};

use crate::index::core::stored_item::StoredItem;
use crate::index::value::PyValue;

const QUERY_DEPTH_LEN: usize = 12;

#[derive(Clone, Default)]
pub struct IndexAPI{
    pub index: IndexTree,
    pub items: Arc<RwLock<Vec<StoredItem>>>,
    pub allowed_items: Arc<RwLock<Bitmap>>,
    pub parent_index: Option<Weak<IndexAPI>>,
}

impl IndexAPI{

    pub fn new(parent_index: Option<Weak<IndexAPI>>) -> Self {
        Self {
            index: Arc::new(RwLock::new(vec![])),
            items: Arc::new(RwLock::new(vec![])),
            allowed_items: Arc::new(RwLock::new(Bitmap::new())),
            parent_index: parent_index,
        }
    }

    pub fn collect(&self, py:Python) -> PyResult<Vec<Py<Indexable>>> {
        let mut result = vec![];
        let allowed_items = self.get_allowed_items_reader();

        for idx in allowed_items.iter(){
            result.push(self.get_items_reader().get(idx as usize).unwrap().get_py_ref(py));
        }
        Ok(result)
    }

    pub fn get_direct_parents(&self, to_get: &Bitmap) -> HybridSet {
        let items_reader = self.get_items_reader();
        let mut result = HybridSet::new();

        for idx in to_get.iter(){
            result.or_inplace(items_reader[idx as usize].get_parent_ids());
        }

        result
    }

    pub fn get_ids_to_root(&self, idx: u32) -> HybridSet {
        let mut res = HybridSet::new();
        if let Some(weak_parent) = &self.parent_index {
            if let Some(parent) = weak_parent.upgrade(){
                res.or_inplace(&parent.get_parents_from_stored_item(idx as usize));
            }
        }
        res
    }

    pub fn get_parents_from_stored_item(&self, idx: usize) -> HybridSet {
        let guard = self.get_items_reader();
        guard[idx].get_path_to_root()
    }

    pub fn add_object_many(
        &self,
        weak_self: Weak<Self>,
        raw_objs: Vec<(Indexable, Py<Indexable>)>
    ) {
        // 3 pass - wrap in ARC - add meta to index with locks - add to index maps which may call meta locks
        let arc_objs: Vec<(Arc<Indexable>, Arc<Py<Indexable>>)> = raw_objs
            .into_iter()
            .map(|(idx, py)| (Arc::new(idx), Arc::new(py)))
            .collect();

        let mut allowed_writer: RwLockWriteGuard<'_, Bitmap> = self.get_allowed_items_writer();
        let mut items_writer = self.get_items_writer();

        for (rust_handle, py_handle) in &arc_objs {

            rust_handle.add_index(weak_self.clone());
            allowed_writer.add(rust_handle.id);

            let idx = rust_handle.id as usize;
            let stored_item = StoredItem::new(py_handle.clone(), rust_handle.clone(),None);

            if items_writer.len() <= idx{
                items_writer.resize(idx * 2, StoredItem::default());
            }

            items_writer[idx] = stored_item;

        }
        drop(allowed_writer);
        drop(items_writer);

        for (rust_handle, _) in arc_objs {
            for (key, value) in rust_handle.get_py_values().iter() {
                self.add_index(weak_self.clone(), rust_handle.id, *key, value);
            }
        }
    }

    pub fn has_object_id(&self, id: u32) -> bool {
        !Arc::ptr_eq(
            self.get_items_reader().get(id as usize).unwrap_or(&StoredItem::default()).get_owned_handle(),
            &DEFAULT_INDEXABLE_ARC
        )
    }

    pub fn register_path(&self, object_id: u32, parent_id: u32) {
        let mut writer = self.get_items_writer();
        let obj = writer.get_mut(object_id as usize).unwrap();
        obj.add_parent(parent_id);
    }

    pub fn add_object(
        &self,
        weak_self: Weak<IndexAPI>,
        idx: u32,
        stored_item: StoredItem,
        py_val_hashmap: MutexGuard<HybridHashmap<StrId, PyValue>>
    ) {

        self.get_allowed_items_writer().add(idx);
        {
            let mut items_writer = self.get_items_writer();
            if items_writer.len() <= idx as usize{
                items_writer.resize(idx as usize + 1, StoredItem::default());
            }

            items_writer[idx as usize] = stored_item;
        }

        for (attr_id, value) in py_val_hashmap.iter() {
            // if key.starts_with("_"){continue;}
            self.add_index(weak_self.clone(), idx, *attr_id, value);
        }
    }

    pub fn remove(&self, item: &Indexable, parent_id: u32) {
        let item_id = item.id;
        let mut writer = self.get_items_writer();
        if let Some(stored_item) = writer.get_mut(item_id as usize){
            stored_item.remove_parent(parent_id);
            if stored_item.is_orphaned() {
                writer[item_id as usize] = StoredItem::default();
                drop(writer);

                for (key, value) in (*item.get_py_values()).iter(){
                    // if key.starts_with("_"){continue;}
                    self.remove_index(item_id, *key as usize, value);
                }

                self.get_allowed_items_writer().remove(item_id);
            }
        }
    }

    pub fn reduce<'py>(
        &self,
        self_arc: Weak<Self>,
        py: Python,
        kwargs: Option<FxHashMap<String, pyo3::Bound<'py, PyAny>>>,
    ) -> PyResult<()> {
        let query = kwargs_to_hash_query(kwargs.unwrap_or_default())?;
        let mut index = self.get_index_writer();

        let survivors = filter_index_by_hashes(&index, &query);
        let survivors = HybridSet::Large(survivors);

        // Step 1: Remove items not in survivors
        for attr_map in index.iter() {
            attr_map.remove(&survivors);
        }

        // Step 2: Add any missing entries for survivors
        for idx in survivors.iter() {

            let reader = self.get_items_reader();
            let item = reader.get(idx as usize).unwrap().clone();

            let py_item = item.borrow_py_ref(py);
            
            for (attr_id, val) in (*py_item.get_py_values()).iter() {
//                if attr.starts_with("_") {
//                    continue;
//                }
                match index.get_mut(*attr_id as usize){
                    Some(val_map) => {
                        val_map.insert(&val, idx);
                    },
                    None => {
                        let qmap = QueryMap::new(self_arc.clone(), *attr_id);
                        qmap.insert(&val, idx);
                        index.insert(*attr_id as usize, qmap);
                    }
                }
            }
        }

        // Optional: clean up empty submaps
//            index.retain(|_, val_map| {
//                val_map.retain(|_, set| !set.is_empty());
//                !val_map.is_empty()
//            });
        Ok(())
    }


    pub fn reduced(
        &self,
        query: std::collections::HashMap<SmolStr, std::collections::HashSet<PyValue>, rustc_hash::FxBuildHasher>
    ) -> FilteredIndex {
        let index = self.get_index_reader();
        self.filter_from_bitmap(
            filter_index_by_hashes(&index, &query)
        )
    }

    pub fn reduced_query(
        &self,
        query: PyQueryExpr,
    ) -> FilteredIndex {
        let index = self.get_index_reader();
        let allowed = self.get_allowed_items_reader();
        self.filter_from_bitmap(
            evaluate_query(&index, &allowed, &query.inner)
        )
    }

    pub fn get_by_attribute(
        &self,
        query: std::collections::HashMap<SmolStr, std::collections::HashSet<PyValue>, rustc_hash::FxBuildHasher>
    ) -> Bitmap {
        let index = self.get_index_reader();
        filter_index_by_hashes(&index, &query)
    }

    pub fn union_with(&self, other: &IndexAPI) -> PyResult<()>{
        let self_index = self.get_index_reader();
        let other_index = other.get_index_reader();

        if self_index.len() < other_index.len() {
            let additional = other_index.len() - self_index.len();
            drop(self_index);
            let mut self_index = self.get_index_writer();
            self_index.reserve(additional);
            drop(self_index);
        }
        
        let self_index = self.get_index_reader();
        for (self_qm, other_qm) in self_index.iter().zip(other_index.iter()) {
            self_qm.merge(other_qm);
        }

        Ok(())
    }

    pub fn group_by(&self, attr: SmolStr) -> Option<SmallVec<[(PyValue, HybridSet); QUERY_DEPTH_LEN]>> {
        let index = self.get_index_reader();
        let (first_attr, _) = attr_parts(attr.clone());
        let first_attr_id = INTERNER.intern(&first_attr);

        if let Some(attr_map) = index.get(first_attr_id as usize){
            attr_map.group_by(attr)
        } else {
            None
        }
    }

//    fn group_by_count(&self, py:Python, attr: &str) -> FxHashMap<PyValue, usize> {
//        py.allow_threads(||{
//            let index = self.get_index_reader();
//            let mut result: FxHashMap<PyValue, usize> = FxHashMap::new();
//            if let Some(attr_index) = index.get(attr) {
//                for (value, items) in attr_index {
//                    result.insert(value.clone(), items.len());
//                }
//                result
//            } else {
//                FxHashMap::new()
//            }
//        })
//    }

    pub fn update_index(
        &self,
        weak_self: Weak<IndexAPI>,
        attr: StrId, 
        old_pv: Option<&PyValue>,
        new_pv: &PyValue,
        item_id: u32,
    ) {
//        if attr.starts_with("_") {
//            return;
//        }
        
        if let Some(old_val) = old_pv {
            self.remove_index(item_id, attr as usize, old_val);
        }
        self.add_index(weak_self, item_id, attr, &new_pv);
    }

    pub fn get_from_indexes(&self, py: Python, indexes: Bitmap) -> PyResult<Vec<Py<Indexable>>>{
        let items_read = self.get_items_reader();
        let results: Vec<Py<Indexable>> = indexes.iter()
            .map(|arc| items_read.get(arc as usize).unwrap().get_py_ref(py))
            .collect();

        Ok(results)
    }

    pub fn add_index(
        &self,
        weak_self: Weak<IndexAPI>,
        obj_id: u32,
        attr_id: StrId,
        value: &PyValue
    ){
        if let Some(qmap) = self.get_index_reader().get(attr_id as usize) {
            qmap.insert(value, obj_id);
            return;
        }

        let qmap = QueryMap::new(weak_self, attr_id);
        qmap.insert(value, obj_id);
        let mut writer = self.get_index_writer();

        if attr_id >= writer.len() as u32 {
            writer.resize_with((attr_id + 1) as usize, Default::default); // or None if Option
        }
        writer[attr_id as usize] = qmap;

    }

    fn remove_index(
        &self,
        idx: u32,
        attr_id: usize,
        py_value: &PyValue
    ){
        let index = self.get_index_reader();
        if index.len() > attr_id {
            if let Some(val) = index.get(attr_id) { 
                val.remove_id(py_value, idx);
                val.check_prune(py_value);
            };

            if index[attr_id].is_empty(){
                drop(index);
                self.get_index_writer()[attr_id] = Default::default();
            }
        }
    }

    pub fn filter_from_bitmap(&self, bm: Bitmap) -> FilteredIndex {
        FilteredIndex {
            index: self.index.clone(),
            items: self.items.clone(),
            allowed_items: bm
        }
    }

    pub fn is_attr_equal(&self, id: usize, str_id: StrId, val: &PyValue) -> bool {
        self.get_items_reader()
            .get(id)
            .and_then(|item| {
                item.with_attr_id(str_id, |py_val| py_val == val)
            })
            .unwrap_or(false)
    }

    fn get_items_writer(&self) -> RwLockWriteGuard<Vec<StoredItem>> {
        self.items.write().unwrap()
        //self.items.try_write().expect("items writer deadlock")
    }

    fn get_items_reader(&self) -> RwLockReadGuard<Vec<StoredItem>> {
        self.items.read().unwrap()
        //self.items.try_read().expect("cannot read from items")
    }

    pub fn get_index_writer(&self) -> RwLockWriteGuard<Vec<QueryMap>> {
        self.index.write().unwrap()
        //self.index.try_write().expect("index writer deadlock")
    }

    pub fn get_index_reader(&self) -> RwLockReadGuard<Vec<QueryMap>> {
        self.index.read().unwrap()
        //self.index.try_read().expect("cannot read from index")
    }

    fn get_allowed_items_writer(&self) -> RwLockWriteGuard<Bitmap> {
        self.allowed_items.write().unwrap()
        //self.allowed_items.try_write().expect("index writer deadlock")
    }

    fn get_allowed_items_reader(&self) -> RwLockReadGuard<Bitmap> {
        self.allowed_items.read().unwrap()
        //self.allowed_items.try_read().expect("cannot read from index")
    }
}

impl fmt::Debug for IndexAPI {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let index = self.get_index_reader();
        let items = self.get_items_reader();
        let allowed_items = self.get_allowed_items_reader();

        f.debug_struct("IndexAPI")
            .field("index_len", &index.len())
            .field("items_len", &items.len())
            .field("allowed_items_cardinality", &allowed_items.cardinality())
            .finish()
    }
}
