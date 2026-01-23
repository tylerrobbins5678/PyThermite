use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak};

use rustc_hash::FxHashMap;
use croaring::Bitmap;
use ordered_float::OrderedFloat;
use pyo3::{Python, types::{PyListMethods, PySetMethods, PyTupleMethods}};
use smallvec::SmallVec;
use smol_str::SmolStr;

const QUERY_DEPTH_LEN: usize = 12;

use crate::index::{Index, core::{id_alloc::{allocate_id, free_id}, query::{BulkQueryMapAdder, attr_parts, b_tree::ranged_b_tree::BitMapBTreeIter}, structures::{composite_key::CompositeKey128, hybrid_set::{HybridSet, HybridSetOps}, ordered_bitmap::NumericalBitmap, positional_bitmap::PositionalBitmap, shards::ShardedHashMap}}, types::StrId, value::{PyIterable, PyValue, RustCastValue, StoredIndexable}};
use crate::index::core::index::IndexAPI;
use crate::index::core::stored_item::{StoredItem, StoredItemParent};
use crate::index::core::query::b_tree::{BitMapBTree, Key};

#[derive(Default)]
pub struct QueryMap {
    pub exact: ShardedHashMap<PyValue, HybridSet>,
    pub str_radix_map: RwLock<PositionalBitmap>,
    pub parent: Weak<IndexAPI>,
    pub num_ordered: RwLock<NumericalBitmap>,
    pub nested: Arc<IndexAPI>,
    pub attr_stored: StrId,
    pub mapped_ids: RwLock<FxHashMap<u32, u32>>,
    pub masked_ids: RwLock<Bitmap>,
    stored_items: Arc<RwLock<Vec<StoredItem>>>,
}

unsafe impl Send for QueryMap {}
unsafe impl Sync for QueryMap {}

impl QueryMap {
    pub fn new(parent: Weak<IndexAPI>, attr_id: StrId) -> Self {
        let stored_items = if let Some(p) = parent.upgrade() {
            p.items.clone()
        } else {
            Arc::new(RwLock::new(Vec::new()))
        };
        Self{
            exact: ShardedHashMap::<PyValue, HybridSet>::with_shard_count(16),
            str_radix_map: RwLock::new(PositionalBitmap::new()),
            attr_stored: attr_id,
            parent: parent.clone(),
            num_ordered: RwLock::new(NumericalBitmap::new()),
            nested: Arc::new(IndexAPI::new(Some(parent))),
            mapped_ids: FxHashMap::default().into(),
            masked_ids: RwLock::new(Bitmap::new()),
            stored_items
        }
    }

    #[inline(always)]
    pub(crate) fn insert_exact(&self, value: &PyValue, obj_id: u32){
        let mut shard = self.exact.get_shard(&value);
        match shard.get_mut (value) {
            Some(hs) => {
                hs.add(obj_id);
            }
            None => {
                shard.insert(value.clone(), HybridSet::of(&[obj_id]));
            }
        }
    }

    #[inline]
    fn insert_str(&self, value: &str, obj_id: u32) {
        self.write_str_radix_map().add(value, obj_id);
    }

    #[inline]
    fn remove_str(&self, value: &str, obj_id: u32) {
        self.write_str_radix_map().remove(value, obj_id);
    }

    fn insert_num_ordered(&self, key: Key, obj_id: u32){
        let composit_key = CompositeKey128::new(key, obj_id);
        let mut writer = self.write_num_ordered();
        writer.add(composit_key.get_value_bits(), obj_id);
    }

    fn remove_num_ordered(&self, key: Key, obj_id: u32){
        let composit_key = CompositeKey128::new(key, obj_id);
        let mut writer = self.write_num_ordered();
        writer.remove(composit_key.get_value_bits(), obj_id);
    }

    pub(crate) fn insert_indexable(&self, index_obj: &StoredIndexable, obj_id: u32){
        let mut path = HybridSet::new();

        if let Some(parent) = self.parent.upgrade() {
            path = parent.get_parents_from_stored_item(obj_id as usize);
        }

        let id: u32 = index_obj.owned_handle.id;

        if path.contains(id){
            return;
        }

        // register the index in the object
        let weak_nested = Arc::downgrade(&self.nested);
        index_obj.owned_handle.add_index(weak_nested.clone());

        if self.nested.has_object_id(id) {
            self.nested.register_path(id, obj_id);
        } else {
            let mut hs = HybridSet::new();
            hs.add(obj_id);
            let stored_parent = StoredItemParent {
                ids: hs,
                path_to_root: path,
                index: weak_nested.clone(),
            };

            let stored_item = StoredItem::new(index_obj.python_handle.clone(), index_obj.owned_handle.clone(), Some(stored_parent));
            let py_values = index_obj.owned_handle.get_py_values();
            self.nested.add_object(weak_nested, id, stored_item, py_values);
        }
    }

    fn insert_iterable(&self, iterable: &PyIterable, obj_id: u32){
        Python::with_gil(|py| {
            match iterable {
                PyIterable::Dict(_) => {
//                    let dict = py_dict.bind(py);
//                    dict.iter().for_each(|(k, v)| {
//                        self.iterable.entry(k).or_insert(k)
//                    });
                },

                PyIterable::List(py_list) => {
                    for item in py_list.bind(py).iter(){
                        let index_id = allocate_id();
                        self.get_mapped_ids_writer().insert(index_id, obj_id);
                        self.get_masked_ids_writer().add(index_id);
                        self.insert(&PyValue::new(item), index_id);
                    }
                },
                PyIterable::Tuple(py_tuple) => {
                    for item in py_tuple.bind(py).iter(){
                        let index_id = allocate_id();
                        self.get_mapped_ids_writer().insert(index_id, obj_id);
                        self.get_masked_ids_writer().add(index_id);
                        self.insert(&PyValue::new(item), index_id);
                    }
                }
                PyIterable::Set(py_set) => {
                    for item in py_set.bind(py).iter(){
                        let index_id = allocate_id();
                        self.get_mapped_ids_writer().insert(index_id, obj_id);
                        self.get_masked_ids_writer().add(index_id);
                        self.insert(&PyValue::new(item), index_id);
                    }
                },
            }
        });
    }

    #[inline(always)]
    pub fn insert(&self, value: &PyValue, obj_id: u32){
        // Insert into the right ordered map based on primitive type
        match &value.get_primitive() {
            RustCastValue::Int(i) => {
                //self.insert_exact(value, obj_id);
                self.insert_num_ordered(Key::Int(*i), obj_id);
            }
            RustCastValue::Float(f) => {
                //elf.insert_exact(value, obj_id);
                self.insert_num_ordered(Key::FloatOrdered(OrderedFloat(*f)), obj_id);
            }
            RustCastValue::Ind(index_obj) => {
                self.insert_exact(value, obj_id);
                self.insert_indexable(index_obj, obj_id);
            },
            RustCastValue::Iterable(py_iterable) => {
                self.insert_iterable(py_iterable, obj_id);
            }
            RustCastValue::Bool(_) => self.insert_exact(value, obj_id),
            RustCastValue::Str(extracted_str) => {
                self.insert_str(extracted_str, obj_id);
                // self.insert_exact(value, obj_id);
            },
            RustCastValue::Unknown => {
                self.insert_exact(value, obj_id);
            },
        }
    }

    pub fn check_prune(&self, val: &PyValue) {
        let mut shard = self.exact.get_shard(&val);
        if let Some(ev) = shard.get(val) {
            if ev.is_empty() {
                shard.remove(val); // no clone needed
            }
        }
    }

    pub fn merge(&self, other: &Self) {
        // Iterate over all values in `self` mutably
        self.exact.for_each_mut(|key_self, bm_self| {
            // Try to get the corresponding value from `other`
            if let Some(bm_other) = other.exact.get(key_self) {
                bm_self.or_inplace(&bm_other);
            }
        });
        self.write_str_radix_map().merge(&other.read_str_radix_map());
        self.write_num_ordered().merge(&other.read_num_ordered());
        self.get_masked_ids_writer().or_inplace(&other.get_masked_ids_reader());
        self.get_mapped_ids_writer().extend(other.get_mapped_ids_reader().iter());
    }

    pub fn is_empty(&self) -> bool {
        self.exact.is_empty()
    }

    pub fn get<'a>(
        &self,
        guard: &'a RwLockReadGuard<FxHashMap<PyValue, HybridSet>>,
        key: &PyValue,
    ) -> Option<&'a HybridSet> {
        guard.get(key)
    }

    pub fn get_mut<'a>(
        &self,
        guard: &'a mut RwLockWriteGuard<FxHashMap<PyValue, HybridSet>>,
        key: &PyValue,
    ) -> Option<&'a mut HybridSet> {
        guard.get_mut(key)
    }

    fn remove_exact(&self, py_value: &PyValue, idx: u32) {
        let mut shard = self.exact.get_shard(py_value);
        if let Some(hs) = shard.get_mut(py_value){
            hs.remove(idx);
        }
    }

    fn remove_iterable(&self, iterable: &PyIterable, obj_id: u32) {
        Python::with_gil(|py| {
            match iterable {
                PyIterable::Dict(_) => {
    //                let dict = py_dict.bind(py);
    //                dict.iter().for_each(|(k, v)| {
    //                    self.iterable.entry(k).or_insert(k)
    //                });
                },

                PyIterable::List(py_list) => {
                    for item in py_list.bind(py).iter(){
                        let mut writer = self.get_mapped_ids_writer();
                        let mut ids_writer = self.get_masked_ids_writer();
                        writer.get(&obj_id).map(|mapped_id| {
                            free_id(*mapped_id);
                            ids_writer.remove(*mapped_id);
                            self.remove_id(&PyValue::new(item), *mapped_id);
                        });
                        writer.remove(&obj_id);
                    }
                },
                PyIterable::Tuple(py_tuple) => {
                    for item in py_tuple.bind(py).iter(){
                        let mut writer = self.get_mapped_ids_writer();
                        let mut ids_writer = self.get_masked_ids_writer();
                        writer.get(&obj_id).map(|mapped_id| {
                            free_id(*mapped_id);
                            ids_writer.remove(*mapped_id);
                            self.remove_id(&PyValue::new(item), *mapped_id);
                        });
                        writer.remove(&obj_id);
                    }
                }
                PyIterable::Set(py_set) => {
                    for item in py_set.bind(py).iter(){
                        let mut writer = self.get_mapped_ids_writer();
                        let mut ids_writer = self.get_masked_ids_writer();
                        writer.get(&obj_id).map(|mapped_id| {
                            free_id(*mapped_id);
                            ids_writer.remove(*mapped_id);
                            self.remove_id(&PyValue::new(item), *mapped_id);
                        });
                        writer.remove(&obj_id);
                    }
                },
            }
        })
    }

    pub fn remove_id(&self, py_value: &PyValue, idx: u32) {
        match &py_value.get_primitive(){
            RustCastValue::Int(i) => {
                // self.remove_exact(py_value, idx);
                self.remove_num_ordered(Key::Int(*i), idx);
            }
            RustCastValue::Float(f) => {
                // self.remove_exact(py_value, idx);
                self.remove_num_ordered(Key::FloatOrdered(OrderedFloat(*f)), idx);
            }
            RustCastValue::Str(extracted_str) => {
                self.remove_str(extracted_str, idx);
                // self.remove_exact(py_value, idx);
            },
            RustCastValue::Bool(_) => self.remove_exact(py_value, idx),
            RustCastValue::Ind(indexable) => {
                self.remove_exact(py_value, idx);
                self.nested.remove(&indexable.owned_handle, idx);
            },
            RustCastValue::Iterable(py_iterable) => {
                self.remove_iterable(py_iterable, idx);
            },
            RustCastValue::Unknown => {
                self.remove_exact(py_value, idx);
            },
        };
    }

    pub fn unmask_ids(&self, ids: &mut Bitmap) {
        let to_find = self.get_masked_ids_reader().and(ids);
        ids.andnot_inplace(&to_find);
        let mapping = self.get_mapped_ids_reader();
        let mut buff = [0u32; 1024];
        let mut buff_size = 0;
        let mut res = Bitmap::new();
        for i in to_find.iter() {
            if let Some(mapped_id) = mapping.get(&i) {
                buff[buff_size] = *mapped_id;
                buff_size += 1;
            }
            if buff_size >= 1024 {
                res.add_many(&buff[0..buff_size]);
                buff_size = 0;
            }
        }
        res.add_many(&buff[0..buff_size]);
        ids.or_inplace(&res);
    }

    pub fn remove(&self, filter_bm: &HybridSet) {
        self.exact.for_each_mut(|_, bm| {
            bm.and_inplace(filter_bm);
        });
    }

    pub fn group_by(&self, sub_query: SmolStr) -> Option<SmallVec<[(PyValue, HybridSet); QUERY_DEPTH_LEN]>> {
        None
    }

    pub fn get_allowed_parents(&self, child_bm: &Bitmap) -> HybridSet {
        self.nested.get_direct_parents(child_bm)
    }

    pub fn get_stored_items(&self) -> &Arc<RwLock<Vec<StoredItem>>> {
        &self.stored_items
    }
}


impl QueryMap {
    pub fn read_num_ordered(&self) -> std::sync::RwLockReadGuard<'_, NumericalBitmap> {
        self.num_ordered.read().unwrap()
    }
    pub fn write_num_ordered(&self) -> std::sync::RwLockWriteGuard<'_, NumericalBitmap> {
        self.num_ordered.write().unwrap()
    }
    pub fn write_str_radix_map(&self) -> std::sync::RwLockWriteGuard<'_, PositionalBitmap> {
        self.str_radix_map.write().unwrap()
    }
    pub fn read_str_radix_map(&self) -> std::sync::RwLockReadGuard<'_, PositionalBitmap> {
        self.str_radix_map.read().unwrap()
    }
    pub fn get_mapped_ids_reader(&self) -> std::sync::RwLockReadGuard<'_, FxHashMap<u32, u32>> {
        self.mapped_ids.read().unwrap()
    }
    pub fn get_mapped_ids_writer(&self) -> std::sync::RwLockWriteGuard<'_, FxHashMap<u32, u32>> {
        self.mapped_ids.write().unwrap()
    }
    pub fn get_masked_ids_reader(&self) -> std::sync::RwLockReadGuard<'_, Bitmap> {
        self.masked_ids.read().unwrap()
    }
    pub fn get_masked_ids_writer(&self) -> std::sync::RwLockWriteGuard<'_, Bitmap> {
        self.masked_ids.write().unwrap()
    }
}

impl QueryMap {
    pub fn get_bulk_writer(&self) -> BulkQueryMapAdder {
        BulkQueryMapAdder::new(self)
    }
}