use croaring::Bitmap;

use crate::index::{Index, core::{query::{BulkQueryMapAdder, QueryMap, attr_parts, b_tree::ranged_b_tree::BitMapBTreeIter}, structures::{boolean_bitmap::BooleanBitmap, composite_key::CompositeKey128, hybrid_set::{HybridSet, HybridSetOps}, ordered_bitmap::NumericalBitmap, positional_bitmap::PositionalBitmap, shards::ShardedHashMap}}, types::StrId, value::{PyIterable, PyValue, RustCastValue, StoredIndexable}};


impl QueryMap {

    pub fn keep_only(&self, keep: &Bitmap) {
        self.exact.for_each_mut(|_, bm| {
            bm.and_inplace(&HybridSet::Large(keep.clone()));
        });
        self.write_str_radix_map().keep_only(keep);
        self.write_num_ordered().keep_only(keep);
        self.get_bool_map_writer().keep_only(keep);

        let mut writer = self.get_masked_ids_writer();
        let to_be_removed = keep.andnot(&writer);
        writer.and_inplace(keep);
        let mut mapped_ids = self.get_mapped_ids_writer();
        for r in to_be_removed.iter() {
            mapped_ids.remove(&r);
        }
    }

}