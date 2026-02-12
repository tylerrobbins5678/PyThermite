use std::ops::Bound;
use croaring::Bitmap;

use crate::index::core::{query::b_tree::{FULL_KEYS, Key, MAX_KEYS, nodes::leaf_node::LeafNodeIter, ranged_b_tree::{BitMapBTreeNode, Positioning}}, structures::composite_key::CompositeKey128};


#[derive(Debug, Clone)]
pub struct InternalNode {
    pub keys: [CompositeKey128; MAX_KEYS],
    pub children: [BitMapBTreeNode; MAX_KEYS],
    pub children_bitmaps: [Option<Bitmap>; MAX_KEYS],
    pub num_keys: usize,
    pub offset: usize,
}


impl InternalNode {
    pub fn new() -> Self{
        Self{
            keys: [CompositeKey128::default(); MAX_KEYS],
            children: [const { BitMapBTreeNode::Empty }; MAX_KEYS],
            children_bitmaps: std::array::from_fn(|_| None),
            num_keys: 0,
            offset: MAX_KEYS / 2
        }
    }

    #[inline]
    fn shift_left(&mut self, start: usize, end: usize, amount: usize) {
        // Shift keys left
        for i in start..end {
            let to: usize = i - amount;
            self.keys[to] = self.keys[i];
            self.children[to] = std::mem::replace(&mut self.children[i], BitMapBTreeNode::Empty);
            self.children_bitmaps[to] = self.children_bitmaps[i].take();
        }
    }

    #[inline]
    fn shift_right(&mut self, start: usize, end: usize, amount: usize) {
        // Shift keys right
        for i in (start..end).rev() {
            let to = i + amount;
            self.keys[to] = self.keys[i];
            self.children[to] = std::mem::replace(&mut self.children[i], BitMapBTreeNode::Empty);
            self.children_bitmaps[to] = self.children_bitmaps[i].take();
        }
    }

    fn get_key_index(&self, key: &Key, mode: Positioning) -> usize {
        // Find child index to recurse into
        let keys = &self.keys[self.offset..self.offset + self.num_keys];

        let pos = keys.binary_search_by(|probe| {
            probe.cmp_key(key)
        });

        match (mode, pos) {
            (Positioning::LowInclusive, Err(i)) => {
                if i == 0 { 0 } else { i - 1 }
            },
            (Positioning::LowExclusive, Err(i)) => {
                if i == 0 { 0 } else { i - 1 }
            },
            (Positioning::HighInclusive, Err(i)) => {
                if i == 0 { 0 } else { i - 1 }
            },
            (Positioning::HighExclusive, Err(i)) => {
                if i == 0 { 0 } else { i - 1 }
            },
            (Positioning::LowInclusive, Ok(mut i)) => {
                while i > 0 && keys[i] == *key {
                    i -= 1;
                }
                i
            },
            (Positioning::LowExclusive, Ok(mut i)) => {
                while i + 1 < self.num_keys && keys[i + 1] == *key {
                    i += 1;
                }
                i
            },
            (Positioning::HighInclusive, Ok(mut i)) => {
                while i + 1 < self.num_keys && keys[i + 1] == *key {
                    i += 1;
                }
                i
            },
            (Positioning::HighExclusive, Ok(mut i)) => {
                while i > 0 && keys[i] == *key {
                    i -= 1;
                }
                i
            }
        }
    }


    pub fn insert(&mut self, key: CompositeKey128) {

        let keys = &self.keys[self.offset..self.offset + self.num_keys];
        let idx = keys.binary_search_by(|probe| {
            probe.cmp(&key)
        });

        // subtract 1 as the child index is always less than or equal to the key index
        let idx = match idx {
            Ok(_) => panic!("Duplicate ID and key insert"),
            Err(i) => {
                if i == 0 { 0 } else { i - 1 }
            }
        };

        let is_full = match &self.children[self.offset + idx] {
            BitMapBTreeNode::Leaf(leaf) => leaf.is_full(),
            BitMapBTreeNode::Internal(internal) => internal.is_full(),
            BitMapBTreeNode::Empty => false,
        };

        if is_full {
            self.split_and_insert(key, idx);
        } else {
            self.insert_non_full(key, idx);
        }
    }

    pub fn remove(&mut self, key: CompositeKey128) -> bool {
        let keys = &self.keys[self.offset..self.offset + self.num_keys];
        let idx = keys.binary_search_by(|probe| {
            probe.cmp(&key)
        });

        let idx = match idx {
            Ok(i) => i,
            Err(i) => if i == 0 { 0 } else { i - 1 }
        };

        self.children[self.offset + idx].remove_composite_key(key)
    }


    fn insert_non_full(&mut self, key: CompositeKey128, index: usize){
        match &mut self.children[self.offset + index] {
            BitMapBTreeNode::Leaf(leaf) => {
                leaf.insert_non_full(key);
                if let Some(bitmap) = &mut self.children_bitmaps[self.offset + index] {
                    bitmap.add(key.get_id());
                } else {
                    panic!("Bitmap should be present for leaf");
                }
            }
            BitMapBTreeNode::Internal(internal) => {
                internal.insert(key);
                if let Some(bitmap) = &mut self.children_bitmaps[self.offset + index] {
                    bitmap.add(key.get_id());
                } else {
                    panic!("Bitmap should be present for internal");
                }
            }
            BitMapBTreeNode::Empty => panic!("Cannot insert into empty node"),
        }

        if self.offset == 0 || self.offset + self.num_keys == MAX_KEYS {
            self.recenter();
        }
    }

    fn recenter(&mut self) {
        let desired_offset = (MAX_KEYS - self.num_keys) / 2;

        if desired_offset == self.offset {
            return;
        }

        if desired_offset > self.offset {
            self.shift_right(self.offset, self.offset + self.num_keys, desired_offset - self.offset);
        } else {
            self.shift_left(self.offset, self.offset + self.num_keys, self.offset - desired_offset);
        }

        self.offset = desired_offset;
    }

    pub fn split(&mut self) -> (CompositeKey128, InternalNode) {

        let mid = self.num_keys  / 2;
        let len = self.num_keys - mid;
        let mut right_keys = [CompositeKey128::default(); MAX_KEYS];
        let mut children = [const { BitMapBTreeNode::Empty }; MAX_KEYS];
        let mut children_bm = std::array::from_fn(|_| None);
        let offset = MAX_KEYS / 4;
        for i in 0..len {
            right_keys[offset + i] = self.keys[self.offset + mid + i];
            children[offset + i] = std::mem::replace(&mut self.children[self.offset + mid + i], BitMapBTreeNode::Empty);
            children_bm[offset + i] = self.children_bitmaps[self.offset + mid + i].take();
        }

        self.num_keys = mid;
        self.recenter();
        (
            right_keys[offset],
            Self{
                keys: right_keys,
                children: children,
                children_bitmaps: children_bm,
                num_keys: len,
                offset: offset
            }
        )
    }


    fn split_and_insert(&mut self, key: CompositeKey128, idx: usize) {
        let left_node = &mut self.children[self.offset + idx];
        let (sep_key, mut new_node, mut new_bitmap) = match left_node {
            BitMapBTreeNode::Leaf(leaf) => {
                let (k, right_leaf) = leaf.split();
                let bm = right_leaf.get_bitmap();
                (k, BitMapBTreeNode::Leaf(Box::new(right_leaf)), bm)
            }
            BitMapBTreeNode::Internal(internal) => {
                let (k, right_internal) = internal.split();
                let bm = right_internal.get_bitmap();
                (k, BitMapBTreeNode::Internal(Box::new(right_internal)), bm)
            }
            BitMapBTreeNode::Empty => panic!("Cannot split empty node"),
        };

        // update current bitmap to include id since it was inserted into the child
        let mut left_bitmap =
            self.children_bitmaps[self.offset + idx].take().unwrap();
            left_bitmap.andnot_inplace(&new_bitmap);
            
        if key <= sep_key {
            left_node.insert(key);
            left_bitmap.add(key.get_id());
        } else {
            new_node.insert(key);
            new_bitmap.add(key.get_id());
        }

        self.children_bitmaps[self.offset + idx] = Some(left_bitmap);
        let insert: usize;

        // shift to make room for child
        if self.offset > 0 && (idx < self.num_keys / 2) {
            self.shift_left(self.offset, self.offset + idx + 1, 1);
            self.offset -= 1;
        } else {
            self.shift_right(self.offset + idx + 1, self.offset + self.num_keys, 1);
        }
        
        insert = self.offset + idx + 1;
        // Insert separator key at idx - greater than current key
        self.keys[insert] = sep_key;
        self.children[insert] = new_node;
        self.children_bitmaps[insert] = Some(new_bitmap);

        self.num_keys += 1;

        if self.offset == 0 || self.offset + self.num_keys == MAX_KEYS {
            self.recenter();
        }

    }

    pub fn get_bitmap(&self) -> Bitmap {
        let bitmap_refs: Vec<&Bitmap> = self.children_bitmaps.iter()
            .filter_map(|opt| opt.as_ref())
            .collect();
        Bitmap::fast_or(&bitmap_refs)
    }

    pub fn query_range(&self, lower: Bound<&Key>, upper: Bound<&Key>, allowed: &Bitmap) -> Bitmap{
        let mut res = Bitmap::new();

        let low_idx = match lower {
            Bound::Included(k) => self.get_key_index(&k, Positioning::LowInclusive),
            Bound::Excluded(k) => self.get_key_index(&k, Positioning::LowExclusive),
            Bound::Unbounded => 0,
        };

        let high_idx = match upper {
            Bound::Included(k) => self.get_key_index(&k, Positioning::HighInclusive),
            Bound::Excluded(k) => self.get_key_index(&k, Positioning::HighExclusive),
            Bound::Unbounded => self.num_keys,
        };

        // Recurse into left boundary child
        let child_bitmap = self.children[self.offset + low_idx].query_range(lower, upper, allowed);
        res.or_inplace(&child_bitmap);

        // Include fully-contained children bitmaps in the middle
        for i in (low_idx + 1)..high_idx {
            if let Some(bm) = &self.children_bitmaps[self.offset + i] {
                res.or_inplace(&bm.and(allowed));
            }
        }

        // Recurse into right boundary child (only if different from left)
        if high_idx != low_idx {
            let child_bitmap = &self.children[self.offset + high_idx].query_range(lower, upper, allowed);
            res.or_inplace(&child_bitmap);
        }

        res
    }

    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.num_keys >= FULL_KEYS && (self.offset == 0 || self.num_keys + self.offset >= MAX_KEYS)
    }

    pub fn least_key(&self) -> CompositeKey128 {
        self.keys[self.offset]
    }

}


pub struct InternalNodeIter<'a> {
    node: &'a InternalNode,
    child_idx: usize,
    current_child_iter: Option<Box<dyn Iterator<Item = CompositeKey128> + 'a>>,
}


impl<'a> InternalNodeIter<'a> {
    pub fn new(node: &'a InternalNode) -> Self {
        Self {
            node,
            child_idx: 0,
            current_child_iter: None,
        }
    }
}


impl<'a> Iterator for InternalNodeIter<'a> {
    type Item = CompositeKey128;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // 1) Yield from current child
            if let Some(iter) = &mut self.current_child_iter {
                if let Some(item) = iter.next() {
                    return Some(item);
                }
                self.current_child_iter = None;
            }

            // 2) If no more children, stop
            if self.child_idx > self.node.num_keys {
                return None;
            }

            // 3) Create iterator for next valid child
            self.current_child_iter = match &self.node.children[self.node.offset + self.child_idx] {
                BitMapBTreeNode::Leaf(l) => Some(Box::new(LeafNodeIter::new(l))),
                BitMapBTreeNode::Internal(n) => Some(Box::new(InternalNodeIter::new(n))),
                BitMapBTreeNode::Empty => None, // Empty iterator for empty nodes
            };


            self.child_idx += 1;
        }
    }
}