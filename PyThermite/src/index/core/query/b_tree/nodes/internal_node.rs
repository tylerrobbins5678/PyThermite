use std::ops::Bound;
use croaring::Bitmap;

use crate::index::core::query::b_tree::{FULL_KEYS, Key, MAX_KEYS, composite_key::CompositeKey128, ranged_b_tree::{BitMapBTreeNode, Positioning}};


#[derive(Debug)]
pub struct InternalNode {
    pub keys: [Option<CompositeKey128>; MAX_KEYS],
    pub children: [Option<Box<BitMapBTreeNode>>; MAX_KEYS],
    pub children_bitmaps: [Option<Bitmap>; MAX_KEYS],
    pub num_keys: usize,
    pub offset: usize,
}


impl InternalNode {
    pub fn new() -> Self{
        Self{
            keys: std::array::from_fn(|_| None),
            children: std::array::from_fn(|_| None),
            children_bitmaps: std::array::from_fn(|_| None),
            num_keys: 0,
            offset: MAX_KEYS / 2
        }
    }

    fn shift_left(&mut self, start: usize, end: usize, amount: usize) {
        // Shift keys left
        for i in start..end {
            let to: usize = i - amount;
            self.keys[to] = self.keys[i].take();
            self.children[to] = self.children[i].take();
            self.children_bitmaps[to] = self.children_bitmaps[i].take();
        }
        if start == self.offset {
            self.offset -= amount;
        }
    }

    fn shift_right(&mut self, start: usize, end: usize, amount: usize) {
        // Shift keys right
        for i in (start..end).rev() {
            let to = i + amount;
            self.keys[to] = self.keys[i].take();
            self.children[to] = self.children[i].take();
            self.children_bitmaps[to] = self.children_bitmaps[i].take();
        }
        if start == self.offset {
            self.offset += amount;
        }
    }

    fn get_key_index(&self, key: &Key, mode: Positioning) -> usize {
        // Find child index to recurse into
        let keys = &self.keys[self.offset..self.offset + self.num_keys];

        let pos = keys.binary_search_by(|probe| {
            let probe = probe.as_ref().unwrap();
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
                while i > 0 && keys[i].as_ref().unwrap() == key {
                    i -= 1;
                }
                i
            },
            (Positioning::LowExclusive, Ok(mut i)) => {
                while i + 1 < self.num_keys && keys[i + 1].as_ref().unwrap() == key {
                    i += 1;
                }
                i
            },
            (Positioning::HighInclusive, Ok(mut i)) => {
                while i + 1 < self.num_keys && keys[i + 1].as_ref().unwrap() == key {
                    i += 1;
                }
                i
            },
            (Positioning::HighExclusive, Ok(mut i)) => {
                while i > 0 && keys[i].as_ref().unwrap() == key {
                    i -= 1;
                }
                i
            }
        }
    }


    pub fn insert(&mut self, key: CompositeKey128) {

        let keys = &self.keys[self.offset..self.offset + self.num_keys];
        let idx = keys.binary_search_by(|probe| {
            let probe = probe.as_ref().unwrap();
            probe.cmp(&key)
        });

        let idx = match idx {
            Ok(_) => panic!("Duplicate ID and key insert"),
            Err(i) => {
                if i == 0 { 0 } else { i - 1 }
            }
        };

        let child = self.children[self.offset + idx].as_deref_mut().expect("Missing child");
        let is_full = match child {
            BitMapBTreeNode::Leaf(leaf) => leaf.is_full(),
            BitMapBTreeNode::Internal(internal) => internal.is_full(),
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
            let probe = probe.as_ref().unwrap();
            probe.cmp(&key)
        });

        let idx = match idx {
            Ok(i) => i,
            Err(i) => if i == 0 { 0 } else { i - 1 }
        };

        let node = self.children[self.offset + idx].as_deref_mut().expect("Missing child");

        node.remove_composite_key(key)
    }


    fn insert_non_full(&mut self, key: CompositeKey128, index: usize){
        match self.children[self.offset + index].as_deref_mut() {
            Some(BitMapBTreeNode::Leaf(leaf)) => {
                leaf.insert_non_full(key);
                if let Some(bitmap) = &mut self.children_bitmaps[self.offset + index] {
                    bitmap.add(key.get_id());
                } else {
                    panic!("Bitmap should be present for leaf");
                }
            }
            Some(BitMapBTreeNode::Internal(internal)) => {
                internal.insert(key);
                if let Some(bitmap) = &mut self.children_bitmaps[self.offset + index] {
                    bitmap.add(key.get_id());
                } else {
                    panic!("Bitmap should be present for internal");
                }
            }
            None => panic!("No child at index {}", index),
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
        let mut right_keys = std::array::from_fn(|_| None);
        let mut children = std::array::from_fn(|_| None);
        let mut children_bm = std::array::from_fn(|_| None);
        let offset = MAX_KEYS / 4;
        for i in 0..len {
            right_keys[offset + i] = self.keys[self.offset + mid + i].take();
            children[offset + i] = self.children[self.offset + mid + i].take();
            children_bm[offset + i] = self.children_bitmaps[self.offset + mid + i].take();
        }

        self.num_keys = mid;
        self.recenter();
        (
            right_keys[offset].clone().expect("invalid offset"),
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
        let left_node = self.children[self.offset + idx].as_deref_mut().unwrap();
        let (sep_key, mut new_node, mut new_bitmap) = match left_node {
            BitMapBTreeNode::Leaf(leaf) => {
                let (k, right_leaf) = leaf.split();
                let bm = right_leaf.get_bitmap();
                (k, BitMapBTreeNode::Leaf(right_leaf), bm)
            }
            BitMapBTreeNode::Internal(internal) => {
                let (k, right_internal) = internal.split();
                let bm = right_internal.get_bitmap();
                (k, BitMapBTreeNode::Internal(right_internal), bm)
            }
        };

        // update current bitmap to include id since it was inserted into the child
        let mut left_bitmap = self.children_bitmaps[self.offset + idx]
            .as_ref()
            .expect("Bitmap must be initialized before split")
            - &new_bitmap;
        
        
        if key <= sep_key {
            left_node.insert(key);
            left_bitmap.add(key.get_id());
        } else {
            new_node.insert(key);
            new_bitmap.add(key.get_id());
        }
            
        self.children_bitmaps[self.offset + idx] = Some(left_bitmap);

        // shift to make room for child
        self.shift_right(self.offset + idx + 1, self.offset + self.num_keys, 1);

        // Insert separator key at idx - greater than current key
        self.keys[self.offset + idx + 1] = Some(sep_key);
        self.children[self.offset + idx + 1] = Some(Box::new(new_node));
        self.children_bitmaps[self.offset + idx + 1] = Some(new_bitmap);

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
        if let Some(left_child) = &self.children[self.offset + low_idx] {
            let child_bitmap = left_child.query_range(lower, upper, allowed);
            res.or_inplace(&child_bitmap);
        }

        // Include fully-contained children bitmaps in the middle
        for i in (low_idx + 1)..high_idx {
            if let Some(bm) = &self.children_bitmaps[self.offset + i] {
                res.or_inplace(&bm.and(allowed));
            }
        }

        // Recurse into right boundary child (only if different from left)
        if high_idx != low_idx {
            if let Some(right_child) = &self.children[self.offset + high_idx] {
                let child_bitmap = right_child.query_range(lower, upper, allowed);
                res.or_inplace(&child_bitmap);
            }
        }

        res
    }

    pub fn is_full(&self) -> bool {
        self.num_keys >= FULL_KEYS && (self.offset == 0 || self.num_keys + self.offset >= MAX_KEYS)
    }

    pub fn least_key(&self) -> CompositeKey128 {
        self.keys[self.offset].clone().expect("incorrect offset")
    }
    
}