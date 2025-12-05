

use std::ops::Bound;

use croaring::Bitmap;

use crate::index::core::query::b_tree::{FULL_KEYS, Key, MAX_KEYS, composite_key::CompositeKey128};


#[derive(Debug, Clone)]
pub struct LeafNode {
    pub keys: [Option<CompositeKey128>; MAX_KEYS],
    pub num_keys: usize,
    pub offset: usize,
}

impl LeafNode {

    pub fn new() -> Self {
        Self{
            keys: std::array::from_fn(|_| None),
            num_keys: 0,
            offset: MAX_KEYS / 2,
        }
    }

    pub fn split(&mut self) -> (CompositeKey128, LeafNode) {

        let mid = self.num_keys  / 2;
        let len = self.num_keys - mid;
        let mut right_keys = std::array::from_fn(|_| None);
        let offset = MAX_KEYS / 4;
        for i in 0..len {
            right_keys[offset + i] = self.keys[self.offset + mid + i].take();
        }

        self.num_keys = mid;
        self.recenter();

        (
            right_keys[offset].clone().expect("invalid offset"),
            Self{
                keys: right_keys,
                num_keys: len,
                offset: offset
            }
        )
    }

    pub fn get_bitmap(&self) -> Bitmap {
        self.keys[self.offset..self.offset + self.num_keys]
            .iter().filter_map(|&x| x).map(|x | x.get_id()).collect()
    }

    pub fn print_debug(&self, label: &str) {
        println!("=== {} ===", label);
        println!("Offset: {}", self.offset);
        println!("Num Keys: {}", self.num_keys);
        println!("Keys:");
        for i in 0..MAX_KEYS {
            let mark = if i >= self.offset && i < self.offset + self.num_keys { "*" } else { " " };
            println!("{} [{}] = {:?}", mark, i, self.keys[i]);
        }
        println!();
    }

    fn shift_left(&mut self, start: usize, end: usize, amount: usize) {
        for i in start..end {
            let to = i - amount;
            self.keys[to] = self.keys[i].take();
        }
    }

    fn shift_right(&mut self, start: usize, end: usize, amount: usize) {
        for i in (start..end).rev() {
            let to = i + amount;
            self.keys[to] = self.keys[i].take();
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


    pub fn insert_non_full(&mut self, key: CompositeKey128) {
        // Find position to insert by scanning from right to left

        let insert_index = match &self.keys[self.offset..self.offset + self.num_keys]
            .binary_search_by(|probe| probe.as_ref().unwrap().cmp(&key))
        {
            Ok(pos) | Err(pos) => *pos,
        };

        // Decide whether to shift left or right
        if self.offset > 0 && (insert_index < self.num_keys / 2) {
            self.shift_left(self.offset, self.offset + insert_index, 1);
            self.offset -= 1
        } else {
            self.shift_right(self.offset + insert_index, self.offset + self.num_keys, 1);
        }


        // Calculate physical index to insert at
        // recalculated here as offset is calculated during shift
        let position = self.offset + insert_index;

        self.keys[position] = Some(key);
        // self.ids[position] = Some(id);

        self.num_keys += 1;

        // shift of one side is full
        if self.offset == 0 || self.offset + self.num_keys == MAX_KEYS {
            self.recenter();
        }

    }

    pub fn remove(&mut self, key: CompositeKey128) -> bool {

        let remove_index = match &self.keys[self.offset..self.offset + self.num_keys]
            .binary_search_by(|probe| probe.as_ref().unwrap().cmp(&key))
        {
            Ok(pos) => *pos,
            Err(_) => return false,
        };

        self.keys[self.offset + remove_index] = None;

        // Decide whether to shift left or right
        if remove_index < self.num_keys / 2 {
            self.shift_right(self.offset, self.offset + remove_index, 1);
            self.offset += 1;
        } else {
            self.shift_left(self.offset + remove_index, self.offset + self.num_keys, 1);
        }

        self.num_keys -= 1;
        true
    }


    pub fn query_range(&self, lower: Bound<&Key>, upper: Bound<&Key>, allowed: &Bitmap) -> Bitmap{
        let mut res = Bitmap::new();
        
        let mut i = 0;
        while let Some(key) = &self.keys[i + self.offset] {

            if match lower {
                Bound::Included(lo) => key >= lo,
                Bound::Excluded(lo) => key > lo,
                Bound::Unbounded => true,
            } {
                break;
            }
            
            i += 1;

            if i >= self.num_keys {
                return res;
            }
        }

        while let Some(key) = &self.keys[i + self.offset] {

            if i >= self.num_keys {
                break;
            }
            
            if match upper {
                Bound::Included(hi) => key > hi,
                Bound::Excluded(hi) => key >= hi,
                Bound::Unbounded => false,
            } {
                break;
            }

            res.add(self.keys[i + self.offset].unwrap().get_id());
            
            i += 1;
        }
        res.and_inplace(allowed);
        res

    }


    pub fn is_full(&self) -> bool {
        self.num_keys >= FULL_KEYS && (self.offset == 0 || self.num_keys + self.offset >= MAX_KEYS)
    }

    pub fn least_key(&self) -> CompositeKey128 {
        self.keys[self.offset].clone().expect("invalid offset")
    }

}
