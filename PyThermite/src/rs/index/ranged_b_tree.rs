use std::cmp::{Ordering};
use std::ops::Bound;
use croaring::Bitmap;

const MAX_KEYS: usize = 96;
const FILL_FACTOR: f64 = 0.9;
const FULL_KEYS: usize = (MAX_KEYS as f64 * FILL_FACTOR) as usize;

const NUMERIC_MASK: u128 = !0u128 << 32; // Upper 96 bits
const EXPONENT_BIAS: u16 = 16383;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct CompositeKey128 {
    raw: u128, // Packed representation
    key: Key,
}

impl CompositeKey128 {
    /// Constructs a CompositeKey128 from an f64 and u32 ID.
    pub fn new(value: Key, id: u32) -> Self {
        let float_bits = match value {
            Key::Int(int) => Self::encode_i64_to_float96(int),
            Key::FloatOrdered(float) => Self::encode_f64_to_float96(float),
        };
        let packed = (float_bits << 32) | (id as u128);

        Self {
            raw: packed,
            key: value
        }
    }

    fn encode_f64_to_float96(val: ordered_float::OrderedFloat<f64>) -> u128 {

        if val.0 == 0.0 {
            return 1u128 << 95;
        }

        let bits = val.to_bits();
        let sign = (bits >> 63) & 1;
        let ieee_exponent = ((bits >> 52) & 0x7FF) as i32;
        let ieee_mantissa = bits & 0x000F_FFFF_FFFF_FFFF;

        let (exp, mantissa) = if ieee_exponent == 0 {
            // Subnormal: normalize mantissa manually
            let leading = ieee_mantissa.leading_zeros() - 12; // 64 - 52
            let shift = leading + 1;
            let norm_mantissa = ieee_mantissa << shift;
            let exponent = -1022 - (shift as i32) + 1 + (EXPONENT_BIAS as i32);
            (exponent as u16, (norm_mantissa as u128) << (80 - 52))
        } else {
            // Normal: add implicit 1 and shift left to 80-bit alignment
            let exponent = ieee_exponent - 1023 + EXPONENT_BIAS as i32;
            let mantissa_53 = (1u64 << 52) | ieee_mantissa;
            let mantissa = ((mantissa_53 >> 1) as u128) << (80 - 52);
            (exponent as u16, mantissa)
        };

        let mut key_bits = ((sign as u128) << 95) | ((exp as u128) << 80) | (mantissa & ((1u128 << 80) - 1)  as u128 );
        // println!("key: {:?}, encoded: {:0128b}", *val, key_bits << 32);
        if sign == 1 {
            key_bits = !key_bits;
        } else {
            key_bits |= 1u128 << 95; // force sign bit to 1 for proper unsigned sorting
        }
        key_bits

    }

    fn encode_i64_to_float96(n: i64) -> u128 {
        if n == 0 {
            return 1u128 << 95;
        }

        let sign = if n < 0 { 1u128 } else { 0 };
        let abs = n.unsigned_abs();

        let leading = 63 - abs.leading_zeros(); // log2(n)
        let exponent = EXPONENT_BIAS + leading as u16;

        let mantissa = (abs as u128) << (80 - leading - 1); // Normalize to 1.x...

        let mut key_bits = (sign << 95) | ((exponent as u128) << 80) | (mantissa & ((1u128 << 80) - 1));
        // println!("key: {:?}, encoded: {:0128b}", n, key_bits << 32);
        if sign == 1 {
            key_bits = !key_bits;
        } else {
            key_bits |= 1u128 << 95; // force sign bit to 1 for proper unsigned sorting
        }
        key_bits

    }

    pub fn get_id(&self) -> u32 {
        (self.raw & 0xFFFF_FFFF) as u32
    }

    pub fn get_key(&self) -> u128 {
        self.raw
    }

    pub fn cmp_key(&self, key: &Key) -> std::cmp::Ordering {
        let key_bits = match key {
            Key::Int(int) => Self::encode_i64_to_float96(*int),
            Key::FloatOrdered(float) => Self::encode_f64_to_float96(*float),
        };

        let target_raw = key_bits << 32;
        (self.raw & NUMERIC_MASK).cmp(&(target_raw & NUMERIC_MASK))
        // self.raw.cmp(&target_raw)
    }
}

impl PartialOrd for CompositeKey128 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.raw.cmp(&other.raw))
    }
}

impl Ord for CompositeKey128 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.raw.cmp(&other.raw)
    }
}

impl PartialEq<Key> for CompositeKey128 {
    fn eq(&self, other: &Key) -> bool {
        self.cmp_key(other) == Ordering::Equal
    }
}

impl PartialOrd<Key> for CompositeKey128 {
    fn partial_cmp(&self, other: &Key) -> Option<Ordering> {
        Some(self.cmp_key(other))
    }
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Key {
    Int(i64),
    FloatOrdered(ordered_float::OrderedFloat<f64>),
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Key::Int(a), Key::Int(b)) => a.cmp(b),
            (Key::FloatOrdered(a), Key::FloatOrdered(b)) => a.cmp(b),
            (Key::Int(a), Key::FloatOrdered(b)) => (*a as f64).partial_cmp(&b.0).unwrap_or(Ordering::Equal),
            (Key::FloatOrdered(a), Key::Int(b)) => a.0.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal),
        }
    }
}

enum Positioning {
    LowInclusive,  // Find the Low `<= key`
    LowExclusive,  // Find the Low `< key`
    HighInclusive,   // Find the High `>= key`
    HighExclusive,   // Find the High `> key`
}

pub struct BitMapBTree {
    pub root: Box<BitMapBTreeNode>,
}

impl BitMapBTree {
    pub fn new() -> Self {
        Self {
            root: Box::new(BitMapBTreeNode::Leaf(LeafNode::new())),
        }
    }

    pub fn insert(&mut self, key: Key, id: u32) {
        if self.root.is_full() {
            self.split_root();
        }
        let composite_key = CompositeKey128::new(key, id);
        self.root.insert(composite_key);
    }

    pub fn remove(&mut self, key: Key, id: u32) -> bool {
        self.root.remove(key, id)
    }

    fn split_root(&mut self) {
        // Extract the current root node
        let old_root = std::mem::replace(&mut self.root, Box::new(BitMapBTreeNode::Leaf(LeafNode::new())));
        let base_index = MAX_KEYS / 2;

        match *old_root {
            BitMapBTreeNode::Leaf(mut leaf) => {
                // Split the full leaf node
                let (sep_key, right_leaf) = leaf.split();
                let left_leaf = leaf; // Left side is the old leaf after split
                
                // Create a new internal node to be the new root
                let mut new_root = InternalNode::new();

                // Insert separator key
                new_root.keys[base_index] = Some(left_leaf.least_key());
                new_root.keys[base_index + 1] = Some(sep_key.clone());

                // Insert the two children
                new_root.children[base_index] = Some(Box::new(BitMapBTreeNode::Leaf(left_leaf)));
                new_root.children[base_index + 1] = Some(Box::new(BitMapBTreeNode::Leaf(right_leaf)));

                // Initialize children bitmaps
                new_root.children_bitmaps[base_index] = new_root.children[base_index].as_ref().map(|child| child.get_bitmap());
                new_root.children_bitmaps[base_index + 1] = new_root.children[base_index + 1].as_ref().map(|child| child.get_bitmap());

                new_root.num_keys = 2;
                new_root.offset = base_index;

                self.root = Box::new(BitMapBTreeNode::Internal(new_root));
            }

            BitMapBTreeNode::Internal(mut internal) => {
                // Split internal node root (similar process)
                let (sep_key, right_internal) = internal.split();
                let left_internal = internal;

                let mut new_root = InternalNode::new();

                new_root.keys[base_index] = Some(left_internal.least_key());
                new_root.keys[base_index + 1] = Some(sep_key.clone());

                new_root.children[base_index] = Some(Box::new(BitMapBTreeNode::Internal(left_internal)));
                new_root.children[base_index + 1] = Some(Box::new(BitMapBTreeNode::Internal(right_internal)));

                new_root.children_bitmaps[base_index] = new_root.children[base_index].as_ref().map(|child| child.get_bitmap());
                new_root.children_bitmaps[base_index + 1] = new_root.children[base_index + 1].as_ref().map(|child| child.get_bitmap());

                new_root.num_keys = 2;
                new_root.offset = base_index;

                self.root = Box::new(BitMapBTreeNode::Internal(new_root));
            }
        }
    }

    pub fn range_query(&self, lower: Bound<&Key>, upper: Bound<&Key>, allowed: &Bitmap) -> Bitmap {
        self.root.query_range(lower, upper, allowed)
    }

    pub fn debug_print(&self) {
        self.root.debug_print(0);
    }

    pub fn debug_print_range(
        &self,
        indent: usize,
        lower: Option<&Key>,
        upper: Option<&Key>,
    ) {
        self.root.debug_print_range(indent, lower, upper);
    }
}

impl Default for BitMapBTree {
    fn default() -> Self {
        BitMapBTree::new()
    }
}

#[derive(Debug)]
pub enum BitMapBTreeNode {
    Internal(InternalNode),
    Leaf(LeafNode),
}

impl BitMapBTreeNode {
    pub fn get_bitmap(&self) -> Bitmap {
        match self {
            BitMapBTreeNode::Leaf(leaf) => leaf.get_bitmap(),
            BitMapBTreeNode::Internal(internal) => internal.get_bitmap(),
        }
    }

    pub fn is_full(&self) -> bool {
        match self {
            BitMapBTreeNode::Leaf(leaf) => leaf.is_full(),
            BitMapBTreeNode::Internal(internal) => internal.is_full(),
        }
    }


    pub fn insert(&mut self, key: CompositeKey128) {
        match self {
            BitMapBTreeNode::Leaf(leaf) => leaf.insert_non_full(key),
            BitMapBTreeNode::Internal(internal) => internal.insert(key),
        }
    }

    pub fn remove(&mut self, key: Key, id: u32) -> bool {
        let composite_key = CompositeKey128::new(key, id);
        self.remove_composite_key(composite_key)
    }
    
    pub fn remove_composite_key(&mut self, key: CompositeKey128) -> bool {
        match self {
            BitMapBTreeNode::Leaf(leaf) => leaf.remove(key),
            BitMapBTreeNode::Internal(internal) => internal.remove(key),
        }
    }

    pub fn query_range(&self, lower: Bound<&Key>, upper: Bound<&Key>, allowed: &Bitmap) -> Bitmap {
        match self {
            BitMapBTreeNode::Leaf(leaf) => {
                leaf.query_range(lower, upper, allowed)
            }

            BitMapBTreeNode::Internal(internal) => {
                internal.query_range(lower, upper, allowed)
            }
        }
    }

    pub fn debug_print_range(
        &self,
        indent: usize,
        lower: Option<&Key>,
        upper: Option<&Key>,
    ) {
        let pad = "  ".repeat(indent);

        match self {
            BitMapBTreeNode::Leaf(leaf) => {
                // println!("{pad}📄 Leaf (offset: {}, keys: {}):", leaf.offset, leaf.num_keys);
                for i in leaf.offset..leaf.offset + leaf.num_keys {
                    if let Some(key) = &leaf.keys[i] {
                        // Check if key is in range
                        if (lower.is_none() || key >= lower.unwrap())
                            && (upper.is_none() || key <= upper.unwrap())
                        {
                            println!("{pad}  *[{i}] = {:?}", key);
                        }
                    }
                }
            }

            BitMapBTreeNode::Internal(internal) => {
                // println!("{pad}🧭 Internal (offset: {}, keys: {}):", internal.offset, internal.num_keys);

                // Print keys within range
                for i in internal.offset..internal.offset + internal.num_keys {
                    if let Some(key) = &internal.keys[i] {
                        if (lower.is_none() || key >= lower.unwrap())
                            && (upper.is_none() || key <= upper.unwrap())
                        {
                            println!("{pad}  *Key[{i}] = {:?}", key);
                        }
                    }
                }

                // Recurse only on children whose keys might overlap the range.
                // Since this is a B-tree, child i corresponds to keys in
                // range (keys[i-1], keys[i]] or appropriate bounds.

                // For simplicity, recurse on all children but you can optimize
                for i in 0..MAX_KEYS {
                    if let Some(child) = &internal.children[i] {
                        // TODO: optimize by checking child's key range if available
                        // println!("{pad}  └── Child[{i}]:");
                        child.debug_print_range(indent + 1, lower, upper);
                    }
                }
            }
        }
    }    

    pub fn debug_print(&self, indent: usize) {
        let pad = "  ".repeat(indent);
        match self {
            BitMapBTreeNode::Leaf(leaf) => {
                println!("{pad}📄 Leaf (offset: {}, keys: {}):", leaf.offset, leaf.num_keys);
                for i in 0..MAX_KEYS {
                    let mark = if i >= leaf.offset && i < leaf.offset + leaf.num_keys { "*" } else { " " };
                    if let Some(key) = &leaf.keys[i] {
                        println!("{pad}  {mark}[{i}] = {:?}", key);
                    } else {
                        println!("{pad}   [{i}] = <empty>");
                    }
                }
            }
            BitMapBTreeNode::Internal(internal) => {
                println!("{pad}🧭 Internal (offset: {}, keys: {}):", internal.offset, internal.num_keys);
                for i in 0..MAX_KEYS {
                    let mark = if i >= internal.offset && i < internal.offset + internal.num_keys { "*" } else { " " };
                    if let Some(key) = &internal.keys[i] {
                        println!("{pad}  {mark}Key[{i}] = {:?}", key);
                    } else {
                        println!("{pad}   Key[{i}] = <empty>");
                    }
                }

                for i in 0..MAX_KEYS {
                    if let Some(child) = &internal.children[i] {
                        println!("{pad}  └── Child[{i}]:");
                        child.debug_print(indent + 1);
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct InternalNode {
    pub keys: [Option<CompositeKey128>; MAX_KEYS],
    pub children: [Option<Box<BitMapBTreeNode>>; MAX_KEYS],
    pub children_bitmaps: [Option<Bitmap>; MAX_KEYS],
    pub num_keys: usize,
    pub offset: usize,
}

#[derive(Debug)]
pub struct LeafNode {
    pub keys: [Option<CompositeKey128>; MAX_KEYS],
    pub num_keys: usize,
    pub offset: usize,
}

impl InternalNode {
    fn new() -> Self{
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
            Ok(e) => panic!("Duplicate ID and key insert"),
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

    fn get_bitmap(&self) -> Bitmap {
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
            self.offset -= 1;
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
