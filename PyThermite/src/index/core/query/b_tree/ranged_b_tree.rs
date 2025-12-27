use std::ops::Bound;
use croaring::Bitmap;

use crate::index::core::query::b_tree::{Key, composite_key::CompositeKey128, nodes::{InternalNode, InternalNodeIter, LeafNode, LeafNodeIter}};

pub const MAX_KEYS: usize = 96;
pub const FILL_FACTOR: f64 = 0.97;
pub const FULL_KEYS: usize = (MAX_KEYS as f64 * FILL_FACTOR) as usize;


pub enum Positioning {
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
            root: Box::new(BitMapBTreeNode::Leaf(Box::new(LeafNode::new()))),
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
        let old_root = std::mem::replace(&mut self.root, Box::new(BitMapBTreeNode::Leaf(Box::new(LeafNode::new()))));
        let base_index = MAX_KEYS / 2;

        match *old_root {
            BitMapBTreeNode::Leaf(mut leaf) => {
                // Split the full leaf node
                let (sep_key, right_leaf) = leaf.split();
                let left_leaf = leaf; // Left side is the old leaf after split
                
                // Create a new internal node to be the new root
                let mut new_root = InternalNode::new();

                // Insert separator key
                new_root.keys[base_index] = left_leaf.least_key();
                new_root.keys[base_index + 1] = sep_key;

                // Insert the two children
                new_root.children[base_index] = Some(BitMapBTreeNode::Leaf(left_leaf));
                new_root.children[base_index + 1] = Some(BitMapBTreeNode::Leaf(Box::new(right_leaf)));

                // Initialize children bitmaps
                new_root.children_bitmaps[base_index] = new_root.children[base_index].as_ref().map(|child| child.get_bitmap());
                new_root.children_bitmaps[base_index + 1] = new_root.children[base_index + 1].as_ref().map(|child| child.get_bitmap());

                new_root.num_keys = 2;
                new_root.offset = base_index;

                self.root = Box::new(BitMapBTreeNode::Internal(Box::new(new_root)));
            }

            BitMapBTreeNode::Internal(mut internal) => {
                // Split internal node root (similar process)
                let (sep_key, right_internal) = internal.split();
                let left_internal = internal;

                let mut new_root = InternalNode::new();

                new_root.keys[base_index] = left_internal.least_key();
                new_root.keys[base_index + 1] = sep_key;

                new_root.children[base_index] = Some(BitMapBTreeNode::Internal(left_internal));
                new_root.children[base_index + 1] = Some(BitMapBTreeNode::Internal(Box::new(right_internal)));

                new_root.children_bitmaps[base_index] = new_root.children[base_index].as_ref().map(|child| child.get_bitmap());
                new_root.children_bitmaps[base_index + 1] = new_root.children[base_index + 1].as_ref().map(|child| child.get_bitmap());

                new_root.num_keys = 2;
                new_root.offset = base_index;

                self.root = Box::new(BitMapBTreeNode::Internal(Box::new(new_root)));
            }
            BitMapBTreeNode::Empty => {
                // Do nothing for empty root
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

#[derive(Debug, Clone)]
pub enum BitMapBTreeNode {
    Internal(Box<InternalNode>),
    Leaf(Box<LeafNode>),
    Empty,
}

impl BitMapBTreeNode {
    pub fn get_bitmap(&self) -> Bitmap {
        match self {
            BitMapBTreeNode::Leaf(leaf) => leaf.get_bitmap(),
            BitMapBTreeNode::Internal(internal) => internal.get_bitmap(),
            BitMapBTreeNode::Empty => Bitmap::new(),
        }
    }

    pub fn is_full(&self) -> bool {
        match self {
            BitMapBTreeNode::Leaf(leaf) => leaf.is_full(),
            BitMapBTreeNode::Internal(internal) => internal.is_full(),
            BitMapBTreeNode::Empty => false,
        }
    }


    pub fn insert(&mut self, key: CompositeKey128) {
        match self {
            BitMapBTreeNode::Leaf(leaf) => leaf.insert_non_full(key),
            BitMapBTreeNode::Internal(internal) => internal.insert(key),
            BitMapBTreeNode::Empty => {
                panic!("Cannot insert into an empty node!");
            }
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
            BitMapBTreeNode::Empty => {
                panic!("Cannot remove from an empty node!");
            }
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
            BitMapBTreeNode::Empty => {
                Bitmap::new()
            }
        }
    }

    pub fn least_key(&self) -> CompositeKey128 {
        match self {
            BitMapBTreeNode::Internal(internal_node) => internal_node.least_key(),
            BitMapBTreeNode::Leaf(leaf_node) => leaf_node.least_key(),
            BitMapBTreeNode::Empty => CompositeKey128::default(),
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
                    let key = &leaf.keys[i];
                    // Check if key is in range
                    if (lower.is_none() || key >= lower.unwrap())
                        && (upper.is_none() || key <= upper.unwrap())
                    {
                        println!("{pad}  *[{i}] = {:?}", key);
                    }
                }
            }

            BitMapBTreeNode::Internal(internal) => {
                // println!("{pad}🧭 Internal (offset: {}, keys: {}):", internal.offset, internal.num_keys);

                // Print keys within range
                for i in internal.offset..internal.offset + internal.num_keys {
                    let key = &internal.keys[i];
                    if (lower.is_none() || key >= lower.unwrap())
                        && (upper.is_none() || key <= upper.unwrap())
                    {
                        println!("{pad}  *Key[{i}] = {:?}", key);
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
            BitMapBTreeNode::Empty => {
                // Do nothing for empty nodes
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
                    let key = &leaf.keys[i];
                    if *key != CompositeKey128::default() {
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
                    let key = &internal.keys[i];
                    if *key != CompositeKey128::default() {
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
            BitMapBTreeNode::Empty => {
                println!("{pad}<Empty Node>");
            }
        }
    }
}

pub struct BitMapBTreeIter<'a> {
    inner: BitMapBTreeNodeIter<'a>,
}

pub enum BitMapBTreeNodeIter<'a> {
    Leaf(LeafNodeIter<'a>),
    Internal(InternalNodeIter<'a>),
}

impl<'a> BitMapBTreeIter<'a> {
    pub fn new(tree: &'a BitMapBTree) -> Self {
        let inner = match tree.root.as_ref() {
            BitMapBTreeNode::Leaf(leaf) => BitMapBTreeNodeIter::Leaf(LeafNodeIter::new(leaf)),
            BitMapBTreeNode::Internal(internal) => BitMapBTreeNodeIter::Internal(InternalNodeIter::new(internal)),
            BitMapBTreeNode::Empty => panic!("Cannot create iterator for empty tree!"),
        };

        Self { inner }
    }
}


impl<'a> Iterator for BitMapBTreeIter<'a> {
    type Item = CompositeKey128;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            BitMapBTreeNodeIter::Leaf(iter) => iter.next(),
            BitMapBTreeNodeIter::Internal(iter) => iter.next(),
        }
    }
}




#[test]
fn test_btree_iter_after_large_inserts() {
    use crate::index::core::query::b_tree::BitMapBTree;
    use crate::index::core::query::b_tree::Key;

    let mut tree = BitMapBTree::new();

    // ---- Insert 1000 x key=0 ----
    for i in 0..1000 {
        tree.insert(Key::Int(0), i);
    }

    // ---- Insert 1000 x key=1 ----
    for i in 1000..2000 {
        tree.insert(Key::Int(1), i);
    }

    // ---- Insert 1000 x key=50 ----
    for i in 2000..3000 {
        tree.insert(Key::Int(50), i);
    }

    // ---- Iterate ----
    let iter = BitMapBTreeIter::new(&tree);
    let items: Vec<_> = iter.collect();

    assert_eq!(items.len(), 3000, "Iterator did not yield all items!");

    // Extract just the Key::Int values
    let values: Vec<f64> = items
        .iter()
        .map(|ck: &CompositeKey128| ck.decode_float())
        .collect();

    // ---- Check correct ordering ----
    // Should be:
    // 0 repeated 1000 times,
    // 1 repeated 1000 times,
    // 50 repeated 1000 times.

    assert_eq!(values[0], 0.0);
    assert_eq!(values[999], 0.0);

    assert_eq!(values[1000], 1.0);
    assert_eq!(values[1999], 1.0);

    assert_eq!(values[2000], 50.0);
    assert_eq!(values[2999], 50.0);

    // Multiplicity check
    assert_eq!(values.iter().filter(|v| **v == 0.0).count(), 1000);
    assert_eq!(values.iter().filter(|v| **v == 1.0).count(), 1000);
    assert_eq!(values.iter().filter(|v| **v == 50.0).count(), 1000);
}