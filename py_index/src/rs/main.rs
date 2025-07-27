use std::{ops::Bound, time::Instant};
use croaring::Bitmap;
use ordered_float::OrderedFloat;

mod index; // Replace with your actual module path
use index::{BitMapBTree, Key, CompositeKey128};

fn main() {

    let mut tree = BitMapBTree::new();
    let mut all_valid = Bitmap::new();
    let mut id: u32 = 0;

    const N: i64 = 1_000_000;
    // const N: i64 = 15_000;

    // Insert Int keys
    let start = Instant::now();
    for i in -N..N {
        // eprintln!("inserting {}", i);
        all_valid.add(id);
        tree.insert(Key::Int(i), id);
        id += 1;
        //tree.debug_print();
    }


    // Insert Float keys
//    for i in -N * 2..N * 2 {
//        let val = (i as f64) * 0.5;
//        all_valid.add(id);
//        tree.insert(Key::FloatOrdered(OrderedFloat(val)), id);
//        id += 1;
//    }
    let duration = start.elapsed();
    println!("Inserted {} keys in {:?}", id, duration);

    let bm = tree.root.get_bitmap();
    eprintln!("{}", bm.cardinality());

    // tree.debug_print();

    // for i in 0..N{
        // assert!(tree.root.get_bitmap().contains(i as u32));
    // }

    // Query Int range
    let q_start = Instant::now();
    let int_result = tree.range_query(
        Bound::Included(&Key::Int(5000)),
        Bound::Unbounded,
        &all_valid
    );
    let int_duration = q_start.elapsed();
    println!(
        "Int range query returned {} IDs in {:?}",
        int_result.cardinality(),
        int_duration
    );

    // Query Float range
    let f_start = Instant::now();
    let float_result = tree.range_query(
        Bound::Included(&Key::FloatOrdered(OrderedFloat(1.0))),
        Bound::Included(&Key::FloatOrdered(OrderedFloat(15.0))),
        &all_valid
    );

    let f_duration = f_start.elapsed();
    println!(
        "Float range query returned {} IDs in {:?}",
        float_result.cardinality(),
        f_duration
    );

//    tree.debug_print_range(
//        2, 
//        Some(&Key::FloatOrdered(OrderedFloat(0.0))),
//        Some(&Key::FloatOrdered(OrderedFloat(9.5))),
//    );

}
