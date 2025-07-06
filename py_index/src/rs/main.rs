use std::collections::HashSet;
use std::time::Instant;

use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use roaring::RoaringBitmap;

use croaring::Bitmap;

const NUM_BITS: usize = 10_000_000;
const MAX_INDEX: u64 = 100_000_000;


fn main() {
    // Generate random keys
    let mut rng = StdRng::seed_from_u64(42);
    let keys1: Vec<u32> = (0..NUM_BITS).map(|_| rng.gen_range(0..=MAX_INDEX as u32)).collect();
    let keys2: Vec<u32> = (0..NUM_BITS).map(|_| rng.gen_range(0..=MAX_INDEX as u32)).collect();

    // Benchmark RoaringBitmap
    let start = Instant::now();
    let hs1: HashSet<_> = keys1.iter().cloned().collect();
    let hs2: HashSet<_> = keys2.iter().cloned().collect();
    let insert_time = start.elapsed();
    println!("HashSet insert time: {:?}", insert_time);

    let start = Instant::now();
    
    let inter_count = hs1.intersection(&hs2).count();
    let intersect_time = start.elapsed();
    println!("HashSet intersect time: {:?}", intersect_time);
    println!("HashSet intersect count: {}", inter_count);

    // Croaring
    let start = Instant::now();
    let mut bs1 = Bitmap::new();
    for &k in &keys1 {
        bs1.add(k);
    }
    let mut bs2 = Bitmap::new();
    for &k in &keys2 {
        bs2.add(k);
    }
    let insert_time_bs = start.elapsed();
    println!("Croaring BitSet insert time: {:?}", insert_time_bs);

    let start = Instant::now();
    let bs_inter = bs1.and(&bs2);
    let intersect_time_bs = start.elapsed();
    println!("Croaring BitSet intersect time: {:?}", intersect_time_bs);
    println!("Croaring BitSet intersect count: {:?}", bs_inter.cardinality());

}