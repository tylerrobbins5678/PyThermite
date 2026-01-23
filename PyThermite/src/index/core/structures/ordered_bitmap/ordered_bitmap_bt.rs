use croaring::Bitmap;

use crate::index::core::structures::ordered_bitmap::ordered_bitmap::{NumericalBitmap};


impl NumericalBitmap {

    #[inline(always)]
    pub fn get_bt_into(&self, low: u128, high: u128, out: &mut Bitmap, all_valid: &Bitmap) {
        if low > high {
            out.clear();
            return;
        }

        let mut lt = Bitmap::new();
        let mut gt = Bitmap::new();

        self.get_lte_into(high, &mut lt, all_valid);
        self.get_gte_into(low, &mut gt, all_valid);

//        std::thread::scope(|s| {
//            s.spawn(|| {
//                self.get_lte_into(high, &mut lt);
//            });
//
//            s.spawn(|| {
//                self.get_gte_into(low, &mut gt);
//            });
//        });

        // Final intersection (single-threaded)
        lt.and_inplace(&gt);
        out.or_inplace(&lt);
    }


    #[inline(always)]
    pub fn get_bt(&self, low: u128, high: u128) -> Bitmap {
        let mut res = Bitmap::new();
        let all_valid = self.bits[0].all();
        self.get_bt_into(low, high, &mut res, &all_valid);
        res
    }

    #[inline(always)]
    pub fn get_bt_from_valid(&self, low: u128, high: u128, all_valid: &Bitmap) -> Bitmap {
        let mut res = Bitmap::new();
        self.get_bt_into(low, high, &mut res, all_valid);
        res
    }
    
}


#[cfg(test)]
mod tests {
    use super::*;
    use croaring::Bitmap;
    use rand::{Rng, SeedableRng};
    use rand::rngs::StdRng;

    fn scalar_between(v: u128, low: u128, high: u128) -> bool {
        v >= low && v <= high
    }

    fn build_index(values: &[u128]) -> NumericalBitmap {
        let mut idx = NumericalBitmap::new();
        for (id, &v) in values.iter().enumerate() {
            idx.add(v, id as u32);
        }
        idx
    }

    fn bitmap_to_set(bm: &Bitmap) -> Vec<u32> {
        bm.iter().collect()
    }

    #[test]
    fn test_between_basic() {
        let values = vec![
            0u128, 1, 2, 3, 4, 5, 10, 42, 100, 255,
        ];

        let idx = build_index(&values);

        let low = 3;
        let high = 42;

        let res = idx.get_bt(low, high);

        let expected: Vec<u32> = values.iter()
            .enumerate()
            .filter(|(_, &v)| scalar_between(v, low, high))
            .map(|(i, _)| i as u32)
            .collect();

        assert_eq!(bitmap_to_set(&res), expected);
    }

    #[test]
    fn test_between_equal_bounds() {
        let values = (0u128..100).collect::<Vec<_>>();
        let idx = build_index(&values);

        let low = 37;
        let high = 37;

        let res = idx.get_bt(low, high);

        let expected: Vec<u32> = values.iter()
            .enumerate()
            .filter(|(_, &v)| v == 37)
            .map(|(i, _)| i as u32)
            .collect();

        assert_eq!(bitmap_to_set(&res), expected);
    }

    #[test]
    fn test_between_full_range() {
        let values = (0u128..256).collect::<Vec<_>>();
        let idx = build_index(&values);

        let res = idx.get_bt(u128::MIN, u128::MAX);

        assert_eq!(res.cardinality() as usize, values.len());
    }

    #[test]
    fn test_between_empty() {
        let values = (0u128..100).collect::<Vec<_>>();
        let idx = build_index(&values);

        let res = idx.get_bt(200, 300);

        assert!(res.is_empty());
    }

    #[test]
    fn test_between_degenerates_to_lte() {
        let values = (0u128..128).collect::<Vec<_>>();
        let idx = build_index(&values);

        let high = 42;

        let between = idx.get_bt(u128::MIN, high);
        let lte = idx.get_lte(high);

        assert_eq!(between, lte);
    }

    #[test]
    fn test_between_degenerates_to_gte() {
        let values = (0u128..128).collect::<Vec<_>>();
        let idx = build_index(&values);

        let low = 42;

        let between = idx.get_bt(low, u128::MAX);
        let gte = idx.get_gte(low);

        assert_eq!(between, gte);
    }

    #[test]
    fn test_between_adversarial_bits() {
        let values = vec![
            0b0000,
            0b0001,
            0b0011,
            0b0111,
            0b1000,
            0b1111,
        ];

        let idx = build_index(&values);

        let low  = 0b0011;
        let high = 0b1000;

        let res = idx.get_bt(low, high);

        let expected: Vec<u32> = values.iter()
            .enumerate()
            .filter(|(_, &v)| v >= low && v <= high)
            .map(|(i, _)| i as u32)
            .collect();

        assert_eq!(bitmap_to_set(&res), expected);
    }

    #[test]
    fn test_between_fuzz() {
        let mut rng = StdRng::seed_from_u64(0xDEADBEEF);

        let values: Vec<u128> = (0..10_000)
            .map(|_| rng.random::<u64>() as u128)
            .collect();

        let idx = build_index(&values);

        for _ in 0..100 {
            let low  = rng.random::<u64>() as u128;
            let high = rng.random::<u64>() as u128;

            let (low, high) = if low <= high {
                (low, high)
            } else {
                (high, low)
            };

            let res = idx.get_bt(low, high);

            let expected: Vec<u32> = values.iter()
                .enumerate()
                .filter(|(_, &v)| scalar_between(v, low, high))
                .map(|(i, _)| i as u32)
                .collect();

            assert_eq!(
                bitmap_to_set(&res),
                expected,
                "failed for range [{low}, {high}]"
            );
        }
    }
}