use std::sync::OnceLock;

use croaring::Bitmap;

use crate::index::core::structures::ordered_bitmap::ordered_bitmap::{BIT_LENGTH, TMP_BITMAP, NumericalBitmap};

type GetGtFn = unsafe fn(&NumericalBitmap, u128, &mut Bitmap, &Bitmap);
static GET_GT_FN: OnceLock<GetGtFn> = OnceLock::new();
static GET_GTE_FN: OnceLock<GetGtFn> = OnceLock::new();

macro_rules! define_get_gt_body {
    ($self:ident, $value:ident, $out:ident, $all_valid:ident) => {{
        TMP_BITMAP.with(|scratch| {
            let mut tmp = scratch.borrow_mut();
            let mut prefix_eq = $all_valid.clone();

            for bit in (0..BIT_LENGTH).rev() {
                let v = (($value >> bit) & 1) as usize;

                // tmp.clone_from(&prefix_eq);
                tmp.clear();
                tmp.or_inplace(&prefix_eq);
                tmp.and_inplace($self.bits[bit].contains(1));

                let mask = $self.bits[bit].contains(v ^ 1);
                tmp.and_inplace(mask);
                $out.or_inplace(&tmp);

                prefix_eq.and_inplace($self.bits[bit].contains(v));
            }
        })
    }};
}

macro_rules! define_get_gte_body {
    ($self:ident, $value:ident, $out:ident, $all_valid:ident) => {{
        TMP_BITMAP.with(|scratch| {
            let mut tmp = scratch.borrow_mut();
            let mut prefix_eq = $all_valid.clone();

            for bit in (0..BIT_LENGTH).rev() {
                let v = (($value >> bit) & 1) as usize;

                tmp.clear();
                tmp.or_inplace(&prefix_eq);
                tmp.and_inplace($self.bits[bit].contains(1));

                let mask = $self.bits[bit].contains(v ^ 1);
                tmp.and_inplace(mask);
                $out.or_inplace(&tmp);

                prefix_eq.and_inplace($self.bits[bit].contains(v));
            }
            
            // identical except for this addition of the equality check
            $out.or_inplace(&prefix_eq);
        })
    }};
}

macro_rules! define_get_gt {
    // with target
    ($name:ident, $feat:literal) => {
        #[cfg_attr(target_arch = "x86_64", target_feature(enable = $feat))]
        unsafe fn $name(&self, value: u128, out: &mut Bitmap, all_valid: &Bitmap) {
            define_get_gt_body!(self, value, out, all_valid);
        }
    };

    // base
    ($name:ident) => {
        #[inline(always)]
        fn $name(&self, value: u128, out: &mut Bitmap, all_valid: &Bitmap) {
            define_get_gt_body!(self, value, out, all_valid);
        }
    };
}

macro_rules! define_get_gte {
    // with target
    ($name:ident, $feat:literal) => {
        #[cfg_attr(target_arch = "x86_64", target_feature(enable = $feat))]
        unsafe fn $name(&self, value: u128, out: &mut Bitmap, all_valid: &Bitmap) {
            define_get_gte_body!(self, value, out, all_valid);
        }
    };

    // base
    ($name:ident) => {
        #[inline(always)]
        fn $name(&self, value: u128, out: &mut Bitmap, all_valid: &Bitmap) {
            define_get_gte_body!(self, value, out, all_valid);
        }
    };
}

impl NumericalBitmap {

    #[inline(always)]
    fn get_gt_impl(&self) -> &GetGtFn {
        GET_GT_FN.get_or_init(|| {
            #[cfg(target_arch = "x86_64")]
            {
                if std::is_x86_feature_detected!("avx512f") {
                    return Self::get_gt_into_avx512;
                }
                if std::is_x86_feature_detected!("avx2") {
                    return Self::get_gt_into_avx2;
                }
                if std::is_x86_feature_detected!("sse2") {
                    return Self::get_gt_into_sse2;
                }
            }
            Self::get_gt_into_base
        })
    }

    #[inline(always)]
    fn get_gte_impl(&self) -> &GetGtFn {
        GET_GTE_FN.get_or_init(|| {
            #[cfg(target_arch = "x86_64")]
            {
                if std::is_x86_feature_detected!("avx512f") {
                    return Self::get_gte_into_avx512;
                }
                if std::is_x86_feature_detected!("avx2") {
                    return Self::get_gte_into_avx2;
                }
                if std::is_x86_feature_detected!("sse2") {
                    return Self::get_gte_into_sse2;
                }
            }
            Self::get_gte_into_base
        })
    }

    #[inline(always)]
    pub fn get_gt_into(&self, value: u128, out: &mut Bitmap, all_valid: &Bitmap) {
        let f = self.get_gt_impl();
        unsafe { f(self, value, out, all_valid) }
    }

    #[inline(always)]
    pub fn get_gte_into(&self, value: u128, out: &mut Bitmap, all_valid: &Bitmap) {
        let f = self.get_gte_impl();
        unsafe { f(self, value, out, all_valid) }
    }
    
    define_get_gt!(get_gt_into_avx512, "avx512f");
    define_get_gt!(get_gt_into_avx2, "avx2");
    define_get_gt!(get_gt_into_sse2, "sse2");
    define_get_gt!(get_gt_into_base);

    define_get_gte!(get_gte_into_avx512, "avx512f");
    define_get_gte!(get_gte_into_avx2, "avx2");
    define_get_gte!(get_gte_into_sse2, "sse2");
    define_get_gte!(get_gte_into_base);

    #[inline(always)]
    pub fn get_gt(&self, value: u128) -> Bitmap {
        let mut res = Bitmap::new();
        let all_valid = self.bits[0].all();
        self.get_gt_into(value, &mut res, &all_valid);
        res
    }

    #[inline(always)]
    pub fn get_gt_from_valid(&self, value: u128, all_valid: &Bitmap) -> Bitmap {
        let mut res = Bitmap::new();
        self.get_gt_into(value, &mut res, all_valid);
        res
    }

    #[inline(always)]
    pub fn get_gte(&self, value: u128) -> Bitmap {
        let mut res = Bitmap::new();
        let all_valid = self.bits[0].all();
        self.get_gte_into(value, &mut res, &all_valid);
        res
    }

    #[inline(always)]
    pub fn get_gte_from_valid(&self, value: u128, all_valid: &Bitmap) -> Bitmap {
        let mut res = Bitmap::new();
        self.get_gte_into(value, &mut res, all_valid);
        res
    }
    
}


#[cfg(test)]
mod tests {
    use super::*;
    use croaring::Bitmap;

    fn bitmap_to_vec(bm: &Bitmap) -> Vec<u32> {
        bm.iter().collect()
    }

    #[test]
    fn gt_empty_index() {
        let idx = NumericalBitmap::new();
        let out = idx.get_gt(10);
        assert!(out.is_empty());
    }

    #[test]
    fn gt_single_value_no_match() {
        let mut idx = NumericalBitmap::new();

        idx.add(5, 1);

        let out = idx.get_lt(5);
        assert!(out.is_empty());
    }

    #[test]
    fn gt_single_value_match() {
        let mut idx = NumericalBitmap::new();

        idx.add(7, 1);

        let out = idx.get_gt(5);
        assert!(out.contains(1));
        assert_eq!(out.cardinality(), 1);
    }

    #[test]
    fn gt_two_values() {
        let mut idx = NumericalBitmap::new();

        idx.add(3, 1);
        idx.add(7, 2);

        let out = idx.get_gt(5);

        assert!(out.contains(2));
        assert!(!out.contains(1));
    }

    #[test]
    fn gt_multiple_distinct() {
        let mut idx = NumericalBitmap::new();

        idx.add(1, 1);
        idx.add(4, 2);
        idx.add(6, 3);
        idx.add(10, 4);

        let out = idx.get_gt(5);

        let res = bitmap_to_vec(&out);
        assert_eq!(res, vec![3, 4]);
    }

    #[test]
    fn gt_same_prefix_different_lsb() {
        let mut idx = NumericalBitmap::new();

        idx.add(0b101000, 1);
        idx.add(0b101001, 2);
        idx.add(0b101010, 3);

        let out = idx.get_gt(0b101000);

        let res = bitmap_to_vec(&out);
        assert_eq!(res, vec![2, 3]);
    }

    #[test]
    fn gt_first_differing_bit_high() {
        let mut idx = NumericalBitmap::new();

        idx.add(0b100000, 1);
        idx.add(0b011111, 2);

        let out = idx.get_gt(0b011111);

        assert!(out.contains(1));
        assert!(!out.contains(2));
    }

    #[test]
    fn gt_first_differing_bit_low() {
        let mut idx = NumericalBitmap::new();

        idx.add(0b111100, 1);
        idx.add(0b111101, 2);

        let out = idx.get_gt(0b111100);

        assert!(out.contains(2));
        assert!(!out.contains(1));
    }

    #[test]
    fn gt_highest_bit_boundary() {
        let mut idx = NumericalBitmap::new();

        let high = 1u128 << (BIT_LENGTH - 1);

        idx.add(high, 1);
        idx.add(high - 1, 2);

        let out = idx.get_gt(high - 1);

        assert!(out.contains(1));
        assert!(!out.contains(2));
    }

    #[test]
    fn gt_all_values() {
        let mut idx = NumericalBitmap::new();

        for i in 0..100 {
            idx.add(i, i as u32);
        }

        let out = idx.get_gt(50);

        let res: Vec<u32> = out.iter().collect();
        let expected: Vec<u32> = (51..100).map(|v| v as u32).collect();

        assert_eq!(res, expected);
    }

    #[test]
    fn gt_none_values() {
        let mut idx = NumericalBitmap::new();

        for i in 0..100 {
            idx.add(i, i as u32);
        }

        let out = idx.get_gt(100);
        assert!(out.is_empty());
    }

    #[test]
    fn gt_id_stability() {
        let mut idx = NumericalBitmap::new();

        idx.add(10, 42);
        idx.add(20, 7);
        idx.add(30, 99);

        let out = idx.get_gt(15);

        assert!(out.contains(7));
        assert!(out.contains(99));
        assert!(!out.contains(42));
    }
}