use std::sync::OnceLock;

use croaring::Bitmap;

use crate::index::core::structures::ordered_bitmap::ordered_bitmap::{BIT_LENGTH, TMP_BITMAP, NumericalBitmap};

type GetLtFn = unsafe fn(&NumericalBitmap, u128, &mut Bitmap, &Bitmap);
static GET_LT_FN: OnceLock<GetLtFn> = OnceLock::new();
static GET_LTE_FN: OnceLock<GetLtFn> = OnceLock::new();

macro_rules! define_get_lt_body {
    ($self:ident, $value:ident, $out:ident, $all_valid:ident) => {{
        TMP_BITMAP.with(|scratch| {
            let mut tmp = scratch.borrow_mut();
            let mut prefix_eq = $all_valid.clone();

            for bit in (0..BIT_LENGTH).rev() {
                let v = (($value >> bit) & 1) as usize;

                tmp.clear();
                tmp.or_inplace(&prefix_eq);
                tmp.and_inplace($self.bits[bit].contains(0));

                let mask = $self.bits[bit].contains(v ^ 1);
                tmp.and_inplace(mask);
                $out.or_inplace(&tmp);

                prefix_eq.and_inplace($self.bits[bit].contains(v));
            }
        })
    }};
}

macro_rules! define_get_lte_body {
    ($self:ident, $value:ident, $out:ident, $all_valid:ident) => {{
        TMP_BITMAP.with(|scratch| {
            let mut tmp = scratch.borrow_mut();
            let mut prefix_eq = $all_valid.clone();

            for bit in (0..BIT_LENGTH).rev() {
                let v = (($value >> bit) & 1) as usize;

                tmp.clear();
                tmp.or_inplace(&prefix_eq);
                tmp.and_inplace($self.bits[bit].contains(0));

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

macro_rules! define_get_lt {
    // with target
    ($name:ident, $feat:literal) => {
        #[cfg_attr(target_arch = "x86_64", target_feature(enable = $feat))]
        unsafe fn $name(&self, value: u128, out: &mut Bitmap, all_valid: &Bitmap) {
            define_get_lt_body!(self, value, out, all_valid);
        }
    };

    // base
    ($name:ident) => {
        #[inline(always)]
        fn $name(&self, value: u128, out: &mut Bitmap, all_valid: &Bitmap) {
            define_get_lt_body!(self, value, out, all_valid);
        }
    };
}

macro_rules! define_get_lte {
    // with target
    ($name:ident, $feat:literal) => {
        #[cfg_attr(target_arch = "x86_64", target_feature(enable = $feat))]
        unsafe fn $name(&self, value: u128, out: &mut Bitmap, all_valid: &Bitmap) {
            define_get_lte_body!(self, value, out, all_valid);
        }
    };

    // base
    ($name:ident) => {
        #[inline(always)]
        fn $name(&self, value: u128, out: &mut Bitmap, all_valid: &Bitmap) {
            define_get_lte_body!(self, value, out, all_valid);
        }
    };
}

impl NumericalBitmap {

    #[inline(always)]
    fn get_lt_impl(&self) -> &GetLtFn {
        GET_LT_FN.get_or_init(|| {
            #[cfg(target_arch = "x86_64")] 
            {
                if std::is_x86_feature_detected!("avx512f") {
                    return Self::get_lt_into_avx512;
                }
                if std::is_x86_feature_detected!("avx2") {
                    return Self::get_lt_into_avx2;
                }
                if std::is_x86_feature_detected!("sse2") {
                    return Self::get_lt_into_sse2;
                }
            }
            Self::get_lt_into_base
        })
    }

    #[inline(always)]
    fn get_lte_impl(&self) -> &GetLtFn {
        GET_LTE_FN.get_or_init(|| {
            #[cfg(target_arch = "x86_64")]
            {
                if std::is_x86_feature_detected!("avx512f") {
                    return Self::get_lte_into_avx512;
                }
                if std::is_x86_feature_detected!("avx2") {
                    return Self::get_lte_into_avx2;
                }
                if std::is_x86_feature_detected!("sse2") {
                    return Self::get_lte_into_sse2;
                }
            }
            Self::get_lte_into_base
        })
    }

    #[inline(always)]
    pub fn get_lt_into(&self, value: u128, out: &mut Bitmap, all_valid: &Bitmap) {
        let f = self.get_lt_impl();
        unsafe { f(self, value, out, all_valid) }
    }

    #[inline(always)]
    pub fn get_lte_into(&self, value: u128, out: &mut Bitmap, all_valid: &Bitmap) {
        let f = self.get_lte_impl();
        unsafe { f(self, value, out, all_valid) }
    }
    
    define_get_lt!(get_lt_into_avx512, "avx512f");
    define_get_lt!(get_lt_into_avx2, "avx2");
    define_get_lt!(get_lt_into_sse2, "sse2");
    define_get_lt!(get_lt_into_base);

    define_get_lte!(get_lte_into_avx512, "avx512f");
    define_get_lte!(get_lte_into_avx2, "avx2");
    define_get_lte!(get_lte_into_sse2, "sse2");
    define_get_lte!(get_lte_into_base);

    #[inline(always)]
    pub fn get_lt(&self, value: u128) -> Bitmap {
        let mut res = Bitmap::new();
        let all_valid = self.bits[0].all();
        self.get_lt_into(value, &mut res, &all_valid);
        res
    }

    #[inline(always)]
    pub fn get_lt_from_valid(&self, value: u128, all_valid: &Bitmap) -> Bitmap {
        let mut res = Bitmap::new();
        self.get_lt_into(value, &mut res, all_valid);
        res
    }

    #[inline(always)]
    pub fn get_lte(&self, value: u128) -> Bitmap {
        let mut res = Bitmap::new();
        let all_valid = self.bits[0].all();
        self.get_lte_into(value, &mut res, &all_valid);
        res
    }

    #[inline(always)]
    pub fn get_lte_from_valid(&self, value: u128, all_valid: &Bitmap) -> Bitmap {
        let mut res = Bitmap::new();
        self.get_lte_into(value, &mut res, all_valid);
        res
    }
    
}
