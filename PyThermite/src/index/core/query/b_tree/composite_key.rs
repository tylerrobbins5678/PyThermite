use std::cmp::Ordering;
use crate::index::core::query::b_tree::Key;

const EXPONENT_BIAS: u16 = 16383;
const NUMERIC_MASK: u128 = !0u128 << 32; // Upper 96 bits

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