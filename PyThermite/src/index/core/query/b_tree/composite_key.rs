use std::cmp::Ordering;
use ordered_float::OrderedFloat;

use crate::index::core::query::b_tree::Key;

const EXPONENT_BIAS: u16 = 16383;
const NUMERIC_MASK: u128 = !0u128 << 32; // Upper 96 bits

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct CompositeKey128 {
    raw: u128, // Packed representation
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
        }
    }

    fn encode_f64_to_float96(val: OrderedFloat<f64>) -> u128 {

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
            let mantissa = (mantissa_53 as u128) << (80 - 53);
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

    pub fn decode_float96(encoded: u128) -> f64 {
        let mut key = encoded & ((1u128 << 96)-1);

        if key == 1u128 << 95 { return 0.0; }

        let is_neg = (key >> 95) & 1 == 0;
        if is_neg { key = !key; }

        let exp = ((key >> 80) & 0x7FFF) as i32;
        let mant = key & ((1u128 << 80)-1);

        let ieee_exp: i32;
        let ieee_mant: u64;

        if exp == 0 {
            ieee_exp = 0;
            ieee_mant = (mant >> (80-52)) as u64;
        } else {
            ieee_exp = exp - EXPONENT_BIAS as i32 + 1023;
            ieee_mant = ((mant >> (80-53)) as u64) & ((1<<52)-1);
        }

        let bits = ((is_neg as u64) << 63) | ((ieee_exp as u64) << 52) | ieee_mant;
        f64::from_bits(bits)
    }

    fn decode_i64(encoded: u128) -> i64 {

        // Zero encoding: exponent=mantissa=0, sign bit forced to 1
        if encoded == (1u128 << 95) {
            return 0;
        }

        // Determine if it was originally negative (only negatives have sign=0)
        let was_negative = ((encoded >> 95) & 1) == 0;

        // Undo inversion for negatives
        let restored = if was_negative { !encoded } else { encoded };

        let exponent = ((restored >> 80) & 0x7FFF) as i64;
        let mantissa = restored & ((1u128 << 80) - 1);

        let leading = exponent - EXPONENT_BIAS as i64;
        let shift_back = 80 - leading - 1;

        // SAFETY: shift_back is now always 0..79
        let abs = (mantissa >> shift_back) as i64;

        if was_negative { -abs } else { abs }
    }

    pub fn get_id(&self) -> u32 {
        (self.raw & 0xFFFF_FFFF) as u32
    }

    pub fn get_value_bits(&self) -> u128 {
        self.raw >> 32
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

impl Default for CompositeKey128 {
    fn default() -> Self {
        Self {
            raw: 0u128,
        }
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


#[cfg(test)]
mod tests {
    use super::*;
    use ordered_float::OrderedFloat;

    #[test]
    fn test_f64_encoding_decoding() {
        let values = [
            0.0,
            1.0,
            -1.0,
            123.456,
            -123.456,
            1e-10,
            -1e-10,
            f64::MIN,
            f64::MAX,
        ];

        for &val in &values {
            let ordered = OrderedFloat(val);
            let key_bits = CompositeKey128::encode_f64_to_float96(ordered);
            let decoded = CompositeKey128::decode_float96(key_bits);
            println!("input  : {:064b}", val.to_bits());
            println!("decoded: {:064b}", decoded.to_bits());
            assert!(
                decoded.to_bits() == val.to_bits(),
                "f64 encode/decode failed for {}: got {}",
                val.to_bits(),
                decoded.to_bits()
            );
        }
    }

    #[test]
    fn test_i64_encoding_decoding_to_f64() {
        let values = [0, 1, -1, 42, -42, -2^53 + 1, 2^53 - 1];

        for &val in &values {
            let key_bits = CompositeKey128::encode_i64_to_float96(val);
            // Decode using the float decoder
            let decoded = CompositeKey128::decode_float96(key_bits);
            // integers will be exactly representable as f64
            println!("raw    : {:128b}", key_bits);
            println!("input  : {:064b}", (val as f64).to_bits());
            println!("decoded: {:064b}", decoded.to_bits());
            assert!(
                decoded == (val as f64),
                "i64 encode/decode failed for {}: got {}",
                val,
                decoded
            );
        }
    }

    #[test]
    fn test_i64_encoding_decoding_to_i64() {
        let values = [0, 1, 42, -1, -42, i64::MIN + 1, i64::MAX];

        for &val in &values {
            let key_bits = CompositeKey128::encode_i64_to_float96(val);
            // Decode using the float decoder
            let decoded = CompositeKey128::decode_i64(key_bits);
            // integers will be exactly representable as f64
            println!("raw    : {:128b}", key_bits);
            println!("input  : {:064b}", val);
            println!("decoded: {:064b}", decoded);
            assert!(
                decoded == val,
                "i64 encode/decode failed for {}: got {}",
                val,
                decoded
            );
        }
    }

    #[test]
    fn test_id_preservation() {
        let id: u32 = 123456;
        let val = OrderedFloat(42.0);
        let key = CompositeKey128::new(Key::FloatOrdered(val), id);
        assert_eq!(key.get_id(), id);
    }

    #[test]
    fn test_cmp_key() {
        let k1 = CompositeKey128::new(Key::Int(10), 1);
        let k2 = CompositeKey128::new(Key::Int(20), 2);
        let k3 = CompositeKey128::new(Key::Int(10), 3);

        // Compare numeric keys
        assert!(k1.cmp_key(&Key::Int(10)) == std::cmp::Ordering::Equal);
        assert!(k1.cmp_key(&Key::Int(20)) == std::cmp::Ordering::Less);
        assert!(k2.cmp_key(&Key::Int(10)) == std::cmp::Ordering::Greater);

        // k1 and k3 have same numeric key, different IDs
        assert!(k1.raw != k3.raw);
        assert!(k1.cmp_key(&Key::Int(10)) == k3.cmp_key(&Key::Int(10)));
    }

    #[test]
    fn test_partial_ord() {
        let k1 = CompositeKey128::new(Key::FloatOrdered(OrderedFloat(3.0)), 1);
        let k2 = CompositeKey128::new(Key::FloatOrdered(OrderedFloat(4.0)), 2);
        assert!(k1 < k2);
        assert!(k2 > k1);
    }
}
