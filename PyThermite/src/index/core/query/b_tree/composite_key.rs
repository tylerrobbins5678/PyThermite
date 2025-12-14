use std::cmp::Ordering;
use ordered_float::OrderedFloat;

use crate::index::core::query::b_tree::Key;



const EXPONENT_BITS: u16 = 11;     // 11-bit exponent
const MANTISSA_BITS: u16 = 64;     // 64-bit mantissa
const SIGN_BIT_POS: u16 = 75;      // 76-bit total, sign is top bit

const FLOAT_LENGTH: u16 = EXPONENT_BITS + MANTISSA_BITS + 1; // 1 for sign
const FLOAT_SHIFT: u16 = 128 - FLOAT_LENGTH; // for packing into 128-bit

const EXPONENT_BIAS: u16 = (1 << (EXPONENT_BITS - 1)) - 1;
const NUMERIC_MASK: u128 = ((1u128 << FLOAT_LENGTH) - 1) << (128 - FLOAT_LENGTH);

const ID_MASK: u128 = (1u128 << (128 - FLOAT_LENGTH)) - 1;
const TYPE_BIT_POS: u16 = 32;


#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct CompositeKey128 {
    raw: u128, // Packed representation

    // [f76][<padding>][u1][u32]
    // f76 - 76 bit floating point number
    // bool - type - float => true / int => false
    // u32 - ID attached to said number
}

impl CompositeKey128 {
    /// Constructs a CompositeKey128 from an f64 and u32 ID.
    pub fn new(value: Key, id: u32) -> Self {
        let (float_bits, type_bit) = match value {
            Key::Int(int) => (Self::encode_i64_to_float76(int), 0u128),
            Key::FloatOrdered(float) => (Self::encode_f64_to_float76(float), 1u128),
        };

        Self {
            raw: (float_bits << FLOAT_SHIFT) | (type_bit << TYPE_BIT_POS) | (id as u128),
        }
    }

    fn encode_f64_to_float76(val: OrderedFloat<f64>) -> u128 {

        if val.0 == 0.0 {
            return 1u128 << SIGN_BIT_POS;
        }

        let bits = val.to_bits();
        let sign = (bits >> 63) & 1;
        let ieee_exp = ((bits >> 52) & 0x7FF) as i32;
        let ieee_mant = bits & 0x000F_FFFF_FFFF_FFFF;

        let (exp, mantissa) = if ieee_exp == 0 {
            // subnormal
            let leading = ieee_mant.leading_zeros() - 12;
            let shift = leading + 1;
            let norm_mant = ieee_mant << shift;
            let exponent = -1022 - (shift as i32) + 1 + (EXPONENT_BIAS as i32);
            (exponent as u16, (norm_mant as u128) << (MANTISSA_BITS - 52))
        } else {
            let exponent = ieee_exp - 1023 + EXPONENT_BIAS as i32;
            let mant_53 = (1u64 << 52) | ieee_mant;
            let mantissa = (mant_53 as u128) << (MANTISSA_BITS - 53);
            (exponent as u16, mantissa)
        };

        let mut key_bits = ((sign as u128) << SIGN_BIT_POS)
            | ((exp as u128) << MANTISSA_BITS)
            | (mantissa & ((1u128 << MANTISSA_BITS) - 1));

        if sign == 1 {
            key_bits = !key_bits;
        } else {
            key_bits |= 1u128 << SIGN_BIT_POS;
        }

        key_bits
    }

    fn encode_i64_to_float76(n: i64) -> u128 {
        if n == 0 {
            return 1u128 << SIGN_BIT_POS; // special zero encoding
        }

        let sign = ((n as i128) >> 127) as u128 & 1;
        let abs = n.unsigned_abs();

        let leading = 63 - abs.leading_zeros(); // floor(log2(n))
        let exponent = EXPONENT_BIAS + leading as u16;

        let mantissa = (abs as u128) << (MANTISSA_BITS as u32 - leading - 1); // normalize to 1.x...

        let mut key_bits = (sign << SIGN_BIT_POS) | ((exponent as u128) << MANTISSA_BITS) | (mantissa & ((1u128 << MANTISSA_BITS)-1));

        // Ordering transform
        if sign == 1 {
            key_bits = !key_bits;
        } else {
            key_bits |= 1u128 << SIGN_BIT_POS; // force positive sign bit
        }

        key_bits

    }

    pub fn decode_float(&self) -> f64 {
        let mut key = self.get_value_bits() & ((1u128 << FLOAT_LENGTH)-1);

        if key == (1u128 << SIGN_BIT_POS) {
            return 0.0;
        }

        let is_neg = (key >> SIGN_BIT_POS) & 1 == 0;
        if is_neg { key = !key; }

        let exp = ((key >> MANTISSA_BITS) & ((1u128 << EXPONENT_BITS)-1)) as i32;
        let mant = key & ((1u128 << MANTISSA_BITS)-1);

        let ieee_exp: i32;
        let ieee_mant: u64;

        if exp == 0 {
            ieee_exp = 0;
            ieee_mant = (mant >> (MANTISSA_BITS - 52)) as u64;
        } else {
            ieee_exp = exp - EXPONENT_BIAS as i32 + 1023;
            ieee_mant = ((mant >> (MANTISSA_BITS - 53)) as u64) & ((1<<52)-1);
        }

        let bits = ((is_neg as u64) << 63) | ((ieee_exp as u64) << 52) | ieee_mant;
        f64::from_bits(bits)
    }

    pub fn decode_i64(&self) -> i64 {
        let key = self.get_value_bits() & ((1u128 << FLOAT_LENGTH)-1);

        if key == (1u128 << SIGN_BIT_POS) {
            return 0;
        }

        // Determine if negative
        let was_neg = ((key >> SIGN_BIT_POS) & 1) == 0;

        // Undo inversion
        let restored = if was_neg { !key } else { key };

        let exponent = ((restored >> MANTISSA_BITS) & ((1u128 << EXPONENT_BITS)-1)) as i64;
        let mantissa = restored & ((1u128 << MANTISSA_BITS)-1);

        let leading = exponent - EXPONENT_BIAS as i64;
        let shift_back = MANTISSA_BITS as i64 - leading - 1;

        let abs = (mantissa >> shift_back) as i64;

        if was_neg { -abs } else { abs }
    }

    pub fn get_id(&self) -> u32 {
        // (self.raw & 0xFFFF_FFFF) as u32
        (self.raw & ID_MASK) as u32
    }

    pub fn get_value_bits(&self) -> u128 {
        self.raw >> (128 - FLOAT_LENGTH)
    }

    pub fn get_key(&self) -> u128 {
        self.raw
    }

    pub fn is_float(&self) -> bool {
        ((self.raw >> TYPE_BIT_POS) & 1) != 0
    }

    pub fn cmp_key(&self, key: &Key) -> std::cmp::Ordering {
        let key_bits = match key {
            Key::Int(int) => Self::encode_i64_to_float76(*int),
            Key::FloatOrdered(float) => Self::encode_f64_to_float76(*float),
        };

        let target_raw = key_bits << FLOAT_SHIFT;
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
            let composite = CompositeKey128::new(Key::FloatOrdered(OrderedFloat(val)), 0);
            let decoded = composite.decode_float();
            println!("input  : {:064b}", val.to_bits());
            println!("decoded: {:064b}", decoded.to_bits());
            assert!(composite.is_float() == true);
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
            let composite = CompositeKey128::new(Key::Int(val), 0);
            // Decode using the float decoder
            let decoded = composite.decode_float();
            // integers will be exactly representable as f64
            println!("raw    : {:128b}", composite.get_key());
            println!("input  : {:064b}", (val as f64).to_bits());
            println!("decoded: {:064b}", decoded.to_bits());
            assert!(composite.is_float() == false);
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
            let composite = CompositeKey128::new(Key::Int(val), 0);
            // Decode using the float decoder
            let decoded = composite.decode_i64();
            // integers will be exactly representable as f64
            println!("raw    : {:128b}", composite.get_key());
            println!("input  : {:064b}", val);
            println!("decoded: {:064b}", decoded);
            assert!(composite.is_float() == false);
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
