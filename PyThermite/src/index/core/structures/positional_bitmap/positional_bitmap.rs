use croaring::Bitmap;

#[derive(Debug, Clone)]
struct CharacterMap {
    maps_u8: [Bitmap; 256],
    boundry_bytes: Bitmap // used to mark boundries for start - end
}

impl CharacterMap {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline(always)]
    pub fn add(&mut self, byte_id: u8, id: u32, is_boundry: bool) {
        if is_boundry {
            self.boundry_bytes.add(id);
        }
        unsafe {
            self.maps_u8.get_unchecked_mut(byte_id as usize).add(id)
        }
    }

    #[inline(always)]
    pub fn remove(&mut self, byte_id: u8, id: u32) {
        self.boundry_bytes.remove(id);
        unsafe {
            self.maps_u8.get_unchecked_mut(byte_id as usize).remove(id)
        }
    }

    #[inline(always)]
    pub fn get_boundry_bytes(&self) -> &Bitmap {
        &self.boundry_bytes
    }

    #[inline(always)]
    pub fn merge(&mut self, other: &CharacterMap) {
        unsafe {
            for byte_id in 0..256 {
                self.maps_u8.get_unchecked_mut(byte_id).or_inplace(other.contains(byte_id as u8));
            }
        }
        self.boundry_bytes.or_inplace(&other.boundry_bytes);
    }

    #[inline(always)]
    pub fn contains(&self, byte_id: u8) -> &Bitmap {
        unsafe {
            self.maps_u8.get_unchecked(byte_id as usize)
        }
    }
}

impl Default for CharacterMap {
    fn default() -> Self {
        Self { 
            maps_u8: std::array::from_fn( |_| Bitmap::new()),
            boundry_bytes: Bitmap::new()
        }
    }
}

#[derive(Debug, Default)]
pub struct PositionalBitmap {
    map: Vec<CharacterMap>,
}

impl PositionalBitmap {
    pub fn new() -> Self {
        Self {
            map: Vec::new(),
        }
    }

    #[inline(always)]
    pub fn add(&mut self, s: &str, id: u32) {
        let bytes = s.as_bytes();
        if bytes.len() > self.map.len() {
            self.expand_map(bytes.len());
        }
        let start = ((self.map.len() / 2) - (bytes.len() / 2)).saturating_sub(1);
        for i in 0..bytes.len() {
            let is_boundry = i == 0 || i == bytes.len() - 1;
            self.map[i + start].add(bytes[i], id, is_boundry)
        }
    }

    #[inline(always)]
    pub fn remove(&mut self, s: &str, id: u32) {
        let bytes = s.as_bytes();
        let start = ((self.map.len() / 2) - (bytes.len() / 2)).saturating_sub(1);
        for i in 0..bytes.len() {
            self.map[i + start].remove(bytes[i], id)
        }
    }

    #[inline(always)]
    fn get_all(&self) -> Bitmap {
        let mut res = Bitmap::new();
        for i in 0..(self.map.len() + 1) / 2 {
            res.or_inplace(self.map[i].get_boundry_bytes());
        }
        res
    }

    #[inline(always)]
    pub fn get_exact(&self, chars: &str) -> Bitmap {
        let mut res = Bitmap::new();
        let bytes = chars.as_bytes();
        if bytes.len() > self.map.len() {
            return res;
        }

        let start = ((self.map.len() / 2) - (bytes.len() / 2)).saturating_sub(1);

        res.or_inplace(self.map[start].contains(bytes[0]));
        res.and_inplace(self.map[start].get_boundry_bytes());

        for i in 1..bytes.len() {
            res.and_inplace(self.map[i + start].contains(bytes[i]));
        }

        res.and_inplace(self.map[start + bytes.len() - 1].get_boundry_bytes());
        res
    }

    #[inline(always)]
    pub fn starts_with(&self, chars: &str) -> Bitmap {
        let bytes = chars.as_bytes();
        if bytes.is_empty() {
            return self.get_all()
        }
        let mut res = Bitmap::new();
        let mut inner_res = Bitmap::new();
        let upper_bound = usize::min(self.map.len() / 2, self.map.len().saturating_sub(bytes.len()));
        for pos in 0..upper_bound {

            let byte_map = &self.map[pos];
            inner_res.clear();
            inner_res.or_inplace(byte_map.get_boundry_bytes());
            inner_res.and_inplace(byte_map.contains(bytes[0]));

            for inner_pos in 1..bytes.len() {
                inner_res.and_inplace(&self.map[pos + inner_pos].contains(bytes[inner_pos]));
            }

            res.or_inplace(&inner_res);
        }
        res
    }

    #[inline(always)]
    pub fn ends_with(&self, chars: &str) -> Bitmap {
        let bytes = chars.as_bytes();
        if bytes.is_empty() {
            return self.get_all()
        }
        let mut res = Bitmap::new();
        let mut inner_res = Bitmap::new();
        let max_byte_index = bytes.len().saturating_sub(1);

        let lower_bound = usize::max(max_byte_index, (self.map.len() / 2).saturating_sub(1));
        for pos in (lower_bound..self.map.len()).rev() {

            let byte_map = &self.map[pos];
            inner_res.clear();
            inner_res.or_inplace(byte_map.get_boundry_bytes());
            inner_res.and_inplace(byte_map.contains(bytes[max_byte_index]));

            for inner_pos in (0..max_byte_index).rev() {
                let map_position = pos + inner_pos - max_byte_index;
                inner_res.and_inplace(&self.map[map_position].contains(bytes[inner_pos]));
            }

            res.or_inplace(&inner_res);
        }
        res
    }

    #[inline(always)]
    pub fn contains(&self, chars: &str) -> Bitmap {
        let bytes = chars.as_bytes();
        if bytes.is_empty() {
            return self.get_all()
        }
        let mut res = Bitmap::new();
        let mut inner_res = Bitmap::new();

        let upper_bound = usize::min((self.map.len() / 2) + 1, self.map.len().saturating_sub(bytes.len().saturating_sub(1)));
        for pos in 0..upper_bound {

            let byte_map = &self.map[pos];
            inner_res.clear();
            inner_res.or_inplace(byte_map.contains(bytes[0]));

            for inner in 1..bytes.len() {
                inner_res.and_inplace(&self.map[pos + inner].contains(bytes[inner]));
            }

            res.or_inplace(&inner_res);
        }
        res
    }

    pub fn merge(&mut self, other: &PositionalBitmap) {
        if self.map.len() < other.map.len() {
            self.expand_map(other.map.len());
        }

        for (self_cm, other_cm) in self.map.iter_mut().zip(other.map.iter()) {
            self_cm.merge(other_cm);
        }
    }

    fn expand_map(&mut self, new_size: usize) {
        
        let ns = if new_size % 2 != 0 { new_size + 1 } else { new_size };
        let current_len = self.map.len();

        let extra = ns - current_len;
        let pad_front = extra / 2;

        self.map.resize(ns, CharacterMap::default());

        self.map.rotate_right(pad_front);
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_exact_basic_match() {
        let mut pb = PositionalBitmap::new();

        pb.add("hello", 1);
        pb.add("world", 2);

        let res = pb.get_exact("hello");
        assert!(res.contains(1));
        assert!(!res.contains(2));
        let res = pb.get_exact("hell");
        assert!(!res.contains(1));
        assert!(!res.contains(2));
        let res = pb.get_exact("hello again my old friend");
        assert!(!res.contains(1));
        assert!(!res.contains(2));
    }

    #[test]
    fn test_add_and_starts_with() {
        let mut pb = PositionalBitmap::new();
        pb.add("hello", 1);
        pb.add("hepium", 2);

        // Check starts_with
        let result = pb.starts_with("");
        assert!(result.contains(1), "ID 1 should end with ''");
        assert!(result.contains(2), "ID 2 should end with ''");
        assert!(!result.contains(3), "ID does not exist");
        let result = pb.starts_with("he");
        assert!(result.contains(1), "ID 1 should match 'he'");
        assert!(result.contains(2), "ID 2 should match 'he'");
        assert!(!result.contains(3), "ID 3 does not exist");
        let result = pb.starts_with("hell");
        assert!(result.contains(1), "ID 1 should match 'hell'");
        assert!(!result.contains(2), "ID 2 should not match 'hell'");
        let result = pb.starts_with("hello");
        assert!(result.contains(1), "ID 1 should match 'hello'");
        assert!(!result.contains(2), "ID 2 should not match 'hello'");
        let result = pb.starts_with("hello very long string");
        assert!(!result.contains(1), "ID 1 should not match 'hello'");
        assert!(!result.contains(2), "ID 2 should not match 'hello'");

        let mut pb = PositionalBitmap::new();
        pb.add("CA", 1);
        pb.add("DE", 2);

        let result = pb.starts_with("C");
        assert!(result.contains(1));
        assert!(!result.contains(2));
    }

    #[test]
    fn test_ends_with() {
        let mut pb = PositionalBitmap::new();
        pb.add("hello", 1);
        pb.add("yellow", 2);
        pb.add("help", 3);

        let result = pb.ends_with("");
        assert!(result.contains(1), "ID 1 should end with ''");
        assert!(result.contains(2), "ID 2 should end with ''");
        assert!(result.contains(3), "ID 3 should not match ''");
        let result = pb.ends_with("lo");
        assert!(result.contains(1), "ID 1 should end with 'lo'");
        assert!(!result.contains(2), "ID 2 should end with 'lo'");
        assert!(!result.contains(3), "ID 3 should not match 'lo'");
        let result = pb.ends_with("low");
        assert!(!result.contains(1), "ID 1 should end with 'low'");
        assert!(result.contains(2), "ID 2 should end with 'low'");
        assert!(!result.contains(3), "ID 3 should not match 'low'");
        let result = pb.ends_with("yellow");
        assert!(!result.contains(1), "ID 1 should end with 'yellow'");
        assert!(result.contains(2), "ID 2 should end with 'yellow'");
        assert!(!result.contains(3), "ID 3 should not match 'yellow'");
        let result = pb.ends_with("yellow with a long string attached");
        assert!(!result.contains(1), "ID 1 should not match 'yellow with a long string attached'");
        assert!(!result.contains(2), "ID 2 should not match 'yellow with a long string attached'");
        assert!(!result.contains(3), "ID 3 should not match 'yellow with a long string attached'");

        let mut pb = PositionalBitmap::new();
        pb.add("a", 1);
        pb.add("b", 2);
        pb.add("c", 3);

        let result = pb.ends_with("a");
        assert!(result.contains(1));
    }

    #[test]
    fn test_contains() {
        let mut pb = PositionalBitmap::new();
        pb.add("hello", 1);
        pb.add("yellow", 2);
        pb.add("help", 3);

        let result = pb.contains("");
        assert!(result.contains(1), "ID 1 contains ''");
        assert!(result.contains(2), "ID 2 contains ''");
        assert!(result.contains(3), "ID 3 contains ''");

        let result = pb.contains("ell");
        assert!(result.contains(1), "ID 1 contains 'ell'");
        assert!(result.contains(2), "ID 2 contains 'ell'");
        assert!(!result.contains(3), "ID 3 does not contain 'ell'");

        let result = pb.contains("lo");
        assert!(result.contains(1), "ID 1 contains 'lo'");
        assert!(result.contains(2), "ID 2 contains 'lo'");
        assert!(!result.contains(3), "ID 3 does not contain 'lo'");

        let result = pb.contains("yellow");
        assert!(!result.contains(1), "ID 1 contains 'yellow'");
        assert!(result.contains(2), "ID 2 does not contain 'yellow'");
        assert!(!result.contains(3), "ID 3 does not contain 'yellow'");

        let result = pb.contains("very long check for contains");
        assert!(!result.contains(1));
        assert!(!result.contains(2));
        assert!(!result.contains(3));
    }

    #[test]
    fn test_remove() {
        let mut pb = PositionalBitmap::new();
        pb.add("hello", 1);
        pb.add("helium", 2);

        // Remove "hello"
        pb.remove("hello", 1);

        let result = pb.starts_with("he");
        assert!(!result.contains(1), "ID 1 was removed");
        assert!(result.contains(2), "ID 2 should still match");
    }

    #[test]
    fn test_multiple_adds() {
        let mut pb = PositionalBitmap::new();
        pb.add("test", 1);
        pb.add("testing", 2);
        pb.add("tester", 3);

        // starts_with "test"
        let result = pb.starts_with("test");
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(result.contains(3));

        // ends_with "ing"
        let result2 = pb.ends_with("ing");
        assert!(result2.contains(2));
        assert!(!result2.contains(1));
        assert!(!result2.contains(3));

        // contains "est"
        let result3 = pb.contains("est");
        assert!(result3.contains(1));
        assert!(result3.contains(2));
        assert!(result3.contains(3));

    }
}
