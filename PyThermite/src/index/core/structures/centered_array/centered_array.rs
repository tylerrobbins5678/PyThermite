

pub struct CenteredArray<T, const N: usize> {
    data: [T; N],
    offset: usize,
    len: usize,
}

impl<T: Default + Copy + Ord, const N: usize> CenteredArray<T, N> {
    pub fn new() -> Self {
        Self {
            data: [T::default(); N],
            offset: 0,
            len: 0,
        }
    }

    pub fn insert(&mut self, value: T) {

        if self.len >= N {
            panic!("CenteredArray full");
        }

        if self.offset == 0 || self.offset + self.len >= N {
            self.recenter();
        }

        let idx = match self.iter().binary_search(&value) {
            Ok(_) => return,  
            Err(i) => i,
        };

        if self.offset > 0 && (idx < self.len / 2) {
            self.shift_left(self.offset, self.offset + idx, 1);
            self.offset -= 1
        } else {
            self.shift_right(self.offset + idx, self.offset + self.len, 1);
        }

        self.data[self.offset + idx] = value;
        self.len += 1;
    }

    pub fn remove(&mut self, value: &T) -> bool {

        let idx = match self.iter().binary_search(value) {
            Ok(i) => i,
            Err(_) => return false, // not found
        };

        let remove_pos = self.offset + idx;

        if remove_pos < self.len / 2 {
            self.shift_right(self.offset, remove_pos, 1);
            self.offset += 1;
        } else {
            self.shift_left(remove_pos + 1, self.offset + self.len, 1);
        }

        self.len -= 1;
        true
    }

    fn shift_right(&mut self, start: usize, end: usize, amount: usize) {
        self.data.copy_within(start..end, start + amount);
    }

    fn shift_left(&mut self, start: usize, end: usize, amount: usize) {
        self.data.copy_within(start..end, start - amount);
    }

    fn recenter(&mut self) {
        let desired_offset = (N - self.len) / 2;

        if desired_offset == self.offset {
            return;
        }

        if desired_offset > self.offset {
            self.shift_right(self.offset, self.offset + self.len, desired_offset - self.offset);
        } else {
            self.shift_left(self.offset, self.offset + self.len, self.offset - desired_offset);
        }

        self.offset = desired_offset;
    }

    pub fn iter(&self) -> &[T] {
        &self.data[self.offset..self.offset + self.len]
    }
}


#[cfg(test)]
mod tests {
    use super::CenteredArray;

    #[test]
    fn test_insert_basic() {
        let mut arr: CenteredArray<u32, 8> = CenteredArray::new();
        arr.insert(5);
        arr.insert(2);
        arr.insert(8);
        arr.insert(3);

        let result: Vec<u32> = arr.iter().to_vec();
        assert_eq!(result, vec![2, 3, 5, 8]);
        assert_eq!(arr.len, 4);
    }

    #[test]
    fn test_insert_duplicates() {
        let mut arr: CenteredArray<u32, 8> = CenteredArray::new();
        arr.insert(4);
        arr.insert(4); // duplicate
        arr.insert(2);
        arr.insert(4); // duplicate

        let result: Vec<u32> = arr.iter().to_vec();
        assert_eq!(result, vec![2, 4]);
        assert_eq!(arr.len, 2);
    }

    #[test]
    fn test_remove_basic() {
        let mut arr: CenteredArray<u32, 8> = CenteredArray::new();
        arr.insert(1);
        arr.insert(3);
        arr.insert(2);

        assert!(arr.remove(&2));
        assert!(!arr.remove(&2)); // already removed

        let result: Vec<u32> = arr.iter().to_vec();
        assert_eq!(result, vec![1, 3]);
        assert_eq!(arr.len, 2);
    }

    #[test]
    fn test_remove_first_and_last() {
        let mut arr: CenteredArray<u32, 5> = CenteredArray::new();
        arr.insert(10);
        arr.insert(20);
        arr.insert(30);

        assert!(arr.remove(&10)); // remove first
        assert!(arr.remove(&30)); // remove last

        let result: Vec<u32> = arr.iter().to_vec();
        assert_eq!(result, vec![20]);
        assert_eq!(arr.len, 1);
    }

    #[test]
    fn test_insert_until_full() {
        let mut arr: CenteredArray<u32, 4> = CenteredArray::new();
        arr.insert(1);
        arr.insert(2);
        arr.insert(3);
        arr.insert(4);

        let result: Vec<u32> = arr.iter().to_vec();
        assert_eq!(result, vec![1, 2, 3, 4]);
        assert_eq!(arr.len, 4);

        // Inserting when full should panic
        let result = std::panic::catch_unwind(move || {
            let mut arr = arr; // move ownership into closure
            arr.insert(5);
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_shift_behavior() {
        let mut arr: CenteredArray<u32, 8> = CenteredArray::new();
        arr.insert(3);
        arr.insert(1);
        arr.insert(5);
        arr.insert(2);

        // Remove middle value to test shift left
        arr.remove(&3);

        let result: Vec<u32> = arr.iter().to_vec();
        assert_eq!(result, vec![1, 2, 5]);

        // Insert a value near left to test shift right
        arr.insert(0);
        let result: Vec<u32> = arr.iter().to_vec();
        assert_eq!(result, vec![0, 1, 2, 5]);
    }

    #[test]
    fn test_recenter_after_many_inserts_and_removes() {
        let mut arr: CenteredArray<u32, 10> = CenteredArray::new();

        for i in 0..8 {
            arr.insert(i);
        }

        for i in (0..4).rev() {
            arr.remove(&i);
        }

        let result: Vec<u32> = arr.iter().to_vec();
        assert_eq!(result, vec![4,5,6,7]);
        assert_eq!(arr.len, 4);
    }
}
