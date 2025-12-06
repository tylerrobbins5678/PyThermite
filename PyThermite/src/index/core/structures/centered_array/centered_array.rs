

#[derive(Clone, Debug)]
pub struct CenteredArray<const N: usize> {
    data: [u32; N],
    offset: usize,
    len: usize,
}

impl<const N: usize> CenteredArray<N> {
    pub fn new() -> Self {
        Self {
            data: [0u32; N],
            offset: 0,
            len: 0,
        }
    }

    pub fn from_sorted_slice(slice: &[u32]) -> Self {
        let mut arr = Self::new();
        arr.data[..slice.len()].copy_from_slice(slice);
        arr.recenter();
        arr
    }

    pub fn consuming_sorted_slice(slice: [u32; N]) -> Self {
        let mut arr = Self::new();
        arr.data = slice;
        arr.recenter();
        arr
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn contains(&self, value: &u32) -> bool {
        let slice = &self.data[self.offset .. self.offset + self.len];
        slice.binary_search(value).is_ok()
    }

    pub fn union_with<const M: usize>(&mut self, other: &CenteredArray<M>) {
        let a = &self.data[self.offset..self.offset + self.len];
        let b = &other.data[other.offset..other.offset + other.len];

        let a_len = a.len();
        let b_len = b.len();

        let mut i = 0;
        let mut j = 0;
        let mut len = 0;

        let mut out = [0u32; N];

        // Branchless merge loop
        while i < a_len && j < b_len {
            let av = unsafe { *a.get_unchecked(i) };
            let bv = unsafe { *b.get_unchecked(j) };

            let min = av.min(bv);
            out[len] = min;
            len += 1;

            // Advance pointers: i if av <= bv, j if bv <= av
            i += (av <= bv) as usize;
            j += (bv <= av) as usize;
        }

        // Copy remaining elements (only one will have leftovers)
        if i < a_len {
            let count = a_len - i;
            out[len..len + count].copy_from_slice(&a[i..]);
            len += count;
        }
        if j < b_len {
            let count = b_len - j;
            out[len..len + count].copy_from_slice(&b[j..]);
            len += count;
        }

        // Finalize
        self.data = out;
        self.offset = 0;
        self.len = len;
        self.recenter();
    }

    pub fn and_with<const M: usize>(&mut self, other: &CenteredArray<M>) {
        let len_a = self.len;
        let len_b = other.len;
        let ptr_a = unsafe { self.data.as_ptr().add(self.offset) };
        let ptr_b = unsafe { other.data.as_ptr().add(other.offset) };

        let mut i = 0;
        let mut j = 0;
        let mut len_out = 0;

        while i < len_a && j < len_b {
            let av = unsafe { *ptr_a.add(i) };
            let bv = unsafe { *ptr_b.add(j) };

            if av < bv {
                i += 1;
            } else if av > bv {
                j += 1;
            } else {
                self.data[len_out] = av; // safe write
                len_out += 1;
                i += 1;
                j += 1;
            }
        }

        self.len = len_out;
        self.offset = 0;
        self.recenter();
    }

    pub fn insert(&mut self, value: u32) {

        if self.len >= N {
            panic!("CenteredArray full");
        }

        if self.offset == 0 || self.offset + self.len >= N {
            self.recenter();
        }

        let slice = &self.data[self.offset .. self.offset + self.len];
        let idx = match slice.binary_search(&value) {
            Ok(_) => return, // already present
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

    pub fn remove(&mut self, value: &u32) -> bool {

        let slice = &self.data[self.offset .. self.offset + self.len];
        let idx = match slice.binary_search(&value) {
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
        unsafe {
            let ptr = self.data.as_mut_ptr();
            std::ptr::copy(ptr.add(start), ptr.add(start + amount), end - start);
        }
    }

    fn shift_left(&mut self, start: usize, end: usize, amount: usize) {
        unsafe {
            let ptr = self.data.as_mut_ptr();
            std::ptr::copy(ptr.add(start), ptr.add(start - amount), end - start);
        }
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

    pub fn iter(&self) -> &[u32] {
        &self.data[self.offset..self.offset + self.len]
    }
}


#[cfg(test)]
mod tests {
    use super::CenteredArray;

    #[test]
    fn test_insert_basic() {
        let mut arr: CenteredArray<8> = CenteredArray::new();
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
        let mut arr: CenteredArray<8> = CenteredArray::new();
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
        let mut arr: CenteredArray<8> = CenteredArray::new();
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
        let mut arr: CenteredArray<5> = CenteredArray::new();
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
        let mut arr: CenteredArray<4> = CenteredArray::new();
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
        let mut arr: CenteredArray<8> = CenteredArray::new();
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
        let mut arr: CenteredArray<10> = CenteredArray::new();

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

    #[test]
    fn union_empty_empty() {
        let mut a = CenteredArray::<8>::new();
        let b = CenteredArray::<8>::new();

        a.union_with(&b);
        assert_eq!(a.iter(), &[] as &[u32]);
    }

    #[test]
    fn union_empty_nonempty() {
        let mut a = CenteredArray::<8>::new();
        let mut b = CenteredArray::<8>::new();
        b.insert(3);
        b.insert(5);
        b.insert(7);

        a.union_with(&b);
        assert_eq!(a.iter(), &[3, 5, 7]);
    }

    #[test]
    fn union_no_overlap() {
        let mut a = CenteredArray::<8>::new();
        let mut b = CenteredArray::<8>::new();

        a.insert(1);
        a.insert(2);
        a.insert(3);

        b.insert(10);
        b.insert(11);
        b.insert(12);

        a.union_with(&b);
        assert_eq!(a.iter(), &[1, 2, 3, 10, 11, 12]);
    }

    #[test]
    fn union_with_overlap() {
        let mut a = CenteredArray::<8>::new();
        let mut b = CenteredArray::<8>::new();

        a.insert(1);
        a.insert(3);
        a.insert(5);

        b.insert(2);
        b.insert(3);
        b.insert(7);

        a.union_with(&b);
        assert_eq!(a.iter(), &[1, 2, 3, 5, 7]);
    }

    #[test]
    fn union_duplicate_values() {
        let mut a = CenteredArray::<8>::new();
        let mut b = CenteredArray::<8>::new();

        a.insert(2);
        a.insert(2);
        a.insert(3);

        b.insert(2);
        b.insert(4);

        a.union_with(&b);
        assert_eq!(a.iter(), &[2, 3, 4]);
    }

    #[test]
    fn union_offset_handling() {
        // force offset movement
        let mut a = CenteredArray::<8>::new();
        let mut b = CenteredArray::<8>::new();

        // Insert in a way that pushes offset right or left
        for x in [5, 10, 15] {
            a.insert(x);
        }
        for x in [1, 20] {
            b.insert(x);
        }

        a.union_with(&b);
        assert_eq!(a.iter(), &[1, 5, 10, 15, 20]);
    }

    #[test]
    fn union_symmetric() {
        let mut a = CenteredArray::<8>::new();
        let mut b = CenteredArray::<8>::new();

        for x in [1, 3, 5, 7] {
            a.insert(x);
        }
        for x in [3, 4, 7, 8] {
            b.insert(x);
        }

        let mut u1 = a.clone();
        u1.union_with(&b);

        let mut u2 = b.clone();
        u2.union_with(&a);

        assert_eq!(u1.iter(), u2.iter());
        assert_eq!(u1.iter(), &[1, 3, 4, 5, 7, 8]);
    }
    
    #[test]
    fn test_union_simple() {
        let mut a = CenteredArray::<16>::new();
        a.insert(1);
        a.insert(3);
        a.insert(5);

        let mut b = CenteredArray::<16>::new();
        b.insert(2);
        b.insert(4);
        b.insert(6);

        a.union_with(&b);

        assert_eq!(a.iter(), &[1u32,2,3,4,5,6] );

        // offset should be centered and correct
        assert_eq!(a.len(), 6);
        let expected_offset = (16 - a.len()) / 2;
        assert_eq!(a.offset, expected_offset);
        assert_eq!(&a.data[a.offset..a.offset + a.len], a.iter());
    }

    #[test]
    fn test_union_with_duplicates() {
        let mut a = CenteredArray::<16>::new();
        a.insert(1);
        a.insert(3);
        a.insert(5);

        let mut b = CenteredArray::<16>::new();
        b.insert(3);
        b.insert(4);
        b.insert(5);

        a.union_with(&b);

        assert_eq!(a.iter(), &[1u32,3,4,5]);
        let expected_offset = (16 - a.len()) / 2;
        assert_eq!(a.offset, expected_offset);
    }

    #[test]
    fn test_union_empty_with_nonempty() {
        let mut a = CenteredArray::<16>::new();
        let mut b = CenteredArray::<16>::new();

        b.insert(10);
        b.insert(20);

        a.union_with(&b);

        assert_eq!(a.iter(), &[10u32,20]);

        let expected_offset = (16 - a.len()) / 2;
        assert_eq!(a.offset, expected_offset);
    }

    #[test]
    fn test_union_both_empty() {
        let mut a = CenteredArray::<16>::new();
        let b = CenteredArray::<16>::new();

        a.union_with(&b);

        assert_eq!(a.iter(), &[] as &[u32]);
        assert_eq!(a.len(), 0);

        // offset can be anything valid, but recenter() will move it to midpoint
        assert_eq!(a.offset, (16 - 0) / 2);
    }


    #[test]
    fn test_and_with_disjoint() {
        let mut a: CenteredArray<8> = CenteredArray::new();
        let mut b: CenteredArray<8> = CenteredArray::new();

        for x in &[1, 3, 5, 7] {
            a.insert(*x);
        }
        for x in &[2, 4, 6, 8] {
            b.insert(*x);
        }

        a.and_with(&b);
        assert_eq!(a.len, 0);
        assert!(a.iter().is_empty());
    }

    #[test]
    fn test_and_with_partial_overlap() {
        let mut a: CenteredArray<8> = CenteredArray::new();
        let mut b: CenteredArray<8> = CenteredArray::new();

        for x in &[1, 3, 5, 7] {
            a.insert(*x);
        }
        for x in &[3, 4, 5, 6] {
            b.insert(*x);
        }

        a.and_with(&b);
        assert_eq!(a.len, 2);
        assert_eq!(a.iter(), &[3, 5]);
    }

    #[test]
    fn test_and_with_full_overlap() {
        let mut a: CenteredArray<8> = CenteredArray::new();
        let mut b: CenteredArray<8> = CenteredArray::new();

        for x in &[1, 2, 3, 4] {
            a.insert(*x);
            b.insert(*x);
        }

        a.and_with(&b);
        assert_eq!(a.len, 4);
        assert_eq!(a.iter(), &[1, 2, 3, 4]);
    }

    #[test]
    fn test_and_with_empty() {
        let mut a: CenteredArray<8> = CenteredArray::new();
        let b: CenteredArray<8> = CenteredArray::new();

        a.and_with(&b);
        assert_eq!(a.len, 0);
        assert!(a.iter().is_empty());
    }

    #[test]
    fn test_and_with_single_element() {
        let mut a: CenteredArray<8> = CenteredArray::new();
        let mut b: CenteredArray<8> = CenteredArray::new();

        a.insert(42);
        b.insert(42);

        a.and_with(&b);
        assert_eq!(a.len, 1);
        assert_eq!(a.iter(), &[42]);

        let mut c: CenteredArray<8> = CenteredArray::new();
        c.insert(1);
        a.and_with(&c);
        assert_eq!(a.len, 0);
        assert!(a.iter().is_empty());
    }
}
