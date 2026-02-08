use std::array;

use croaring::Bitmap;


#[derive(Debug, Clone)]
pub struct NibbleIndexU32  {
    data: [Bitmap; 16],
}

impl NibbleIndexU32  {
    pub fn new() -> Self {
        Self {
            data: array::from_fn(|_| Bitmap::new()),
        }
    }

    #[inline(always)]
    unsafe fn add(&mut self, nibble: usize, id: u32) {
        let bitmap = self.data.get_unchecked_mut(nibble);
        bitmap.add(id);
    }

    #[inline(always)]
    unsafe fn remove(&mut self, nibble: usize, id: u32) {
        let bitmap = self.data.get_unchecked_mut(nibble);
        bitmap.remove(id);
    }

    #[inline(always)]
    fn all_into(&self, into: &mut Bitmap) {
        for bm in self.data.iter() {
            into.or_inplace(bm);
        }
    }

    #[inline(always)]
    unsafe fn and_inplace(&self, nibble: usize, bm: &mut Bitmap) {
        bm.and_inplace(self.data.get_unchecked(nibble));
    }

    #[inline(always)]
    fn contains(&self, id: u32) -> bool {
        for bm in self.data.iter() {
            if bm.contains(id) {
                return true;
            }
        }
        false
    }
}

impl Default for NibbleIndexU32  {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Default)]
pub struct M2MU32 {
    forward_maps: [NibbleIndexU32; 8],
    reverse_maps: [NibbleIndexU32; 8],
}

impl M2MU32 {
    pub fn new() -> Self {
        Self {
            forward_maps: array::from_fn(|_| NibbleIndexU32  {
                data: Default::default(),
            }),
            reverse_maps: array::from_fn(|_| NibbleIndexU32  {
                data: Default::default(),
            }),
        }
    }

    #[inline(always)]
    pub fn contains(&self, id: u32) -> bool {
        self.forward_maps[0].contains(id)
    }

    #[inline(always)]
    fn all_forward(&self) -> Bitmap {
        let mut res = Bitmap::new();
        for fm in self.forward_maps.iter() {
            fm.all_into(&mut res);
        }
        res
    }

    #[inline(always)]
    fn all_reverse(&self) -> Bitmap {
        let mut res = Bitmap::new();
        for rm in self.reverse_maps.iter() {
            rm.all_into(&mut res);
        }
        res
    }

    #[inline(always)]
    pub fn add(&mut self, forward: u32, reverse: u32) {
        for i in 0..8 {
            let forward_n = ((forward >> (i * 4)) & 0xF) as usize;
            let reverse_n = ((reverse >> (i * 4)) & 0xF) as usize;

            unsafe {
                self.forward_maps.get_unchecked_mut(i).add(forward_n, reverse);
                self.reverse_maps.get_unchecked_mut(i).add(reverse_n, forward);
            }
        }
    }

    pub fn remove(&mut self, forward: u32, reverse: u32) {
        for i in 0..8 {
            let forward_n = ((forward >> (i * 4)) & 0xF) as usize;
            let reverse_n = ((reverse >> (i * 4)) & 0xF) as usize;

            unsafe {
                self.forward_maps.get_unchecked_mut(i).remove(forward_n, reverse);
                self.reverse_maps.get_unchecked_mut(i).remove(reverse_n, forward);
            }
        }
    }

    #[inline(always)]
    pub fn get_for_forward(&self, forward: u32) -> Bitmap {
        let mut res = self.all_forward();
        for i in 0..8 {
            let forward_n = ((forward >> (i * 4)) & 0xF) as usize;
            unsafe {
                self.forward_maps[i].and_inplace(forward_n, &mut res);
            }
        }
        res
    }

    #[inline(always)]
    pub fn get_for_reverse(&self, reverse: u32) -> Bitmap {
        let mut res = self.all_reverse();
        for i in 0..8 {
            let reverse_n = ((reverse >> (i * 4)) & 0xF) as usize;
            unsafe {
                self.reverse_maps[i].and_inplace(reverse_n, &mut res);
            }
        }
        res
    }

    // bitmap ops
    #[inline]
    pub fn get_for_forward_many(&self, forward_bitmap: &Bitmap) -> Bitmap {
        let all = self.all_forward();
        let mut res = Bitmap::new();
        for forward in forward_bitmap.iter() {
            let mut tmp = all.clone();
            for i in 0..8 {
                let forward_n = ((forward >> (i * 4)) & 0xF) as usize;
                unsafe {
                    self.forward_maps[i].and_inplace(forward_n, &mut tmp);
                }
            }
            res.or_inplace(&tmp);
        }
        res
    }

    #[inline]
    pub fn get_for_reverse_many(&self, reverse_bitmap: &Bitmap) -> Bitmap {
        let all = self.all_reverse();
        let mut res = Bitmap::new();
        for reverse in reverse_bitmap.iter() {
            let mut tmp = all.clone();
            for i in 0..8 {
                let reverse_n = ((reverse >> (i * 4)) & 0xF) as usize;
                unsafe {
                    self.reverse_maps[i].and_inplace(reverse_n, &mut tmp);
                }
            }
            res.or_inplace(&tmp);
        }
        res
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use croaring::Bitmap;

    fn bm(vals: &[u32]) -> Bitmap {
        let mut b = Bitmap::new();
        for &v in vals {
            b.add(v);
        }
        b
    }

    #[test]
    fn add_and_get_single_forward() {
        let mut m = M2MU32::new();

        m.add(1, 10);
        m.add(1, 11);
        m.add(1, 12);

        let res = m.get_for_forward(1);

        assert!(res.contains(10));
        assert!(res.contains(11));
        assert!(res.contains(12));
        assert_eq!(res.cardinality(), 3);
    }

    #[test]
    fn add_and_get_single_reverse() {
        let mut m = M2MU32::new();

        m.add(1, 10);
        m.add(2, 10);
        m.add(3, 10);

        let res = m.get_for_reverse(10);

        assert!(res.contains(1));
        assert!(res.contains(2));
        assert!(res.contains(3));
        assert_eq!(res.cardinality(), 3);
    }

    #[test]
    fn forward_and_reverse_are_symmetric() {
        let mut m = M2MU32::new();

        m.add(42, 99);

        let f = m.get_for_forward(42);
        let r = m.get_for_reverse(99);

        assert!(f.contains(99));
        assert!(r.contains(42));
        assert_eq!(f.cardinality(), 1);
        assert_eq!(r.cardinality(), 1);
    }

    #[test]
    fn remove_clears_both_directions() {
        let mut m = M2MU32::new();

        m.add(5, 50);
        m.remove(5, 50);

        let f = m.get_for_forward(5);
        let r = m.get_for_reverse(50);

        assert!(f.is_empty());
        assert!(r.is_empty());
    }

    #[test]
    fn multiple_relations_do_not_leak() {
        let mut m = M2MU32::new();

        m.add(1, 10);
        m.add(2, 20);
        m.add(3, 30);

        let f = m.get_for_forward(1);
        assert!(f.contains(10));
        assert_eq!(f.cardinality(), 1);

        let r = m.get_for_reverse(20);
        assert!(r.contains(2));
        assert_eq!(r.cardinality(), 1);
    }

    #[test]
    fn get_for_forward_many_intersection() {
        let mut m = M2MU32::new();

        // shared reverse
        m.add(1, 100);
        m.add(2, 100);
        m.add(3, 100);

        // unique reverse
        m.add(1, 200);
        m.add(2, 300);

        let forwards = bm(&[1, 2, 3]);
        let res = m.get_for_forward_many(&forwards);

        assert!(res.contains(100));
        assert!(res.contains(200));
        assert!(res.contains(300));
        assert_eq!(res.cardinality(), 3);
    }

    #[test]
    fn get_for_reverse_many_intersection() {
        let mut m = M2MU32::new();

        // shared forward
        m.add(100, 1);
        m.add(100, 2);
        m.add(100, 3);

        // unique forward
        m.add(200, 1);
        m.add(300, 2);

        let reverses = bm(&[1, 2, 3]);
        let res = m.get_for_reverse_many(&reverses);

        assert!(res.contains(100));
        assert!(res.contains(200));
        assert!(res.contains(300));
        assert_eq!(res.cardinality(), 3);
    }

    #[test]
    fn empty_queries_return_empty() {
        let m = M2MU32::new();

        let res_f = m.get_for_forward(123);
        let res_r = m.get_for_reverse(456);

        assert!(res_f.is_empty());
        assert!(res_r.is_empty());
    }

    #[test]
    fn remove_one_relation_does_not_affect_others() {
        let mut m = M2MU32::new();

        m.add(1, 10);
        m.add(1, 11);
        m.remove(1, 10);

        let res = m.get_for_forward(1);

        assert!(!res.contains(10));
        assert!(res.contains(11));
        assert_eq!(res.cardinality(), 1);
    }
}