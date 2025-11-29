use std::cmp::{Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Key {
    Int(i64),
    FloatOrdered(ordered_float::OrderedFloat<f64>),
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Key::Int(a), Key::Int(b)) => a.cmp(b),
            (Key::FloatOrdered(a), Key::FloatOrdered(b)) => a.cmp(b),
            (Key::Int(a), Key::FloatOrdered(b)) => (*a as f64).partial_cmp(&b.0).unwrap_or(Ordering::Equal),
            (Key::FloatOrdered(a), Key::Int(b)) => a.0.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal),
        }
    }
}