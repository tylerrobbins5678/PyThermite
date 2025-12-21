mod interner;
mod immutable_interner;
mod mutable_interner;
mod interner_view;

pub use interner::StrInterner;
pub use interner_view::StrInternerView;
pub use interner::InternedStr;
pub use immutable_interner::ImmutableInterner;
pub use mutable_interner::MutableInterner;

pub static INTERNER: once_cell::sync::Lazy<StrInterner> = once_cell::sync::Lazy::new(|| {
    StrInterner::with_capacity(1024)
});