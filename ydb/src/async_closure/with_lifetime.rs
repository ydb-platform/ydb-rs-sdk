//! To make our custom asynchronous closures general
//! enough to be reused in different parts of our
//! API, we needed to make some traits and types generic
//! over a lifetime-parametrized type. This is impossible in
//! Rust, so we came up to [`WithLifetime`] trait that
//! allows to express lifetime-parametrized types
//! via GATs.
//!
//! There are some pre-defined implementations
//! of [`WithLifetime`] to express owned values,
//! mutable and immutable references, but if you want
//! to use it with some non-trivial lifetime-parametrized
//! types we don't support out of the box, you can
//! implement the trait for your own types.
//!
//! - [`Owned`] type can be used to define lifetime-parametrized
//!   type that doesn't depend on its parameter.
//! - [`Ref`] type can be used to define reference type
//!   parametrized by its lifetime.
//! - [`Mut`] type can be used to define mutable reference
//!   type paramethrized by its lifetime.
//! - Tuple of [`WithLifetime`] types is [`WithLifetime`] type
//!   representing tuple.

/// The trait that allows to express lifetime-parametrized
/// types as plain types.
///
/// A type implementing this trait is a marker type
/// that defines lifetime-parametrized type as its
/// associated type [`Self::Type`].
pub trait WithLifetime {
    /// Lifetime-parametrized type itself.
    ///
    /// Should live at least as long
    /// as its lifetime parameter.
    ///
    /// Valid lifetime parameter values
    /// for this type can be narrowed by
    /// making [`Self`] have non-static lifetime,
    /// due to `Self: 'a` constraint.
    type Type<'a>: 'a
    where
        Self: 'a;
}

/// Marker type that represents
/// lifetime-independent type.
///
/// Its associated lifetime-parametrized
/// type is `T` for all lifetimes.
pub struct OwnedWithLifetime<T>(T);

impl<T> WithLifetime for OwnedWithLifetime<T> {
    type Type<'a>
        = T
    where
        Self: 'a;
}

/// Marker type that represents
/// reference type parametrized
/// by its lifetime.
pub struct RefWithLifetime<T>(T);

impl<T> WithLifetime for RefWithLifetime<T> {
    type Type<'a>
        = &'a T
    where
        Self: 'a;
}

/// Marker type that represents
/// mutable reference type
/// parametrized by its lifetime.
pub struct MutWithLifetime<T>(T);

impl<T> WithLifetime for MutWithLifetime<T> {
    type Type<'a>
        = &'a mut T
    where
        Self: 'a;
}

macro_rules! impl_tuple {
    ($($param:ident,)*) => {
        #[allow(unused_parens)]
        impl<$($param : WithLifetime),*> WithLifetime for ($($param,)*) {
            type Type<'a>
                = ($($param::Type<'a>,)*)
            where
                Self: 'a;
        }
    };
}

macro_rules! impl_tuples {
    () => {
        impl_tuple!();
    };
    ($param:ident, $($tt:tt)*) => {
        impl_tuple!($param, $($tt)*);
        impl_tuples!($($tt)*);
    };
}

impl_tuples!(A, B, C, D, E, F, G, H, I, J, K, L,);
