//! Type erasure for asynchronous closures.
//!
//! Rust 1.85 introduced [`AsyncFn`], [`AsyncFnMut`] and [`AsyncFnOnce`]
//! traits that [approximate asynchronous function types
//! better](https://smallcultfollowing.com/babysteps/blog/2023/05/09/giving-lending-and-async-closures/)
//! that just [`Fn`], [`FnMut`] and [`FnOnce`] returning a future.
//! The main thing that these traits allow to express is asynchronous
//! closures borrowing from themselves into the returned future.
//!
//! Unfortunately, these traits come with a lot of serious limitations,
//! such as inability to access their associated future type in stable
//! Rust and rustc's inability to correctly infer some lifetimes and
//! trait bounds for asynchronous closures in some cases due to
//! [compiler bugs](https://github.com/rust-lang/rust/issues/110338).
//! This greatly reduces usability of borrowing asynchronous closures.
//!
//! That's why we implemented a temporal solution that can be used
//! to replicate asynchronous closures behavior without relying on
//! unreliable `AsyncFn*` traits, much like [`async_trait`](https://docs.rs/async-trait/latest/async_trait/),
//! allows to replicate traits with asynchronous methods without
//! using anonymous GATs and losing `dyn`-safety,
//! `Send` guarantee and concrete future types.
//! It should be replaced with mere
//! asynchronous closures when they're stable enough to express
//! all lifetime and trait relations we need without breaking the compiler.

use futures_util::future::BoxFuture;

use crate::async_closure::with_lifetime::WithLifetime;

pub mod with_lifetime;

/// [`AsyncFnMut`] equivalent with type-erased future
/// that can be implemented for different types and
/// allows to avoid problems with lifetime binders
/// and asynchronous closures and unstable parts
/// of `AsyncFn*` traits.
///
/// Its future type is required to be [`Send`].
pub trait AsyncFnMut<Args: WithLifetime>: Send {
    type Output;

    fn call<'a>(&'a mut self, args: Args::Type<'a>) -> BoxFuture<'a, Self::Output>;
}

/// Type-erased asynchronous function.
pub struct DynAsyncFnMut<'a, Args: WithLifetime, Output>(
    Box<dyn AsyncFnMut<Args, Output = Output> + 'a>,
);

impl<'c, Args: WithLifetime, Output> AsyncFnMut<Args> for DynAsyncFnMut<'c, Args, Output> {
    type Output = Output;

    fn call<'a>(&'a mut self, args: <Args as WithLifetime>::Type<'a>) -> BoxFuture<'a, Output> {
        self.0.call(args)
    }
}

// This function is implementation detail of [`crate::closure`] macro.
#[doc(hidden)]
pub fn __make_closure<'c, Args, Output, C, F>(
    context: C,
    function: F,
) -> DynAsyncFnMut<'c, Args, Output>
where
    Args: WithLifetime,
    C: Send + 'c,
    F: for<'a> FnMut(&'a mut C, Args::Type<'a>) -> BoxFuture<'a, Output> + Send + 'static,
{
    struct AsyncMutClosure<C, F> {
        context: C,
        function: F,
    }

    impl<Args, Output, F, C> AsyncFnMut<Args> for AsyncMutClosure<C, F>
    where
        Args: WithLifetime,
        C: Send,
        F: for<'a> FnMut(&'a mut C, Args::Type<'a>) -> BoxFuture<'a, Output> + Send,
    {
        type Output = Output;

        fn call<'a>(&'a mut self, args: <Args as WithLifetime>::Type<'a>) -> BoxFuture<'a, Output> {
            (self.function)(&mut self.context, args)
        }
    }

    DynAsyncFnMut(Box::new(AsyncMutClosure { context, function }))
}

/// Constructs a [`DynAsyncFnMut`] from a closure
/// with explicit context capturing. Think of it
/// as an [`async_trait`](https://docs.rs/async-trait/latest/async_trait/)
/// macro, but for closures instead of traits.
///
/// # Syntax
///
/// The macro input consists of two comma-separated parts. The first one
/// is an explicit capture list. It's optional and can be omitted if
/// your closure doesn't capture anything. The second one is an asynchronous closure
/// that doesn't capture anything directly without relying on the explicit
/// capture list.
///
/// An explicit capture list is a comma-separated list of captures
/// put in square brackets.
///
/// Each capture is a variable name, optionally with `&` or `&mut` prefix.
/// It defines a variable captured from outer scope. Variables are captured
/// by reference if defined with `&` prefix, by mutable reference if defined
/// with `&mut` prefix and by value if defined without prefix.
///
/// Alternatively, a capture can be an assignment instead of a variable name,
/// which allows to capture expressions as variables. Note that
/// this syntax requires the assigned expression to have type
/// corresponding to the used prefix. One of use cases for
/// this syntax is to help capturing `self`.
///
/// # Usage example
///
/// ```
/// # use ydb::{
/// #     closure,
/// #     async_closure::{
/// #         AsyncFnMut,
/// #         with_lifetime::Mut,
/// #     }
/// # };
/// # struct Action;
/// #
/// # impl Action {
/// #     fn get() -> Self { Self }
/// #     async fn perform(&mut self, logs: &mut Vec<String>) -> Result<(), ()> {
/// #         Ok(())
/// #     }
/// # }
/// #
/// async fn retry<F: AsyncFnMut<Mut<Action>, Output = bool>>(mut attempt: F) {
///     let mut action = Action::get();
///
///     while !attempt.call(&mut action).await {
///         println!("Failed, trying again...");
///     }
/// }
///
/// #[tokio::main]
/// async fn main() {
///     let mut logs = vec![];
///     retry(closure!(
///         [&mut logs],
///         async |action: &mut Action| {
///             action.perform(logs).await.is_ok()
///         }
///     )).await;
/// }
/// ```
#[macro_export]
macro_rules! closure {
    ([$($tt:tt)*], $body:expr) => {
        $crate::async_closure::__make_closure(
            $crate::__closure_make_tuple!($($tt)*),
            move |ctx, args| {
                $crate::__closure_destruct_tuple!(ctx => $($tt)*);
                ::std::boxed::Box::pin(async move {($body)(args).await})
            }
        )
    };
    ($body:expr) => {
        $crate::closure!([], $body)
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __closure_make_tuple {
    ($(,)?) => {
        ()
    };
    ($var:ident $(, $( $rest:tt )*)?) => {
        ($var, $crate::__closure_make_tuple!($($($rest)*)?))
    };
    (&mut $var:ident $(, $( $rest:tt )*)?) => {
        (&mut $var, $crate::__closure_make_tuple!($($($rest)*)?))
    };
    (& $var:ident $(, $( $rest:tt )*)?) => {
        (& $var, $crate::__closure_make_tuple!($($($rest)*)?))
    };
    ($alias:ident = $expr:expr $(, $( $rest:tt )*)?) => {
        ($expr, $crate::__closure_make_tuple!($($($rest)*)?))
    };
    (&mut $alias:ident = $expr:expr $(, $( $rest:tt )*)?) => {
        ($expr, $crate::__closure_make_tuple!($($($rest)*)?))
    };
    (& $alias:ident = $expr:expr $(, $( $rest:tt )*)?) => {
        ($expr, $crate::__closure_make_tuple!($($($rest)*)?))
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __closure_destruct_tuple {
    ($ctx:ident => $(,)?) => {
        let () = $ctx;
    };
    ($ctx:ident => $var:ident $(, $($rest:tt)*)?) => {
        let $var = &mut $ctx.0;
        let $ctx = &mut $ctx.1;
        $crate::__closure_destruct_tuple!($ctx => $($($rest)*)?);
    };
    ($ctx:ident => &mut $var:ident $(, $($rest:tt)*)?) => {
        let $var = &mut *$ctx.0;
        let $ctx = &mut $ctx.1;
        $crate::__closure_destruct_tuple!($ctx => $($($rest)*)?);
    };
    ($ctx:ident => & $var:ident $(, $($rest:tt)*)?) => {
        let $var = & *$ctx.0;
        let $ctx = &mut $ctx.1;
        $crate::__closure_destruct_tuple!($ctx => $($($rest)*)?);
    };
    ($ctx:ident => $alias:ident = $expr:expr $(, $($rest:tt)*)?) => {
        let $alias = &mut $ctx.0;
        let $ctx = &mut $ctx.1;
        $crate::__closure_destruct_tuple!($ctx => $($($rest)*)?);
    };
    ($ctx:ident => &mut $alias:ident = $expr:expr $(, $($rest:tt)*)?) => {
        let $alias = &mut *$ctx.0;
        let $ctx = &mut $ctx.1;
        $crate::__closure_destruct_tuple!($ctx => $($($rest)*)?);
    };
    ($ctx:ident => & $alias:ident = $expr:expr $(, $($rest:tt)*)?) => {
        let $alias = & *$ctx.0;
        let $ctx = &mut $ctx.1;
        $crate::__closure_destruct_tuple!($ctx => $($($rest)*)?);
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::async_closure::with_lifetime::Mut;

    async fn call_in_loop<F: AsyncFnMut<Mut<i32>, Output = bool>>(mut f: F) {
        let mut x = 10;
        while !f.call(&mut x).await {
            x += 1;
        }
    }

    #[tokio::test]
    async fn test_closure_macro() {
        call_in_loop(closure!(async |x: &mut i32| {
            *x -= 2;
            *x == 0
        }))
        .await;
        let mut d = 1;
        call_in_loop(closure!([&mut d], async |x: &mut i32| {
            *x -= *d;
            *d += 1;
            *x < 0
        }))
        .await;
        assert!(d > 1);
        call_in_loop(closure!([&d], async |x: &mut i32| {
            *x -= d;
            *x < 0
        }))
        .await;
        call_in_loop(closure!([d], async |x: &mut i32| {
            *x -= *d;
            *x < 0
        }))
        .await;
        let mut f = 0;
        call_in_loop(closure!([&mut d, &mut f], async |x: &mut i32| {
            *d += *f;
            *f += 1;
            *x -= *d;
            *x < 0
        }))
        .await;
        call_in_loop(closure!([&mut d, &mut f,], async |x: &mut i32| {
            *d += *f;
            *f += 1;
            *x -= *d;
            *x < 0
        }))
        .await;
        call_in_loop(closure!(
            [&mut d = &mut f, &mut f = &mut d,],
            async |x: &mut i32| {
                *d += *f;
                *f += 1;
                *x -= *d;
                *x < 0
            }
        ))
        .await;
    }
}
