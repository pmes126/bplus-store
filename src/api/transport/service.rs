use std::ops::Bound;
use std::pin::Pin;
use bytes::Bytes;
use futures_core::Stream;

impl<'a> KeyRange<'a> {
    pub fn unbounded() -> Self { Self { start: Bound::Unbounded, end: Bound::Unbounded } }
    pub fn from_start(start: &'a [u8]) -> Self {
        Self { start: Bound::Included(start), end: Bound::Unbounded }
    }
    pub fn between(start: Bound<&'a [u8]>, end: Bound<&'a [u8]>) -> Self { Self { start, end } }
}
