//! Inline-packed metadata publish word for the lock-free commit path.
//!
//! The committed tree state that must be published *atomically* on every commit is
//! just `(root_node_id, height, txn_id)`. Those fit in a single 128-bit word held in an
//! `AtomicU128`:
//!
//! - readers do one atomic load and unpack — no pointer, no dereference, no reclamation;
//! - writers `compare_exchange` the word, and the monotonic `txn_id` makes the CAS
//!   **ABA-safe by construction** (a stale base word can never equal the current word).
//!
//! Layout (LSB-first):
//!
//! ```text
//! bits [0   .. 64)  root_node_id : 64 bits  (full u64)
//! bits [64  .. 80)  height       : 16 bits  (real tree height <= ~10; max 65535)
//! bits [80  .. 128) txn_id       : 48 bits  (~2.8e14 commits; ~8900 yrs @ 1k commits/s)
//! ```
//!
//! The constant fields (`id`, `order`), the durability-only `checksum`, and the
//! approximate `size` live outside the word — see `docs/design/inline-metadata-cas.md`.

use super::node::NodeId;

const HEIGHT_SHIFT: u32 = 64;
const TXN_SHIFT: u32 = 80;

const HEIGHT_BITS: u32 = 16;
const TXN_BITS: u32 = 48;

const HEIGHT_MASK: u64 = (1u64 << HEIGHT_BITS) - 1; // 0xFFFF
/// Largest representable `txn_id` (2^48 - 1).
pub const TXN_MAX: u64 = (1u64 << TXN_BITS) - 1;
/// Largest representable `height` (65535).
pub const HEIGHT_MAX: u64 = HEIGHT_MASK;

/// A `(root_node_id, height, txn_id)` triple packed into a single 128-bit word.
///
/// `Copy` and cheap: it is just a `u128` newtype. Pack once, store the raw word in the
/// atomic, and unpack after loading.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PackedMeta(u128);

impl PackedMeta {
    /// Packs the publish triple.
    ///
    /// # Panics (debug only)
    /// Debug-asserts that `height <= HEIGHT_MAX` and `txn_id <= TXN_MAX`. In release
    /// builds out-of-range inputs are masked to their low bits; callers must keep the
    /// fields in range (heights are tiny and `txn_id` will not wrap for millennia).
    #[inline]
    pub fn new(root_node_id: NodeId, height: u64, txn_id: u64) -> Self {
        debug_assert!(height <= HEIGHT_MAX, "height {height} exceeds {HEIGHT_MAX}");
        debug_assert!(txn_id <= TXN_MAX, "txn_id {txn_id} exceeds {TXN_MAX}");
        let w = (root_node_id as u128)
            | (((height & HEIGHT_MASK) as u128) << HEIGHT_SHIFT)
            | (((txn_id & TXN_MAX) as u128) << TXN_SHIFT);
        PackedMeta(w)
    }

    /// Reinterprets a raw word (e.g. just loaded from the atomic) as a `PackedMeta`.
    #[inline]
    pub fn from_raw(word: u128) -> Self {
        PackedMeta(word)
    }

    /// The raw 128-bit word, to store into the atomic.
    #[inline]
    pub fn to_raw(self) -> u128 {
        self.0
    }

    #[inline]
    pub fn root_node_id(self) -> NodeId {
        self.0 as u64
    }

    #[inline]
    pub fn height(self) -> u64 {
        ((self.0 >> HEIGHT_SHIFT) as u64) & HEIGHT_MASK
    }

    #[inline]
    pub fn txn_id(self) -> u64 {
        ((self.0 >> TXN_SHIFT) as u64) & TXN_MAX
    }

    /// Returns the same triple with `txn_id` advanced by one — the next publish word.
    ///
    /// # Panics (debug only)
    /// Debug-asserts the counter has not reached `TXN_MAX`.
    #[inline]
    pub fn with_next_txn(self, root_node_id: NodeId, height: u64) -> Self {
        let next = self.txn_id() + 1;
        debug_assert!(next <= TXN_MAX, "txn_id counter wrapped");
        PackedMeta::new(root_node_id, height, next)
    }
}

impl core::fmt::Debug for PackedMeta {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PackedMeta")
            .field("root_node_id", &self.root_node_id())
            .field("height", &self.height())
            .field("txn_id", &self.txn_id())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_each_field() {
        let p = PackedMeta::new(0xDEAD_BEEF_1234_5678, 7, 42);
        assert_eq!(p.root_node_id(), 0xDEAD_BEEF_1234_5678);
        assert_eq!(p.height(), 7);
        assert_eq!(p.txn_id(), 42);
    }

    #[test]
    fn raw_round_trip_is_identity() {
        let p = PackedMeta::new(123, 4, 99);
        assert_eq!(PackedMeta::from_raw(p.to_raw()), p);
    }

    #[test]
    fn fields_are_independent() {
        // Full-width root must not bleed into height/txn, and vice versa.
        let p = PackedMeta::new(u64::MAX, HEIGHT_MAX, TXN_MAX);
        assert_eq!(p.root_node_id(), u64::MAX);
        assert_eq!(p.height(), HEIGHT_MAX);
        assert_eq!(p.txn_id(), TXN_MAX);

        let z = PackedMeta::new(0, 0, 0);
        assert_eq!(z.to_raw(), 0);
        assert_eq!(z.root_node_id(), 0);
        assert_eq!(z.height(), 0);
        assert_eq!(z.txn_id(), 0);
    }

    #[test]
    fn aba_property_distinct_txn_distinct_word() {
        // Same root + height, different txn_id => different word. This is exactly the
        // property that makes a stale base word fail the CAS (ABA-safety by construction).
        let a = PackedMeta::new(500, 3, 10);
        let b = PackedMeta::new(500, 3, 11);
        assert_ne!(a.to_raw(), b.to_raw());
        assert_ne!(a, b);
    }

    #[test]
    fn with_next_txn_advances_only_the_counter() {
        let a = PackedMeta::new(500, 3, 10);
        let b = a.with_next_txn(600, 4);
        assert_eq!(b.txn_id(), 11);
        assert_eq!(b.root_node_id(), 600);
        assert_eq!(b.height(), 4);
        assert_ne!(a.to_raw(), b.to_raw());
    }

    #[test]
    fn boundary_values_do_not_overlap() {
        // txn at max, everything else zero: only the top 56 bits set.
        let p = PackedMeta::new(0, 0, TXN_MAX);
        assert_eq!(p.root_node_id(), 0);
        assert_eq!(p.height(), 0);
        assert_eq!(p.txn_id(), TXN_MAX);

        // height at max, everything else zero: only bits [64..72) set.
        let h = PackedMeta::new(0, HEIGHT_MAX, 0);
        assert_eq!(h.to_raw(), (HEIGHT_MASK as u128) << HEIGHT_SHIFT);
        assert_eq!(h.height(), HEIGHT_MAX);
        assert_eq!(h.txn_id(), 0);
    }

    #[test]
    #[should_panic]
    fn debug_rejects_out_of_range_height() {
        // height 256 does not fit in 8 bits.
        let _ = PackedMeta::new(1, HEIGHT_MAX + 1, 0);
    }
}
