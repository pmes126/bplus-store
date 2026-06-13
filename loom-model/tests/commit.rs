//! loom model of the inline-packed commit publish protocol used by the B+ tree's
//! `committed` word (`src/bplustree/tree.rs` + `packed_meta.rs`).
//!
//! The real publish point is an `AtomicU128`, which loom cannot instrument (loom models
//! only up to 64-bit atomics). The publish/observe protocol — a version-stamped CAS with
//! Acquire/Release pairing — is width-independent, so this models the word as a packed
//! `AtomicU64` (`root` in the high 32 bits, `txn` in the low 32) and exhaustively checks
//! the two properties that matter:
//!
//!   1. **Acquire/Release visibility** — a reader that observes a published version also
//!      observes the COW "page" writes that preceded the publish.
//!   2. **ABA-safety / no lost update** — the monotonic `txn` stamp makes a stale CAS
//!      fail, so concurrent version-stamped writers each commit exactly once.
//!
//! Run with:  RUSTFLAGS="--cfg loom" cargo test --release   (from `loom-model/`)
#![cfg(loom)]

use loom::sync::Arc;
use loom::sync::atomic::{AtomicU64, Ordering};

#[inline]
fn pack(root: u32, txn: u32) -> u64 {
    (u64::from(root) << 32) | u64::from(txn)
}
#[inline]
fn root(word: u64) -> u32 {
    (word >> 32) as u32
}
#[inline]
fn txn(word: u64) -> u32 {
    word as u32
}

/// Acquire/Release contract: a reader that observes the published version must also
/// observe the writer's pre-publish "page" write. Mirrors the tree's ordering rule —
/// readers load `committed` with `Acquire` so all COW writes performed before the word
/// was published are visible.
#[test]
fn publish_release_acquire_visibility() {
    loom::model(|| {
        let committed = Arc::new(AtomicU64::new(pack(0, 0)));
        let page = Arc::new(AtomicU64::new(0));

        let c = committed.clone();
        let p = page.clone();
        let writer = loom::thread::spawn(move || {
            // COW: write the page first, then publish the new root with a version-stamped
            // CAS using Release ordering.
            p.store(42, Ordering::Relaxed);
            let cur = c.load(Ordering::Relaxed);
            let next = pack(1, txn(cur) + 1);
            let _ = c.compare_exchange(cur, next, Ordering::Release, Ordering::Relaxed);
        });

        // Reader: load with Acquire. If the published version is observed, the paired
        // Release guarantees the page write is visible.
        let seen = committed.load(Ordering::Acquire);
        if txn(seen) >= 1 {
            assert_eq!(root(seen), 1);
            assert_eq!(page.load(Ordering::Relaxed), 42);
        }

        writer.join().unwrap();
    });
}

/// ABA-safety / no lost update: two writers each publish once via a version-stamped CAS
/// with retry. The monotonic `txn` makes a stale CAS fail, so under every interleaving
/// both commits land and `txn` advances by exactly two.
#[test]
fn version_stamped_cas_has_no_lost_update() {
    loom::model(|| {
        let committed = Arc::new(AtomicU64::new(pack(0, 0)));

        let spawn_writer = |c: Arc<AtomicU64>, new_root: u32| {
            loom::thread::spawn(move || {
                loop {
                    let cur = c.load(Ordering::Acquire);
                    let next = pack(new_root, txn(cur) + 1);
                    if c.compare_exchange(cur, next, Ordering::SeqCst, Ordering::Acquire)
                        .is_ok()
                    {
                        break;
                    }
                }
            })
        };

        let w1 = spawn_writer(committed.clone(), 1);
        let w2 = spawn_writer(committed.clone(), 2);
        w1.join().unwrap();
        w2.join().unwrap();

        let final_word = committed.load(Ordering::Acquire);
        // Both writers committed exactly once: neither was lost to an ABA mismatch.
        assert_eq!(txn(final_word), 2);
        // The surviving root is whichever writer committed last.
        let r = root(final_word);
        assert!(r == 1 || r == 2);
    });
}
