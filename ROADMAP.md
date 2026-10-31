# Roadmap & Known Limitations

Current version: **0.7.0** (pre-1.0). `main` is the source of truth.

This document tracks known limitations and the work remaining before a stable
**1.0.0** release. 1.0.0 is a semver commitment: a frozen public API and an
on-disk format we promise not to break within the 1.x line.

## What is solid today

- **On-disk format is versioned and self-describing.** The superblock carries a
  magic number (`"SUPR"`), a format version, and a CRC-32C, all validated on open
  (`src/database/superblock.rs`, `Database::open`).
- **COW commit path is CAS-based and race-free.** Metadata pointer + height +
  `txn_id` are published via a single 128-bit atomic (free of ABA/UAF); conflicts
  surface as `CommitError::StaleBase` for caller retry.
- **Page reclamation** via epoch-based GC: superseded COW pages return to the
  freelist once no reader is pinned at/below the retire epoch.
- **Intra-page value compaction** (`LeafPage::compact_values`) reclaims dead
  value-arena space after deletes.
- **Delete rebalances the tree.** Underflow is resolved by borrowing from the
  left or right sibling, then by merging with a sibling; a parent that underflows
  as a result propagates the fix up the tree, and a root left with a single child
  is collapsed with a height decrement. (`handle_underflow`,
  `src/bplustree/tree.rs:1084`)

## Known limitations (tracked TODOs)

- [ ] **Merge can decline when entries are variable-size.** Underflow is normally
  resolved (see above), but with variable-length keys and values the combined
  *physical* size of two underfull nodes can exceed a page even when their key
  counts are within bounds. In that case the node is accepted underfull rather
  than violating a page invariant: the tree stays correct, but can be slightly
  unbalanced, with some space amplification under delete-heavy workloads.
  (`handle_underflow`, `src/bplustree/tree.rs:1148`)
- [ ] **Corrupt-metadata recovery is not implemented.** Recovery from a torn or
  corrupt metadata page has no path yet; the corresponding test is a stub.
  (`src/tests/tree_ops.rs:585`)
- [ ] **No file-level compaction / vacuum.** Freed pages are reused in place via
  the freelist, but the data file only grows to its high-water mark — deleting
  data does not shrink `data.db`.
- [ ] **Freelist snapshot is single-page.** After a long operation the freed-page
  set may exceed one page; the snapshot should become a linked list of pages.
  (`src/database/superblock.rs:23`)
- [ ] **`KeyLimits` is parsed but not enforced.** Declared per-tree key limits are
  currently ignored. (`src/database/catalog.rs:110`)
- [ ] **No on-disk format migration path.** The superblock version check is exact
  equality (reject-unknown, which is safe), but there is no defined mechanism to
  upgrade an older-format file to a newer one.

