//! page::leaf — slotted leaf page with pluggable key-block format.
//
// Layout (within a PAGE_SIZE buffer):
//
//  [header (fixed 8 bytes)]
//  [KEY BLOCK bytes ...]                ← managed by KeyBlockFormat
//  [FREE SPACE ...]
//  [VALUE ARENA bytes ... grows downward from the end of free space up toward slot dir]
//  [SLOT DIR entries ... at end of page, grows upward from PAGE_SIZE]
//
// Slot dir entry is fixed-size (4 bytes): (val_off: u16, val_len: u16).
//
// Invariants:
// - header.key_count == number of keys == number of slots
// - slot_dir_off() = PAGE_SIZE - key_count * SLOT_SIZE
// - values occupy [values_hi .. slot_dir_off())
// - free space is [keys_end .. values_hi)
// - keys_end = HEADER_SIZE + key_block_len

use zerocopy::{AsBytes, FromBytes, FromZeroes};

// Hook these to your actual crate paths:
use crate::keyfmt::KeyBlockFormat; // trait; and you'll provide a resolver by id
use crate::keyfmt::resolve_key_format; // you implement: u8 -> &'static dyn KeyBlockFormat
use crate::layout::PAGE_SIZE; // const PAGE_SIZE: usize
use crate::page::LEAF_NODE_TAG;
use crate::page::PageError;
// use crate::storage::PageId; // if you want to carry ids here

// ------ header (packed via manual offsets; no unsafe) ------

const HDR_KIND: usize = 0;             // u8: 0x01 for leaf
const HDR_KEYFMT_ID: usize = 1;        // u8
const HDR_KEY_COUNT: usize = 2;        // u16 LE
const HDR_KEY_BLOCK_LEN: usize = 4;    // u16 LE
const HDR_VALUES_HI: usize = 6;        // u16 LE
pub const HEADER_SIZE: usize = 8;

#[inline]
fn read_u16_le(buf: &[u8], off: usize) -> u16 {
    u16::from_le_bytes([buf[off], buf[off + 1]])
}

#[inline]
fn write_u16_le(buf: &mut [u8], off: usize, v: u16) {
    let b = v.to_le_bytes();
    buf.copy_from_slice(&b);
}

// Slot directory item at the end of the page.
#[repr(C)]
#[derive(Clone, Copy, AsBytes, FromZeroes, FromBytes, Debug)]
struct LeafSlot { val_off: u16, val_len: u16 }
const SLOT_SIZE: usize = core::mem::size_of::<LeafSlot>();
const LEN_SIZE: usize = std::mem::size_of::<u16>();

// Borrowed/mutable view over a leaf page buffer.
#[repr(transparent)]
#[derive(Clone, Copy, AsBytes, FromZeroes, FromBytes, Debug)]
pub struct Header<'a> {
    kind: &'a mut u8,
    keyfmt_id: &'a mut u8,
    key_count: &'a mut u16,
    key_block_len: &'a mut u16,
    values_hi: &'a mut u16,
}

// Borrowed/mutable view over a leaf page buffer.
#[repr(transparent)]
#[derive(Clone, Copy, AsBytes, FromZeroes, FromBytes, Debug)]
pub struct LeafPage<'a> {
    header: Header<'a>,
    buf: &'a mut [u8; PAGE_SIZE - std::mem::size_of::<Header>()],
}

//assert_eq_size!(LeafPage<'_>, [u8; PAGE_SIZE]);

impl<'a> LeafPage<'a> {
    pub fn new(keyfmt_id: u8) -> Self {
        LeafPage {
            header: Header {
                kind: &mut (LEAF_NODE_TAG),
                keyfmt_id: &mut (keyfmt_id),
                key_count: &mut 0u16,
                key_block_len: &mut 0u16,
                values_hi: &mut (PAGE_SIZE as u16),
            },
            buf: &mut [0u8; PAGE_SIZE - std::mem::size_of::<Header>()],
        }
    }

    pub fn from_bytes(buf: &[u8; PAGE_SIZE]) -> Result<&Self, PageError> {
        LeafPage::ref_from(buf).ok_or(PageError::FromBytesError {
            msg: "Failed to convert bytes to LeafPage".to_string(),
        })
    }

    // --- header accessors ---

    #[inline] fn keyfmt_id(&self) -> u8 { self.buf[HDR_KEYFMT_ID] }
    #[inline] fn key_count(&self) -> u16 { read_u16_le(self.buf, HDR_KEY_COUNT) }
    #[inline] fn set_key_count(&mut self, n: u16) { write_u16_le(self.buf, HDR_KEY_COUNT, n) }

    #[inline] fn key_block_len(&self) -> u16 { read_u16_le(self.buf, HDR_KEY_BLOCK_LEN) }
    #[inline] fn set_key_block_len(&mut self, n: u16) { write_u16_le(self.buf, HDR_KEY_BLOCK_LEN, n) }

    #[inline] fn values_hi(&self) -> u16 { read_u16_le(self.buf, HDR_VALUES_HI) }
    #[inline] fn set_values_hi(&mut self, off: u16) { write_u16_le(self.buf, HDR_VALUES_HI, off) }

    // --- derived regions ---

    #[inline] fn keys_start(&self) -> usize { HEADER_SIZE }
    #[inline] fn keys_end(&self) -> usize { self.keys_start() + self.key_block_len() as usize }
    #[inline] fn key_block(&self) -> &[u8] { &self.buf[self.keys_start()..self.keys_end()] }
    #[inline] fn key_block_mut(&mut self) -> &mut [u8] {
        let end = self.keys_end();
        &mut self.buf[self.keys_start()..end]
    }

    #[inline] fn slot_dir_off(&self) -> usize {
        PAGE_SIZE - self.key_count() as usize * SLOT_SIZE
    }
    #[inline] fn values_range(&self) -> core::ops::Range<usize> {
        self.values_hi() as usize .. self.slot_dir_off()
    }
    #[inline] fn free_range(&self) -> core::ops::Range<usize> {
        self.keys_end() .. self.values_hi() as usize
    }
    #[inline] fn free_bytes(&self) -> usize {
        let r = self.free_range();
        r.end.saturating_sub(r.start)
    }

    // Resolve runtime key format
    fn fmt(&self) -> &dyn KeyBlockFormat {
        resolve_key_format(self.keyfmt_id())
            .expect("unknown key format id; register it in keyfmt::resolve_key_format")
    }

    // Lightweight view for calling the format
    fn key_run<'s>(&'s self) -> PageKeyRun<'s> {
        PageKeyRun { body: self.key_block(), fmt: self.fmt() }
    }

    // ---- search ----

    /// Binary seek using the key format; returns (insertion index, found).
    pub fn find_slot(&self, key_enc: &[u8], scratch: &mut Vec<u8>) -> (usize, bool) {
        let (i, found) = self.key_run().seek(key_enc, scratch);
        debug_assert!(i <= self.key_count() as usize);
        (i, found)
    }

    // ---- read value by index ----

    pub fn read_value_at(&self, idx: usize) -> Result<&[u8], PageError> {
        if idx >= self.key_count() as usize { return Err(PageError::Bounds); }
        let slot = self.read_slot(idx)?;
        let off = slot.val_off as usize;
        let len = slot.val_len as usize;
        let vr = self.values_range();
        if off < vr.start || off.checked_add(len).unwrap_or(usize::MAX) > vr.end {
            return Err(PageError::Corrupt("slot points outside value arena"));
        }
        Ok(&self.buf[off..off + len])
    }

    // ---- insert ----

    /// Insert a new (key,value) where `key_enc` is the order-preserving encoded key bytes.
    pub fn insert(&mut self, key_enc: &[u8], val_bytes: &[u8]) -> Result<(), PageError> {
        // 0) space check (rough): need value bytes + one slot; key delta rebuilt in-place
        let need = val_bytes.len() + SLOT_SIZE;
        if self.free_bytes() < need {
            return Err(PageError::NoSpace);
        }

        // 1) allocate value into arena (growing down from the end)
        let (val_off, val_len) = self.value_arena_alloc(val_bytes)?;

        // 2) find index
        let mut scratch = Vec::new();
        let (idx, found) = self.find_slot(key_enc, &mut scratch);
        if found {
            // Your policy: overwrite? return error? For now, treat as upsert: replace value.
            self.overwrite_value_at(idx, val_off, val_len)?;
            return Ok(());
        }

        // 3) insert slot into directory
        self.slot_dir_insert(idx, LeafSlot { val_off, val_len })?;

        // 4) re-encode a small key window [s..e) containing the insertion point
        let (s, e) = self.window_for_insert(idx);
        let mut new_keys: Vec<&[u8]> = Vec::with_capacity(e - s + 1);
        // collect keys s..idx
        for j in s..idx {
            new_keys.push(self.key_at(j, &mut scratch));
        }
        // the new key
        new_keys.push(key_enc);
        // collect keys idx..e
        for j in idx..e {
            new_keys.push(self.key_at(j, &mut scratch));
        }

        // rebuild
        let mut out = Vec::new();
        self.key_run().rebuild_window(s, e, &new_keys, &mut out);
        self.splice_key_block(s, e, &out)?;

        // bump counts
        let kc = self.key_count().checked_add(1).ok_or(PageError::Corrupt("key_count overflow"))?;
        self.set_key_count(kc);

        Ok(())
    }

    // ---- delete ----

    pub fn delete_at(&mut self, idx: usize) -> Result<(), PageError> {
        if idx >= self.key_count() as usize { return Err(PageError::Bounds); }

        // 1) free policy: we don't reclaim value space (classic slotted page).
        //    You can add a free-list later. For now, only remove slot.
        self.slot_dir_remove(idx)?;

        // 2) rebuild a small window around idx (drop key at idx)
        let (s, e) = self.window_for_delete(idx);
        let mut scratch = Vec::new();
        let mut new_keys: Vec<&[u8]> = Vec::with_capacity(e - s - 1);
        for j in s..e {
            if j == idx { continue; }
            new_keys.push(self.key_at(j, &mut scratch));
        }
        let mut out = Vec::new();
        self.key_run().rebuild_window(s, e, &new_keys, &mut out);
        self.splice_key_block(s, e, &out)?;

        // 3) dec count
        let kc = self.key_count().checked_sub(1).ok_or(PageError::Corrupt("underflow key_count"))?;
        self.set_key_count(kc);
        Ok(())
    }

    // ---- internal helpers ----

    /// Decode the i-th encoded key BYTES into scratch and return it.
    fn key_at<'s>(&self, i: usize, scratch: &'s mut Vec<u8>) -> &'s [u8] {
        self.fmt().decode_at(self.key_block(), i, scratch)
    }

    /// Choose a rebuild window for insert (format-specific; default: the whole block for Raw).
    fn window_for_insert(&self, idx: usize) -> (usize, usize) {
        // For Prefix+Restarts, expand to [prev_restart(idx) .. next_restart_ge(idx)]
        // For Raw, tiny window is fine (just the inserted index).
        // We'll default to a small window of size 1 around idx.
        (idx, idx) // [s..e) half-open; when s==e we'll treat as "insert at idx"
    }

    /// Choose a rebuild window for delete.
    fn window_for_delete(&self, idx: usize) -> (usize, usize) {
        // Default: just the deleted index
        (idx, idx + 1)
    }

    /// Splice the key-block bytes: replace [s..e) logical entries with `out` bytes.
    fn splice_key_block(&mut self, s: usize, e: usize, out: &[u8]) -> Result<(), PageError> {
        // Compute byte offsets of logical entries s and e within the key-block.
        // For Raw, this is straightforward; for Prefix, you'll compute from restart metadata.
        // To keep this file format-agnostic, we lean on the format to give us byte ranges.
        // Minimal approach: rebuild ENTIRE block using encode_all for now (correctness first).
        // TODO: optimize to in-place splice by teaching KeyBlockFormat to expose byte ranges.
        let mut all_keys: Vec<Vec<u8>> = Vec::with_capacity(self.key_count() as usize);
        let mut scratch = Vec::new();
        for i in 0..self.key_count() as usize {
            if i >= s && i < e {
                continue; // replaced by `out` keys we already encoded
            }
            all_keys.push(self.key_at(i, &mut scratch).to_vec());
        }
        // Insert the new ones at s
        let mut res: Vec<&[u8]> = Vec::with_capacity(all_keys.len() + out.len());
        for (i, k) in all_keys.iter().enumerate() {
            if i == s { /* fallthrough after pushing new */ }
            // We'll just rebuild everything for now; use encode_all:
        }
        // Fallback correctness path:
        let mut rebuilt = Vec::new();
        // We need the set [0..key_count'] in order. We've already adjusted key_count
        // in insert/delete, so derive final list by re-seeking. Simpler: parse `out`
        // caller-building made `out` be a full window re-encode, not individual keys;
        // so we must rebuild all keys explicitly here. To avoid confusion,
        // prefer the simpler, correct approach:
        self.rebuild_all_keys_using_format()?;
        Ok(())
    }

    /// Correctness-first: rebuild the entire key block from decoded keys.
    fn rebuild_all_keys_using_format(&mut self) -> Result<(), PageError> {
        let mut scratch = Vec::new();
        let mut keys: Vec<&[u8]> = Vec::with_capacity(self.key_count() as usize);
        for i in 0..self.key_count() as usize {
            keys.push(self.key_at(i, &mut scratch));
        }
        let mut out = Vec::new();
        self.fmt().encode_all(&keys, &mut out);
        // write back
        let need = HEADER_SIZE + out.len();
        if need > self.values_hi() as usize {
            return Err(PageError::NoSpace);
        }
        // move free space boundary and copy
        self.set_key_block_len(out.len() as u16);
        self.key_block_mut().copy_from_slice(&out);
        Ok(())
    }

    // ---- slot dir ops ----

    fn read_slot(&self, idx: usize) -> Result<LeafSlot, PageError> {
        let kc = self.key_count() as usize;
        if idx >= kc { return Err(PageError::Bounds); }
        let base = self.slot_dir_off() + idx * SLOT_SIZE;
        let off = read_u16_le(self.buf, base);
        let len = read_u16_le(self.buf, base + LEN_SIZE);
        Ok(LeafSlot { val_off: off, val_len: len })
    }

    fn write_slot(&mut self, idx: usize, slot: LeafSlot) -> Result<(), PageError> {
        let kc = self.key_count() as usize;
        if idx > kc { return Err(PageError::Bounds); }
        let base = self.slot_dir_off() + idx * SLOT_SIZE;
        write_u16_le(self.buf, base, slot.val_off);
        write_u16_le(self.buf, base + LEN_SIZE, slot.val_len);
        Ok(())
    }

    fn slot_dir_insert(&mut self, idx: usize, slot: LeafSlot) -> Result<(), PageError> {
        let kc = self.key_count() as usize;
        if idx > kc { return Err(PageError::Bounds); }
        // new slot dir starts one slot earlier
        let new_slot_off = PAGE_SIZE - (kc + 1) * SLOT_SIZE;
        // Is there space? (free space does not account for slot dir growth; check explicitly.)
        if new_slot_off < self.values_hi() as usize {
            return Err(PageError::NoSpace);
        }
        // shift existing slots right by one from idx..kc-1
        for i in (idx..kc).rev() {
            let src = self.slot_dir_off() + i * SLOT_SIZE;
            let dst = src + SLOT_SIZE;
            self.buf.copy_within(src..src + SLOT_SIZE, dst);
        }
        // write new slot
        let base = new_slot_off + idx * SLOT_SIZE;
        write_u16_le(self.buf, base, slot.val_off);
        write_u16_le(self.buf, base + LEN_SIZE, slot.val_len);
        Ok(())
    }

    fn slot_dir_remove(&mut self, idx: usize) -> Result<(), PageError> {
        let kc = self.key_count() as usize;
        if idx >= kc { return Err(PageError::Bounds); }
        // shift left idx+1..kc-1
        for i in idx + 1..kc {
            let src = self.slot_dir_off() + i * SLOT_SIZE;
            let dst = src - SLOT_SIZE;
            self.buf.copy_within(src..src + SLOT_SIZE, dst);
        }
        // (optional) zero the now-free last slot
        let last_off = PAGE_SIZE - SLOT_SIZE;
        self.buf[last_off..].fill(0);
        Ok(())
    }

    // ---- value arena ----

    fn value_arena_alloc(&mut self, val: &[u8]) -> Result<(u16, u16), PageError> {
        // val len must fit in u16
        let len = u16::try_from(val.len()).map_err(|_| PageError::NoSpace)?;
        let free = self.free_bytes();
        // Reserve also one slot worth? We checked earlier before calling.
        if free < len { return Err(PageError::NoSpace); }
        // values_hi grows downward
        let new_hi = self.values_hi()
            .checked_sub(len).ok_or(PageError::NoSpace)?;
        if new_hi < self.keys_end() { return Err(PageError::NoSpace); }
        self.buf[new_hi..new_hi + len].copy_from_slice(val);
        self.set_values_hi(new_hi);
        Ok((new_hi, len))
    }
}

// Tiny helper view handed to the KeyBlockFormat
struct PageKeyRun<'a> {
    body: &'a [u8],
    fmt:  &'a dyn KeyBlockFormat,
}
impl<'a> PageKeyRun<'a> {
    fn seek(&self, needle: &[u8], scratch: &mut Vec<u8>) -> (usize, bool) {
        self.fmt.seek(self.body, needle, scratch)
    }
    fn rebuild_window(&self, start: usize, end: usize, new_keys: &[&[u8]], out: &mut Vec<u8>) {
        self.fmt.rebuild_window(self.body, start, end, new_keys, out)
    }
}

