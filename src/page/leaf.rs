//! page::leaf — slotted leaf page with pluggable key-block format.
//!
//! Layout (in a PAGE_SIZE buffer):
//!   [ header ][ KEY BLOCK ][ SLOT DIR ][     FREE     ][ VALUE ARENA ↓ from page end ]
//!    0        ^            ^ slots_end                               ^ values_hi
//!             keys_end = HEADER + key_block_len
//!             slots_end = keys_end + key_count * SLOT_SIZE
//!
//! Invariants:
//! - slots_end <= values_hi <= PAGE_SIZE
//! - key_count == number of slots
//! - slot i stores {val_off, val_len} into VALUE ARENA (values themselves are append-only, compacted lazily)

use zerocopy::{AsBytes, FromBytes, FromZeroes};

// Hook these to your actual crate paths:
use crate::keyfmt::KeyBlockFormat; // use the trait and resolve by id
use crate::keyfmt::resolve_key_format; // you implement: u8 -> &'static dyn KeyBlockFormat
use crate::layout::PAGE_SIZE; // const PAGE_SIZE: usize
use crate::page::LEAF_NODE_TAG;
use crate::page::PageError;

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
#[repr(C)]
#[derive(Clone, Copy, AsBytes, FromZeroes, FromBytes, Debug)]
pub struct Header {
    kind: u8,
    keyfmt_id: u8,
    key_count: u16,
    key_block_len: u16,
    values_hi: u16,
}

// Borrowed/mutable view over a leaf page buffer.
#[repr(C)]
#[derive(Clone, Copy, AsBytes, FromZeroes, FromBytes, Debug)]
pub struct LeafPage {
    header: Header,
    buf: [u8; PAGE_SIZE - std::mem::size_of::<Header>()],
}

//assert_eq_size!(LeafPage, [u8; PAGE_SIZE]);

impl LeafPage {
    pub fn new(keyfmt_id: u8) -> Self {
        LeafPage {
            header: Header {
                kind: LEAF_NODE_TAG,
                keyfmt_id,
                key_count: 0u16,
                key_block_len: 0u16,
                values_hi: PAGE_SIZE as u16,
            },
            buf: [0u8; PAGE_SIZE - std::mem::size_of::<Header>()],
        }
    }

    #[inline]
    pub fn from_bytes(buf: &[u8; PAGE_SIZE]) -> Result<&Self, PageError> {
        LeafPage::ref_from(buf).ok_or(PageError::FromBytesError {
            msg: "Failed to convert bytes to LeafPage".to_string(),
        })
    }

    #[inline]
    pub fn to_bytes(&self) -> Result<&[u8; PAGE_SIZE], std::array::TryFromSliceError> {
        let array: &[u8; PAGE_SIZE] = self.as_bytes().try_into()?; // also scoped
        Ok(array)
    }
    // --- header accessors ---

    #[inline] fn keyfmt_id(&self) -> u8 { self.header.keyfmt_id }
    #[inline] fn key_count(&self) -> u16 { self.header.key_count }
    #[inline] fn set_key_count(&mut self, n: u16) { self.header.key_count = n; }

    #[inline] fn key_block_len(&self) -> u16 { self.header.key_block_len }
    #[inline] fn set_key_block_len(&mut self, n: u16) { self.header.key_block_len = n; }

    #[inline] fn values_hi(&self) -> u16 { self.header.values_hi }
    #[inline] fn set_values_hi(&mut self, off: u16) { self.header.values_hi = off }

    // --- derived regions ---

    #[inline] fn keys_start(&self) -> usize { HEADER_SIZE }
    #[inline] fn keys_end(&self) -> usize { self.keys_start() + self.key_block_len() as usize }
    #[inline] fn key_block(&self) -> &[u8] { &self.buf[self.keys_start()..self.keys_end()] }
    #[inline] fn key_block_mut(&mut self) -> &mut [u8] {
        let end = self.keys_end();
        &mut self.buf[self.keys_start()..end]
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
    /// Seek on encoded key bytes; returns (insertion index, found).
    pub fn find_slot(&self, key_enc: &[u8], scratch: &mut Vec<u8>) -> (usize, bool) {
        self.fmt().seek(self.key_block(), key_enc, scratch)
    }

    // -------- value access --------

    pub fn read_value_at(&self, idx: usize) -> Result<&[u8], PageError> {
        let slot = self.read_slot(idx)?;
        let off = slot.val_off as usize;
        let len = slot.val_len as usize;
        let lo = self.values_hi_usize();
        let hi = self.slots_end();
        if off < lo || off.checked_add(len).unwrap_or(usize::MAX) > hi {
            return Err(PageError::Corrupt("slot outside arena"));
        }
        Ok(&self.buf[off..off + len])
    }

    /// Overwrite metadata to point to a new location (doesn't move the old bytes).
    pub fn overwrite_value_at(&mut self, idx: usize, val_off: u16, val_len: u16) -> Result<(), PageError> {
        self.write_slot(idx, LeafSlot { val_off, val_len })
    }

    // -------- slot access --------
    // -------- insert (encoded key & value) --------

    pub fn insert_encoded(&mut self, key_enc: &[u8], val_bytes: &[u8]) -> Result<(), PageError> {
        // 1) find position
        let mut scratch = Vec::new();
        let (idx, found) = self.find_slot(key_enc, &mut scratch);

        if found {
            // Upsert policy: allocate new tail and repoint the slot (no key changes).
            let (val_off, val_len) = self.alloc_value_tail(val_bytes)?; // respects slot region
            self.overwrite_value_at(idx, val_off, val_len)?;
            return Ok(());
        }

        // 2) build new key block bytes (correctness-first: rebuild whole block)
        let old_kb = self.key_block();
        let old_len = old_kb.len();

        let mut all_owned: Vec<Vec<u8>> = Vec::with_capacity(self.key_count() as usize + 1);
        for i in 0..self.key_count() as usize {
            let k = self.decode_key_at(i, &mut scratch);
            all_owned.push(k.to_vec());
        }
        all_owned.insert(idx, key_enc.to_vec());

        let mut refs: Vec<&[u8]> = all_owned.iter().map(|v| v.as_slice()).collect();
        let mut new_kb = Vec::new();
        self.fmt().encode_all(&refs, &mut new_kb);
        let new_len = new_kb.len();

        let delta_k = new_len as isize - old_len as isize;

        // 3) plan slot & value growth and verify capacity
        let key_count = self.key_count() as usize;
        let new_keys_end = (self.keys_end() as isize + delta_k) as usize;
        let new_slots_end = new_keys_end + (key_count + 1) * SLOT_SIZE;
        let new_values_hi = self.values_hi().checked_sub(val_bytes.len()).ok_or(PageError::PageFull {} )?;
        if new_slots_end > new_values_hi {
            return Err(PageError::PageFull {});
        }

        // 4) move slots by Δk to keep them flush with the key block
        self.move_slot_dir(delta_k)?;

        // 5) write new key block and commit len
        {
            let dst = &mut self.buf[self.keys_start()..self.keys_start()+new_len];
            dst.copy_from_slice(&new_kb);
            self.set_key_block_len(new_len as u16);
        }

        // 6) place value at the tail (just below current slots)
        let (val_off, val_len) = self.alloc_value_tail(val_bytes)?; // writes values_hi

        // 7) insert slot at idx
        self.slot_dir_insert(idx, LeafSlot { val_off, val_len })?;
        self.set_key_count(self.key_count() + 1);

        Ok(())
    }

    // -------- delete (by index) --------

    pub fn delete_at(&mut self, idx: usize) -> Result<(), PageError> {
        if idx >= self.key_count() as usize { return Err(PageError::IndexOutOfBounds { msg: "LeafPage::delete_at".to_string() } ); }

        // Rebuild key block without key idx
        let old_kb = self.key_block();
        let old_len = old_kb.len();

        let mut scratch = Vec::new();
        let mut all_owned: Vec<Vec<u8>> = Vec::with_capacity(self.key_count() as usize - 1);
        for i in 0..self.key_count() as usize {
            if i == idx { continue; }
            all_owned.push(self.decode_key_at(i, &mut scratch).to_vec());
        }
        let refs: Vec<&[u8]> = all_owned.iter().map(|v| v.as_slice()).collect();
        let mut new_kb = Vec::new();
        self.fmt().encode_all(&refs, &mut new_kb);
        let new_len = new_kb.len();
        let delta_k = new_len as isize - old_len as isize; // likely negative

        // capacity is a non-issue on delete (releasing space), but we'll still move slots first if shrinking negative after write
        // Move slots by Δk (can be negative)
        self.move_slot_dir(delta_k)?;

        // Write new key block
        {
            let dst = &mut self.buf[self.keys_start()..self.keys_start()+new_len];
            dst.copy_from_slice(&new_kb);
            self.set_key_block_len(new_len as u16);
        }

        // Remove slot idx
        self.slot_dir_remove(idx)?;
        self.set_key_count(self.key_count() - 1);

        Ok(())
    }

    // -------- compaction (optional) --------

    /// Pack value bytes tightly at the end and fix slot offsets.
    pub fn compact_values(&mut self) {
        let n = self.key_count() as usize;
        let mut write = PAGE_SIZE;
        // Copy values in reverse order to avoid overlap
        for i in (0..n).rev() {
            let slot = self.read_slot(i).unwrap();
            let off = slot.val_off as usize;
            let len = slot.val_len as usize;
            write -= len;
            // memmove
            self.buf.copy_within(off..off+len, write);
            // update slot
            self.write_slot(i, LeafSlot { val_off: write as u16, val_len: len as u16 }).unwrap();
        }
        self.set_values_hi(write as u16);
    }

    // ====== internals ======

    /// Move the entire slot directory by Δk bytes to keep it flush with the key block.
    fn move_slot_dir(&mut self, delta_k: isize) -> Result<(), PageError> {
        if delta_k == 0 { return Ok(()); }
        let k0 = self.keys_end();   // current end of keys (before commit of new len)
        let s0 = self.slots_end();  // current end of slots
        if delta_k > 0 {
            let dk = delta_k as usize;
            // Ensure room to move slots forward by dk
            if s0 + dk > self.values_hi() {
                return Err(PageError::PageFull {});
            }
            // move forward
            self.buf.copy_within(k0..s0, k0 + dk);
        } else {
            let dk = (-delta_k) as usize;
            // move backward
            self.buf.copy_within(k0..s0, k0 - dk);
        }
        Ok(())
    }

    /// Decode i-th encoded key bytes into scratch and return a view.
    fn decode_key_at<'s>(&self, i: usize, scratch: &'s mut Vec<u8>) -> &'s [u8] {
        self.fmt().decode_at(self.key_block(), i, scratch)
    }

    // ---- slot dir ops ----

    fn slot_off_for(&self, idx: usize) -> usize {
        self.slots_base() + idx * SLOT_SIZE
    }

    fn read_slot(&self, idx: usize) -> Result<LeafSlot, PageError> {
        if idx >= self.key_count() as usize { return Err(PageError::IndexOutOfBounds {}); }
        let base = self.slot_off_for(idx);
        Ok(LeafSlot { val_off: read_u16_le(&self.buf, base), val_len: read_u16_le(&self.buf, base + 2) })
    }

    fn write_slot(&mut self, idx: usize, slot: LeafSlot) -> Result<(), PageError> {
        if idx > self.key_count() as usize { return Err(PageError::IndexOutOfBounds {}); }
        let base = self.slot_off_for(idx);
        write_u16_le(&mut self.buf, base, slot.val_off);
        write_u16_le(&mut self.buf, base + 2, slot.val_len);
        Ok(())
    }

    fn slot_dir_insert(&mut self, idx: usize, slot: LeafSlot) -> Result<(), PageError> {
        let kc = self.key_count() as usize;
        if idx > kc { return Err(PageError::IndexOutOfBounds {}); }
        // shift right by one entry
        let base = self.slots_base();
        let from = base + idx * SLOT_SIZE;
        let to   = base + (kc + 1) * SLOT_SIZE;
        self.buf.copy_within(from..from + kc * SLOT_SIZE - idx * SLOT_SIZE, from + SLOT_SIZE);
        // write new
        write_u16_le(&mut self.buf, from, slot.val_off);
        write_u16_le(&mut self.buf, from + 2, slot.val_len);
        Ok(())
    }

    fn slot_dir_remove(&mut self, idx: usize) -> Result<(), PageError> {
        let kc = self.key_count() as usize;
        if idx >= kc { return Err(PageError::IndexOutOfBounds {}); }
        let base = self.slots_base();
        let from = base + (idx + 1) * SLOT_SIZE;
        let to   = base + kc * SLOT_SIZE;
        // shift left by one
        self.buf.copy_within(from..to, from - SLOT_SIZE);
        // zero last slot (optional)
        let last = base + (kc - 1) * SLOT_SIZE;
        for b in &mut self.buf[last..last + SLOT_SIZE] { *b = 0; }
        Ok(())
    }

    // ---- value arena ----

    /// Allocate value at tail **below current slots** (uses header.values_hi and slot count).
    fn alloc_value_tail(&mut self, val: &[u8]) -> Result<(u16, u16), PageError> {
        let val_len = val.len();
        let new_hi = self.values_hi().checked_sub(val_len).ok_or(PageError::PageFull {})?;
        if new_hi < self.slots_end() { return Err(PageError::PageFull {}); }
        self.buf[new_hi..new_hi + val_len].copy_from_slice(val);
        self.set_values_hi(new_hi as u16);
        Ok((new_hi as u16, val_len as u16))
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

