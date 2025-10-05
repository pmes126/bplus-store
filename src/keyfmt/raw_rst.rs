#[derive(Copy, Clone)]
pub struct RawWithRestarts {
    pub restart_interval: u16, // advisory; we keep existing restart positions on insert
}

impl RawWithRestarts {
    fn count_entries(blk: &[u8]) -> usize {
        let (entries_end, _, _) = Self::tail(blk).unwrap_or((0, 0, 0));
        let mut p = &blk[..entries_end];
        let mut n = 0;
        while p.len() >= 2 {
            let len = u16::from_le_bytes([p[0], p[1]]) as usize;
            let need = 2 + len;
            if p.len() < need { break; }
            n += 1; p = &p[need..];
        }
        n
    }
    fn tail(blk: &[u8]) -> Option<(usize, usize, usize)> {
        if blk.len() < 4 { return None; }
        let rcount = u32::from_le_bytes(blk[blk.len()-4..].try_into().ok()?) as usize;
        let table_bytes = rcount * 4;
        if blk.len() < 4 + table_bytes { return None; }
        let restarts = blk.len() - 4 - table_bytes;
        Some((restarts, restarts, rcount))
    }
    fn entry_range(blk: &[u8], idx: usize) -> std::ops::Range<usize> {
        let (entries_end, _, _) = Self::tail(blk).expect("corrupt");
        let mut off = 0usize;
        for _ in 0..idx {
            let len = u16::from_le_bytes([blk[off], blk[off+1]]) as usize;
            off += 2 + len;
        }
        let len = u16::from_le_bytes([blk[off], blk[off+1]]) as usize;
        let start = off; let end = off + 2 + len;
        debug_assert!(end <= entries_end);
        start..end
    }
    fn load_restarts(blk: &[u8]) -> (usize, Vec<u32>) {
        let (entries_end, restarts, rcount) = Self::tail(blk).expect("corrupt");
        let mut offs = Vec::with_capacity(rcount);
        for i in 0..rcount {
            let o = restarts + i * 4;
            offs.push(u32::from_le_bytes(blk[o..o+4].try_into().unwrap()));
        }
        (entries_end, offs)
    }
    fn write_restarts(out: &mut [u8], entries_end: usize, offs: &[u32]) {
        let mut w = entries_end;
        for &o in offs {
            out[w..w+4].copy_from_slice(&o.to_le_bytes()); w += 4;
        }
        out[w..w+4].copy_from_slice(&(offs.len() as u32).to_le_bytes());
        debug_assert_eq!(w+4, out.len());
    }
}

impl super::KeyBlockFormat for RawWithRestarts {
    fn format_id(&self) -> u8 { 2 }

    fn seek(&self, blk: &[u8], needle: &[u8], _sc: &mut Vec<u8>) -> (usize, bool) {
        // binsearch restarts; then linear inside block (length-prefixed so cheap)
        let (entries_end, restarts, rcount) = Self::tail(blk).unwrap_or((0, 0, 0));
        // trivial: just linear for brevity (opt: binsearch on restarts)
        let mut off = 0usize;
        let mut idx = 0usize;
        while off < entries_end {
            let len = u16::from_le_bytes([blk[off], blk[off+1]]) as usize;
            let k = &blk[off+2..off+2+len];
            match k.cmp(needle) {
                core::cmp::Ordering::Equal => return (idx, true),
                core::cmp::Ordering::Greater => return (idx, false),
                core::cmp::Ordering::Less => { off += 2+len; idx += 1; }
            }
        }
        (idx, false)
    }

    fn decode_at<'s>(&self, blk: &[u8], i: usize, _sc: &'s mut Vec<u8>) -> &'s [u8] {
        let r = Self::entry_range(blk, i);
        unsafe { &*(&blk[r.start+2..r.end] as *const [u8]) }
    }

    fn insert_delta(&self, blk: &[u8], idx: usize, new_key: &[u8], _sc: &mut Vec<u8>) -> isize {
        let (entries_end, _, _) = Self::tail(blk).expect("corrupt");
        let new_entry = 2 + new_key.len();
        let old_entry = if idx < Self::count_entries(blk) {
            let r = Self::entry_range(blk, idx); r.end - r.start
        } else { 0 };
        // table size unchanged (we keep restart positions), so delta is entries-only
        (new_entry as isize - old_entry as isize)
    }

    fn delete_delta(&self, blk: &[u8], idx: usize, _sc: &mut Vec<u8>) -> isize {
        let r = Self::entry_range(blk, idx);
        -(r.end as isize - r.start as isize)
    }

    fn insert_apply(
        &self,
        block_in: &[u8],
        block_out: &mut [u8],
        idx: usize,
        new_key: &[u8],
        _scratch: &mut Vec<u8>,
    ) {
        let (entries_end_in, mut rest_offs) = Self::load_restarts(block_in);
        let old_len = block_in.len();
        let new_len = block_out.len();
        let entry_bytes = {
            let mut v = Vec::with_capacity(2 + new_key.len());
            v.extend_from_slice(&(new_key.len() as u16).to_le_bytes());
            v.extend_from_slice(new_key);
            v
        };
        let ins = if idx < Self::count_entries(block_in) {
            Self::entry_range(block_in, idx)
        } else {
            entries_end_in..entries_end_in
        };
        // copy entries region
        let mut w = 0usize;
        block_out[w..w+ins.start].copy_from_slice(&block_in[..ins.start]); w += ins.start;
        block_out[w..w+entry_bytes.len()].copy_from_slice(&entry_bytes); w += entry_bytes.len();
        block_out[w..w+(entries_end_in - ins.end)].copy_from_slice(&block_in[ins.end..entries_end_in]); w += entries_end_in - ins.end;
        let entries_end_out = w;

        // adjust restart offsets: any offset > ins.start shifts by delta
        let delta = entries_end_out as isize - entries_end_in as isize;
        if delta != 0 {
            for o in &mut rest_offs {
                if (*o as usize) > ins.start {
                    *o = ((*o as isize) + delta) as u32;
                }
            }
        }

        // write table
        Self::write_restarts(block_out, entries_end_out, &rest_offs);
        debug_assert_eq!(block_out.len(), entries_end_out + rest_offs.len()*4 + 4);
    }

    fn delete_apply(
        &self,
        block_in: &[u8],
        block_out: &mut [u8],
        idx: usize,
        _scratch: &mut Vec<u8>,
    ) {
        let (entries_end_in, mut rest_offs) = Self::load_restarts(block_in);
        let del = Self::entry_range(block_in, idx);

        // copy entries region
        let mut w = 0usize;
        block_out[w..w+del.start].copy_from_slice(&block_in[..del.start]); w += del.start;
        block_out[w..w+(entries_end_in - del.end)].copy_from_slice(&block_in[del.end..entries_end_in]); w += entries_end_in - del.end;
        let entries_end_out = w;

        // adjust restarts after deleted range
        let delta = entries_end_out as isize - entries_end_in as isize; // negative
        for o in &mut rest_offs {
            if (*o as usize) >= del.end {
                *o = ((*o as isize) + delta) as u32;
            } else if (*o as usize) > del.start {
                // a restart was inside the deleted entry (rare in raw-with-restarts since entries aren't “restarts”),
                // drop it by snapping to del.start (or rebuild table based on interval). Here: shift by delta anyway.
                *o = ((*o as isize) + delta) as u32;
            }
        }
        Self::write_restarts(block_out, entries_end_out, &rest_offs);
    }

    fn adjust_after_splice(&self, block_final: &mut [u8], splice_at: usize, delta: isize, _idx: usize) {
        if block_final.len() < 4 { return; }
        let rcount = u32::from_le_bytes(block_final[block_final.len()-4..].try_into().unwrap()) as usize;
        let table_bytes = rcount * 4;
        if block_final.len() < 4 + table_bytes { return; }
        let table_off = block_final.len() - 4 - table_bytes;
        let delta32 = delta as i64; // avoid overflow

        for i in 0..rcount {
            let o = table_off + i*4;
            let mut off = u32::from_le_bytes(block_final[o..o+4].try_into().unwrap()) as i64;
            if off as usize > splice_at {
                off += delta32;
                let adj = (off as u32).to_le_bytes();
                block_final[o..o+4].copy_from_slice(&adj);
            }
        }
        // count stays the same; table stays at the tail automatically.
    }
}

