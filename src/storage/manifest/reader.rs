use std::{fs::File, io::{self, Read}, path::Path};
use crc32c::crc32c;
use crate::storage::manifest::ManifestRec;

pub struct ManifestReader {
    file: File,
}
impl ManifestReader {
    pub fn open(path: &Path) -> io::Result<Self> {
        Ok(Self { file: File::open(path)? })
    }

    pub fn next(&mut self) -> io::Result<Option<ManifestRec>> {
        // read LEN(varu64); EOF -> Ok(None)
        let len = match read_varu64(&mut self.file) {
            Ok(n) => n as usize,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        };

        // read TYPE + PAYLOAD + CRC
        let mut buf = vec![0u8; len];
        if let Err(e) = self.file.read_exact(&mut buf) {
            // torn record -> stop cleanly
            if e.kind() == io::ErrorKind::UnexpectedEof { return Ok(None); }
            return Err(e);
        }
        let (typ, rest) = buf.split_first().unwrap();
        let (payload, crc_bytes) = rest.split_at(rest.len()-4);
        let crc_read = u32::from_le_bytes(crc_bytes.try_into().unwrap());
        let crc_calc = crc32c(&[std::slice::from_ref(typ), payload].concat());
        if crc_read != crc_calc {
            // bad CRC -> stop at last good
            return Ok(None);
        }
        // deserialize (match typ)
        let rec: ManifestRec = bincode::deserialize(payload).unwrap(); // align with writer
        Ok(Some(rec))
    }
}

fn read_varu64<R: Read>(r: &mut R) -> io::Result<u64> {
    let mut x = 0u64; let mut s = 0;
    for _ in 0..10 {
        let mut b = [0u8;1]; r.read_exact(&mut b)?;
        let byte = b[0];
        x |= ((byte & 0x7F) as u64) << s;
        if byte & 0x80 == 0 { return Ok(x); }
        s += 7;
    }
    Err(io::Error::new(io::ErrorKind::InvalidData, "varint too long"))
}

