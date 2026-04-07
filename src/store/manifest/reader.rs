use std::{fs::File, io::{self, Read}, path::Path};
use crate::store::manifest::ManifestRec;

pub struct ManifestReader {
    file: File,
}

impl ManifestReader {
    pub fn open(path: &Path) -> io::Result<Self> {
        Ok(Self { file: File::open(path)? })
    }

    pub fn next(&mut self) -> io::Result<Option<ManifestRec>> {
        // read TYPE + PAYLOAD, TODO: add CRC
        ManifestRec::decode(&mut self.file.by_ref()).map(Some)
    }
}
