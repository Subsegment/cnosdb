use std::fs::OpenOptions;
use std::io::Result;
use std::path::Path;
use std::sync::Arc;
use std::{mem, ptr};

use crate::file_system::file::raw_file::RawFile;
use crate::file_system::file::{asyncify, ReadableFile};

pub struct MmapFile {
    file: RawFile,
    mmap: Arc<memmap2::MmapRaw>,
    size: usize,
}

impl MmapFile {
    pub async fn open<P: AsRef<Path>>(path: P, options: OpenOptions) -> Result<MmapFile> {
        let path = path.as_ref().to_owned();
        let res = asyncify(move || {
            let file = RawFile(Arc::new(options.open(path)?));
            let mmap = Arc::new(memmap2::MmapRaw::map_raw(&file.0)?);
            let size = mmap.len();
            Ok(MmapFile { file, mmap, size })
        })
        .await?;
        Ok(res)
    }
}

#[async_trait::async_trait]
impl ReadableFile for MmapFile {
    async fn read_at(&self, pos: usize, data: &mut [u8]) -> Result<usize> {
        let mmap = self.mmap.clone();
        let size = self.size;
        let len = data.len();
        let dst = data.as_ptr() as usize;
        let size = asyncify(move || {
            unsafe {
                let memory = std::slice::from_raw_parts(mmap.as_ptr(), size);
                let src = memory.as_ptr().add(pos);
                ptr::copy_nonoverlapping(src, dst as *mut u8, len);
            }
            Ok(len)
        })
        .await?;
        Ok(size)
    }

    fn file_size(&self) -> Result<usize> {
        self.file.file_size()
    }
}

#[cfg(test)]
mod test {
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::path::PathBuf;

    use memmap2::MmapRaw;

    use crate::file_system::file::asyncify;

    #[tokio::test]
    async fn test_mmap_raw() {
        let tempdir = tempfile::tempdir().unwrap();
        let path: PathBuf = tempdir.path().join("flush");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        file.set_len(128).unwrap();

        let mmap = MmapRaw::map_raw(&file).unwrap();

        asyncify(move || {
            unsafe {
                let mut memory = std::slice::from_raw_parts_mut(mmap.as_mut_ptr(), 128);
                memory.write_all(b"Hello, world!").unwrap();
                mmap.flush().unwrap();
            }
            Ok(())
        })
        .await
        .unwrap();
    }
}
