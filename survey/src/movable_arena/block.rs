//! Units of allocated memory.

use crate::common::*;

/// The unique block index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockId(usize);

/// The block initializer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockInit {
    /// The assigned block index to the block to be built.
    pub id: BlockId,
    /// The block of size that will be allocated.
    pub size: usize,
}

impl BlockInit {
    /// Allocates a block according to the parameters.
    pub fn allocate(self) -> Result<Block> {
        let Self { id, size } = self;
        let mmap = MmapMut::map_anon(size)?;

        Ok(Block {
            id,
            size,
            ref_count: AtomicUsize::new(0),
            mmap,
        })
    }
}

/// A block of contiguous movable memory.
#[derive(Debug)]
pub struct Block {
    /// The block index.
    pub id: BlockId,
    /// The size of the block.
    pub size: usize,
    /// The reference count to the block.
    pub ref_count: AtomicUsize,
    /// The anonymous mmap memory.
    pub mmap: MmapMut,
}

impl Block {
    pub fn ptr(&self) -> NonNull<[u8]> {
        unsafe {
            // the lifetime of this ptr is bounded by self.mmap
            let ptr = self.mmap.as_ref() as *const [u8] as *mut [u8];
            NonNull::new_unchecked(ptr)
        }
    }
}

impl Drop for Block {
    fn drop(&mut self) {
        // panics if ref_count is not zero when dropped
        assert_eq!(self.ref_count.load(Ordering::Relaxed), 0);
    }
}
