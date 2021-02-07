//! The movable memory.

use super::block::{Block, BlockId};
use crate::common::*;

/// The arena initializer.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ArenaInit {
    pub block_size: NonZeroUsize,
}

impl ArenaInit {
    /// Build a memory arena.
    pub fn build(self) -> Result<Arena> {
        let Self { block_size } = self;
        let block_size = block_size.get();

        Ok(Arena {
            last_block_id: AtomicUsize::new(0),
            block_size,
            blocks: HashMap::new(),
            free_list: Arc::new(RwLock::new(LinkedList::new())),
        })
    }
}

/// The memory arena.
#[derive(Debug)]
pub struct Arena {
    /// The latest unused block index.
    pub last_block_id: AtomicUsize,
    /// The size when each block is allocated.
    pub block_size: usize,
    /// The collection of indexed blocks.
    pub blocks: HashMap<BlockId, Block>,
    /// The linked list of free contiguous memory regions.
    pub free_list: Arc<RwLock<LinkedList<FreeEntry>>>,
}

impl Arena {
    pub fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        todo!();
    }

    pub unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        todo!();
    }

    pub fn grow(&self, size: usize) {
        let num_blocks = (size + self.block_size - 1) / self.block_size;
    }
}

/// An entry of free memory region.
#[derive(Debug)]
pub struct FreeEntry {
    /// The referenced block index.
    pub block_id: BlockId,
    /// The offset in referenced block.
    pub offset: usize,
    /// The length of the referenced region.
    pub len: usize,
}
