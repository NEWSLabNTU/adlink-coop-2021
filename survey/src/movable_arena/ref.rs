//! Smart pointers to the movable memory.

use crate::common::*;

/// Shareable reference to memory in arena.
#[derive(Debug, Clone)]
pub struct Ref {}

/// Exclusive reference to memory in arena.
#[derive(Debug)]
pub struct RefMut {}
