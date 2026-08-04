//! Correctness model for the topic transaction SLO.
//!
//! Each partition contains one unconsumed [`ChainEvent`]. A transaction consumes
//! that event, stores the corresponding [`ChainTransition`], and writes the next
//! generation back to the same partition. After workers stop, committed topic
//! offsets, topic end offsets, and table transitions must describe the same
//! contiguous chain.

mod chain;
mod params;

pub use chain::{ChainEvent, ChainTransition, MessageCoordinate, PartitionId, TopicOffset};
pub use params::{Params, parse_params};
