//! A module that performs operations related to messages.

use crate::{common::*, utils};

/// The identifier for a broadcast.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BroadcastId {
    /// The ID of the message sender of (m, s).
    #[serde(with = "utils::serde_peer_id")]
    pub peer_id: PeerId,
    /// The sequence number of the message.
    pub seq: ZInt,
}

impl Display for BroadcastId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}/{}", self.peer_id, self.seq)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// The enumeration of all types of messages used in reliable broadcast.
pub enum Message<T> {
    /// The message type of *(m, s)*.
    Broadcast(Broadcast<T>),
    /// The message type of *present*.
    Present(Present),
    /// The message type of *echo(m, s)*
    Echo(Echo),
}

impl<T> From<Broadcast<T>> for Message<T> {
    fn from(from: Broadcast<T>) -> Self {
        Self::Broadcast(from)
    }
}

impl<T> From<Present> for Message<T> {
    fn from(from: Present) -> Self {
        Self::Present(from)
    }
}

impl<T> From<Echo> for Message<T> {
    fn from(from: Echo) -> Self {
        Self::Echo(from)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Derivative, Serialize, Deserialize)]
#[derivative(Hash)]
/// The structure for the message type *(m, s)*.
pub struct Broadcast<T> {
    /// The data to send in the message.
    pub data: T,
}

#[derive(Debug, Clone, PartialEq, Eq, Derivative, Serialize, Deserialize)]
#[derivative(Hash)]
/// The structure for the message type *echo(m, s)*
pub struct Echo {
    pub broadcast_ids: Vec<BroadcastId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Derivative, Serialize, Deserialize)]
#[derivative(Hash)]
/// The structure for the message type *present*.
pub struct Present {}
