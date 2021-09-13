#![allow(dead_code, unused_variables)]
use crate::common::*;
use edcert::{certificate::Certificate, signature::Signature};
use sha2::Sha256;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VehicleType {
    vehicle_type: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    speed_limit: f64,
    allowed_vehicles: Vec<VehicleType>,
    soft_timeout: Duration,
    hard_timeout: Duration,
    capacity: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SpatialRange {
    range: f64,
}

pub struct ParticipantSpec {
    deadline: HLC,
    spatial_range: SpatialRange,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Priority {
    FirstNodeHigher,
    SecondNodeHigher,
}

#[derive(Debug, Clone)]
pub struct Route {
    node: Node,
    edges: HashSet<(Node, Node, Priority)>,
}

pub struct InitBlock {
    nonce: Sha256,
    timestamp: HLC,
    signature: Signature,
}

pub struct PhantomBlock {
    curr_hash: Sha256,
    timestamp: HLC,
    signature: Signature,
}

pub struct RouteBlock {
    curr_hash: Sha256,
    prev_hash: Sha256,
    timestamp: HLC,
    routes: Route,
}

/// Choices of intentions vehicles have
/// Node indicates a possible path
#[derive(Debug, Clone, PartialEq)]
pub enum Choice {
    Stop,
    Proceed(Node),
}

/// decision_map: node -> (choice, start_time, end_time)
pub struct Decision {
    decision_map: HashMap<Node, (Choice, HLC, HLC)>,
}

pub struct PreDecision {
    proposals: Vec<Proposal>,
}

pub struct Proposal {
    sender: String,
    node: Choice,
    start_time: HLC,
    end_time: HLC,
    latest_decision_block: Sha256,
    latest_route_block: Sha256,
}

pub struct DecisionBlock {
    signature: Signature,
    curr_hash: Sha256,
    prev_hash: Sha256,
    timestamp: HLC,
    decision: Decision,
    participant_spec: ParticipantSpec,
}
pub struct RouteChain {
    logs: Vec<RouteBlock>,
    set: HashSet<RouteBlock>,
}

impl RouteChain {
    pub fn init() -> RouteChain {
        todo!("Add implementation");
    }

    pub fn append_block() -> Result<()> {
        todo!("Add implementation");
    }

    pub fn condense_log() -> Result<()> {
        todo!("Add implementation");
    }
}

pub struct DecisionChain {
    logs: Vec<DecisionBlock>,
    set: HashSet<DecisionBlock>,
}

impl DecisionChain {
    pub fn init() -> DecisionChain {
        todo!("Add implementation");
    }

    pub fn append_block() -> Result<()> {
        todo!("Add implementation");
    }

    pub fn condense_log() -> Result<()> {
        todo!("Add implementation");
    }
}

pub struct RSU {
    route_chain: RouteChain,
    decision_chain: DecisionChain,
    key: zenoh::Path,
}

impl RSU {
    pub fn init() -> RSU {
        todo!("Add implementation");
    }

    pub fn cert() -> Certificate {
        todo!("Add implementation");
    }

    pub fn query_log(duration: Duration) -> (Vec<DecisionBlock>, Vec<RouteBlock>) {
        todo!("Add implementation");
    }

    pub fn combine(proposals: PreDecision) -> Decision {
        todo!("Add implementation");
    }

    pub fn verify(decision: Decision, prev_block: DecisionBlock) -> Decision {
        todo!("Add implementation");
    }
}
