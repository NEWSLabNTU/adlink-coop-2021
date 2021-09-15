#![allow(dead_code, unused_variables)]
use crate::common::*;
use edcert::{certificate::Certificate, signature::Signature};
use sha2::Sha256;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VehicleType {
    pub vehicle_type: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    pub speed_limit: f64,
    pub allowed_vehicles: Vec<VehicleType>,
    pub soft_timeout: Duration,
    pub hard_timeout: Duration,
    pub capacity: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SpatialRange {
    pub range: f64,
}

pub struct ParticipantSpec {
    pub deadline: HLC,
    pub spatial_range: SpatialRange,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Priority {
    FirstNodeHigher,
    SecondNodeHigher,
}

#[derive(Debug, Clone)]
pub struct Route {
    pub node: Node,
    pub edges: HashSet<(Node, Node, Priority)>,
}

pub enum Block {
    Init(InitBlock),
    Phantom(PhantomBlock),
    Route(RouteBlock),
    Decision(DecisionBlock),
}

impl Block {
    pub fn curr_hash(&self) -> &Sha256 {
        match self {
            Self::Init(block) => &block.nonce,
            Self::Phantom(block) => &block.curr_hash,
            Self::Route(block) => &block.curr_hash,
            Self::Decision(block) => &block.curr_hash,
        }
    }

    pub fn validate(&self) -> bool {
        match self {
            Self::Init(block) => true,
            Self::Phantom(block) => true,
            Self::Route(block) => block.validate(),
            Self::Decision(block) => block.validate(),
        }
    }
}

pub struct InitBlock {
    pub nonce: Sha256,
    pub timestamp: HLC,
    pub signature: Signature,
}

pub struct PhantomBlock {
    pub curr_hash: Sha256,
    pub timestamp: HLC,
    pub signature: Signature,
}

pub struct RouteBlock {
    pub curr_hash: Sha256,
    pub prev_hash: Sha256,
    pub timestamp: HLC,
    pub routes: Route,
}

impl RouteBlock {
    pub fn validate(&self) -> bool {
        todo!();
    }
}

pub struct DecisionBlock {
    pub signature: Signature,
    pub curr_hash: Sha256,
    pub prev_hash: Sha256,
    pub timestamp: HLC,
    pub decision: Decision,
    pub participant_spec: ParticipantSpec,
}

impl DecisionBlock {
    pub fn validate(&self) -> bool {
        todo!();
    }
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
    pub decision_map: HashMap<Node, (Choice, HLC, HLC)>,
}

pub struct PreDecision {
    pub proposals: Vec<Proposal>,
}

pub struct Proposal {
    pub sender: String,
    pub node: Choice,
    pub start_time: HLC,
    pub end_time: HLC,
    pub latest_decision_block: Sha256,
    pub latest_route_block: Sha256,
}

pub struct BlockChain {
    pub logs: Vec<Sha256>,
    pub set: HashMap<Sha256, Block>,
}

impl BlockChain {
    pub fn new() -> Self {
        let nonce: Sha256 = todo!();

        let init_block = InitBlock {
            nonce,
            timestamp: todo!(),
            signature: todo!(),
        };

        Self {
            logs: vec![nonce],
            set: hashset! {
                nonce => init_block
            },
        }
    }

    pub fn append_block(block: Block) -> Result<()> {
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
    cert: Certificate,
}

impl RSU {
    pub fn new(_cert: Certificate) -> RSU {
        todo!("Add implementation");
    }

    pub fn cert() -> &Certificate {
        &self.cert
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
