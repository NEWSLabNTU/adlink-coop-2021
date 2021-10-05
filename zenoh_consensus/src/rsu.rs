#![allow(dead_code, unused_variables)]
use crate::common::*;
use edcert::{certificate::Certificate, signature::Signature};
// use maplit::hashset;
// use sha2::{Digest, Sha256};

type Sha256Hash = Vec<u8>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VehicleType {
    pub vehicle_type: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Path {
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

pub struct RBInfo {
    pub deadline: HLC,
    pub spatial_range: SpatialRange,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Priority {
    FirstPathHigher,
    SecondPathHigher,
}

#[derive(Debug, Clone)]
pub struct RoutingChart {
    pub paths: Vec<Path>,
    pub conflicting_paths: HashSet<(Path, Path, Priority)>,
}

pub enum Block {
    Init(InitBlock),
    Phantom(PhantomBlock),
    RoutingChart(RoutingChartBlock),
    Decision(DecisionBlock),
}

impl Block {
    pub fn curr_hash(&self) -> &Sha256Hash {
        match self {
            Self::Init(block) => &block.nonce,
            Self::Phantom(block) => &block.curr_hash,
            Self::RoutingChart(block) => &block.curr_hash,
            Self::Decision(block) => &block.curr_hash,
        }
    }

    pub fn validate(&self) -> bool {
        match self {
            Self::Init(block) => true,
            Self::Phantom(block) => true,
            Self::RoutingChart(block) => block.validate(),
            Self::Decision(block) => block.validate(),
        }
    }
}

pub struct InitBlock {
    pub nonce: Sha256Hash,
    pub timestamp: HLC,
    pub signature: Signature,
}

pub struct PhantomBlock {
    pub curr_hash: Sha256Hash,
    pub timestamp: HLC,
    pub signature: Signature,
}

pub struct RoutingChartBlock {
    pub curr_hash: Sha256Hash,
    pub prev_hash: Sha256Hash,
    pub timestamp: HLC,
    pub routing_chart: RoutingChart,
}

impl RoutingChartBlock {
    pub fn validate(&self) -> bool {
        todo!();
    }
}

pub struct DecisionBlock {
    pub signature: Signature,
    pub curr_hash: Sha256Hash,
    pub prev_hash: Sha256Hash,
    pub timestamp: HLC,
    pub decision: Decision,
    pub rb_info: RBInfo,
}

impl DecisionBlock {
    pub fn validate(&self) -> bool {
        todo!();
    }
}

/// Actions of intentions vehicles have
/// Path indicates a possible path
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Stop,
    Proceed(Path),
}

/// decision_map: node -> (action, start_time, end_time)
pub struct Decision {
    pub decision_map: HashMap<Path, (Action, HLC, HLC)>,
}

pub struct PreDecision {
    pub proposals: Vec<Proposal>,
}

pub struct Proposal {
    pub sender: String,
    pub action: Action,
    pub start_time: HLC,
    pub end_time: HLC,
    pub latest_decision_block: Sha256Hash,
    pub latest_route_block: Sha256Hash,
}

pub struct BlockChain {
    pub logs: Vec<Sha256Hash>,
    pub set: HashMap<Sha256Hash, Block>,
}

impl BlockChain {
    pub fn new() -> Self {
        let nonce: Sha256Hash = vec![];

        let init_block = Block::Init(InitBlock {
            nonce,
            timestamp: todo!(),
            signature: todo!(),
        });

        Self {
            logs: vec![nonce],
            set: hashmap! {
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
    route_chain: BlockChain,
    decision_chain: BlockChain,
    key: zenoh::Path,
    cert: Certificate,
}

impl RSU {
    pub fn new(_cert: Certificate) -> RSU {
        todo!("Add implementation");
    }

    pub fn cert(self) -> Certificate {
        self.cert.clone()
    }

    pub fn query_log(duration: Duration) -> (Vec<DecisionBlock>, Vec<RoutingChartBlock>) {
        todo!("Add implementation");
    }

    pub fn combine(proposals: PreDecision) -> Decision {
        todo!("Add implementation");
    }

    pub fn verify(decision: Decision, prev_block: DecisionBlock) -> Decision {
        todo!("Add implementation");
    }
}
