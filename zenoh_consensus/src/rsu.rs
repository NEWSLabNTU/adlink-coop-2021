#![allow(dead_code, unused_variables)]
use noisy_float::types::R64;
use std::ops::Deref;
use uhlc::Timestamp;

use crate::common::*;
use edcert::{certificate::Certificate, signature::Signature};
use textnonce::TextNonce;
// use maplit::hashset;
use sha2::{Digest, Sha256};

type Sha256Hash = Vec<u8>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VehicleType {
    pub vehicle_type: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Eq, Hash)]
pub struct Path {
    pub id: usize,
    pub speed_limit: u64,
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
    pub deadline: Timestamp,
    pub spatial_range: SpatialRange,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Priority {
    FirstPathHigher,
    SecondPathHigher,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

    pub fn get_signature(&self) -> &Signature {
        match self {
            Self::Init(block) => &block.signature,
            Self::Phantom(block) => &block.signature,
            Self::RoutingChart(block) => &block.signature,
            Self::Decision(block) => &block.signature,
        }
    }

    pub fn get_timestamp(&self) -> Timestamp {
        match self {
            Self::Init(block) => todo!("Solve HLC clone/copy issue"),
            Self::Phantom(block) => todo!("Solve HLC clone/copy issue"),
            Self::RoutingChart(block) => todo!("Solve HLC clone/copy issue"),
            Self::Decision(block) => todo!("Solve HLC clone/copy issue"),
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
    pub timestamp: Timestamp,
    pub signature: Signature,
}

pub struct PhantomBlock {
    pub curr_hash: Sha256Hash,
    pub timestamp: Timestamp,
    pub signature: Signature,
}

pub struct RoutingChartBlock {
    pub curr_hash: Sha256Hash,
    pub prev_hash: Sha256Hash,
    pub timestamp: Timestamp,
    pub routing_chart: RoutingChart,
    pub signature: Signature,
}

impl RoutingChartBlock {
    pub fn validate(&self) -> bool {
        todo!();
    }

    pub fn new(
        prev_hash: Sha256Hash,
        timestamp: Timestamp,
        routing_chart: RoutingChart,
    ) -> RoutingChartBlock {
        let mut hasher = Sha256::new();
        hasher.update(prev_hash);
        let routing_chart_bin = bincode::serialize(&routing_chart).unwrap();
        hasher.update(routing_chart_bin);
        todo!("Hash timestamp");
        let curr_hash = hasher.finalize().to_vec();
        let signature = todo!("Fill in signature");
        RoutingChartBlock {
            curr_hash,
            prev_hash,
            timestamp,
            routing_chart,
            signature,
        }
    }
}

pub struct DecisionBlock {
    pub signature: Signature,
    pub curr_hash: Sha256Hash,
    pub prev_hash: Sha256Hash,
    pub timestamp: Timestamp,
    pub decision: Decision,
    pub rb_info: RBInfo,
}

impl DecisionBlock {
    pub fn validate(&self) -> bool {
        todo!();
    }
    pub fn new(
        prev_hash: Sha256Hash,
        timestamp: Timestamp,
        decision: Decision,
        rb_info: RBInfo,
    ) -> DecisionBlock {
        let mut hasher = Sha256::new();
        hasher.update(prev_hash);
        todo!("Hash timestamp, decision, and rb_info, requires HLC to be Serializable");
        let curr_hash = hasher.finalize().to_vec();
        let signature = todo!("Fill in signature");
        DecisionBlock {
            signature,
            curr_hash,
            prev_hash,
            timestamp,
            decision,
            rb_info,
        }
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
    pub decision_map: HashMap<Path, (Action, Timestamp, Timestamp)>,
}
impl Decision {
    pub fn validate(&self) -> bool {
        todo!();
    }
}

pub struct PreDecision {
    pub proposals: Vec<Proposal>,
}

pub struct Proposal {
    pub sender: String,
    pub action: Action,
    pub start_time: Timestamp,
    pub end_time: Timestamp,
    pub latest_decision_block: Sha256Hash,
    pub latest_route_block: Sha256Hash,
}

pub struct BlockChain {
    pub logs: Vec<Sha256Hash>,
    pub set: HashMap<Sha256Hash, Block>,
}

impl BlockChain {
    pub fn new() -> Self {
        let text_nonce = TextNonce::new();
        let mut hasher = Sha256::new();
        hasher.update(text_nonce.into_string());
        let nonce: Sha256Hash = hasher.finalize().to_vec();

        let init_block = Block::Init(InitBlock {
            nonce,
            timestamp: todo!("Fill timestamp"),
            signature: todo!("Fill signature"),
        });

        Self {
            logs: vec![nonce],
            set: hashmap! {
                nonce => init_block
            },
        }
    }

    pub fn append_block(mut self, block: Block) -> Result<()> {
        let curr_block_hash = block.curr_hash();
        self.logs.push(curr_block_hash.clone());
        self.set.insert(curr_block_hash.clone(), block);
        Ok(())
    }

    pub fn condense_log(mut self, num_blocks_to_remain: usize) -> Result<()> {
        if self.logs.len() <= num_blocks_to_remain {
            Ok(())
        } else {
            let id_block_to_condense = self.logs.len() - 1 - num_blocks_to_remain;
            let hash_block_to_condense = self.logs[id_block_to_condense].clone();
            let phantom_block = {
                let block_to_condense = self.set.get(&hash_block_to_condense).unwrap();
                Block::Phantom(PhantomBlock {
                    curr_hash: hash_block_to_condense.clone(),
                    timestamp: block_to_condense.get_timestamp(),
                    signature: block_to_condense.get_signature().clone(),
                })
            };
            //create new log that starts with phantom block
            let new_log = self.logs.split_off(id_block_to_condense);
            self.set.insert(hash_block_to_condense, phantom_block);
            // remove blocks that are being condensed
            for hash_value in self.logs.iter() {
                self.set.remove(hash_value);
            }
            self.logs = new_log;
            Ok(())
        }
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
