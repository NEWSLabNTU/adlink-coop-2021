#![allow(dead_code, unused_variables)]
use noisy_float::types::R64;
use std::{collections::VecDeque, ops::Deref};
use uhlc::Timestamp;

use crate::common::*;
use edcert::certificate::Certificate;
use textnonce::TextNonce;
// use maplit::hashset;
use sha2::{Digest, Sha256};

type Sha256Hash = Vec<u8>;
type Signature = Vec<u8>;

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpatialRange {
    pub range: f64,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct RBInfo {
    pub deadline: Timestamp,
    pub spatial_range: SpatialRange,
}
impl RBInfo {
    pub fn validate(&self) -> bool {
        if self.spatial_range.range > 0.0 {
            return true;
        }
        return false;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Priority {
    FirstPathHigher,
    SecondPathHigher,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingChart {
    pub paths: VecDeque<Path>,
    pub conflicting_paths: HashSet<(Path, Path, Priority)>,
}

impl RoutingChart {
    pub fn validate(&self) -> bool {
        if self.paths.len() == 0 {
            return false;
        }
        for (k, v, _) in &self.conflicting_paths {
            if !self.paths.contains(k) {
                return false;
            }
            if !self.paths.contains(v) {
                return false;
            }
        }
        return true;
    }
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
            Self::Init(block) => block.timestamp.clone(),
            Self::Phantom(block) => block.timestamp.clone(),
            Self::RoutingChart(block) => block.timestamp.clone(),
            Self::Decision(block) => block.timestamp.clone(),
        }
    }

    pub fn validate(&self, last_block: &Block) -> bool {
        match self {
            Self::Init(block) => true,
            Self::Phantom(block) => true,
            Self::RoutingChart(block) => block.validate(last_block),
            Self::Decision(block) => block.validate(last_block),
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
    pub fn validate(&self, last_block: &Block) -> bool {
        let last_block = match last_block {
            Block::RoutingChart(block) => Some(block),
            _ => None,
        };
        if let Some(last_block) = last_block {
            if !(self.prev_hash == last_block.curr_hash) {
                warn!("prev_hash does not match the hash in last_block");
                return false;
            }
            if !self.routing_chart.validate() {
                warn!("Routing Chart is invalid");
                return false;
            }
            if self.timestamp < last_block.timestamp {
                warn!("Block timestamp is smaller than that of last_block");
                return false;
            }
            return true;
        } else {
            warn!("Block Type incorrect. Requires last_block to be type 'Block::RoutingChart'");
            return false;
        }
    }

    pub fn new(
        prev_hash: Sha256Hash,
        timestamp: Timestamp,
        routing_chart: RoutingChart,
        cert: &Certificate,
    ) -> RoutingChartBlock {
        let mut hasher = Sha256::new();
        hasher.update(&prev_hash);
        let routing_chart_bin = bincode::serialize(&routing_chart).unwrap();
        hasher.update(routing_chart_bin);
        let timestamp_bin = bincode::serialize(&timestamp).unwrap();
        hasher.update(timestamp_bin);
        let curr_hash = hasher.finalize().to_vec();
        let signature = cert.sign(&curr_hash).unwrap();
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
// Todo: Add pointer to RoutingChartBlock

impl DecisionBlock {
    pub fn validate(&self, last_block: &Block) -> bool {
        let last_block = match last_block {
            Block::Decision(block) => Some(block),
            _ => None,
        };
        if let Some(last_block) = last_block {
            if !(self.prev_hash == last_block.curr_hash) {
                warn!("prev_hash does not match the hash in last_block");
                return false;
            }
            if !self.decision.validate() {
                warn!("Decision is invalid");
                return false;
            }

            if !self.rb_info.validate() {
                warn!("RBInfo is invalid");
                return false;
            }

            if self.timestamp < last_block.timestamp {
                warn!("Block timestamp is smaller than that of last_block");
                return false;
            }
            return true;
        } else {
            warn!("Block Type incorrect. Requires last_block to be type 'Block::Decision'");
            return false;
        }
    }
    pub fn new(
        prev_hash: Sha256Hash,
        timestamp: Timestamp,
        decision: Decision,
        rb_info: RBInfo,
        cert: &Certificate,
    ) -> DecisionBlock {
        let mut hasher = Sha256::new();
        hasher.update(&prev_hash);
        let timestamp_bin = bincode::serialize(&timestamp).unwrap();
        hasher.update(timestamp_bin);
        let decision_bin = bincode::serialize(&decision).unwrap();
        hasher.update(decision_bin);
        let rb_info_bin = bincode::serialize(&rb_info).unwrap();
        hasher.update(rb_info_bin);
        let curr_hash = hasher.finalize().to_vec();
        let signature = cert.sign(&curr_hash).unwrap();
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Eq)]
pub enum Action {
    Stop,
    Proceed(Path),
}

/// decision_map: node (vehicle_id) -> (action, start_time, end_time)
#[derive(Debug, Serialize, Deserialize)]
pub struct Decision {
    pub decision_map: HashMap<usize, (Action, Timestamp, Timestamp)>,
}
impl Decision {
    pub fn validate(&self) -> bool {
        let len_decision_map = self.decision_map.len();
        for (_, (action, start_time, end_time)) in &self.decision_map {
            todo!("Get routing chart to find conflicting paths with current path, and check if there exists a conflicting path with overlapping timestamp");
        }
        return true;
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
    pub fn new(timestamp: Timestamp, cert: &Certificate) -> Self {
        let text_nonce = TextNonce::new();
        let mut hasher = Sha256::new();
        hasher.update(text_nonce.into_string());
        let nonce: Sha256Hash = hasher.finalize().to_vec();
        let signature = cert.sign(&nonce).unwrap();

        let init_block = Block::Init(InitBlock {
            nonce: nonce.clone(),
            timestamp,
            signature,
        });

        Self {
            logs: vec![nonce.clone()],
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
    hlc_instance: HLC,
}

impl RSU {
    pub fn new(cert: Certificate, key: zenoh::Path) -> RSU {
        let hlc_instance = HLC::default();
        let timestamp = hlc_instance.new_timestamp();
        RSU {
            route_chain: BlockChain::new(timestamp, &cert),
            decision_chain: BlockChain::new(timestamp, &cert),
            key,
            cert,
            hlc_instance,
        }
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
