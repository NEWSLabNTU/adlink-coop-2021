#![allow(dead_code, unused_variables)]
use itertools::Itertools;
use std::collections::VecDeque;
use uhlc::Timestamp;

use crate::common::*;
use edcert::certificate::Certificate;
use textnonce::TextNonce;
// use maplit::hashset;
use sha2::{Digest, Sha256};

type Sha256Hash = Vec<u8>;
type Signature = Vec<u8>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
/// Defines the type of vehicle.
pub struct VehicleType {
    pub vehicle_type: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Eq, Hash)]
/// Defines a path that vehicle can take.
pub struct Path {
    /// The ID of the [Path].
    pub id: usize,
    /// The speed limit of the [Path].
    pub speed_limit: u64,
    /// The types of vehicles that can drive on the [Path].
    pub allowed_vehicles: Vec<VehicleType>,
    /// The duration that a vehicle can be staying on the [Path]. Vehicles can extend the duration.
    pub soft_timeout: Duration,
    /// The duration that a vehicle can be staying on the [Path]. Vehicles can <span style="color:red">NOT</span> extend the duration.
    pub hard_timeout: Duration,
    /// The number of vehicles allowed to be on the [Path] simultaneously.
    pub capacity: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// The range in which vehicles should start participating in Reliable Broadcast.
pub struct SpatialRange {
    pub range: f64,
}
#[derive(Debug, Serialize, Deserialize)]
/// The information of Reliable Broadcast
pub struct RBInfo {
    /// The starting time of the reliable_broadcast
    pub start_time: Timestamp,
    /// The deadline of the Reliable Broadcast
    pub deadline: Timestamp,
    /// The range in which vehicles should start participating in Reliable Broadcast.
    pub spatial_range: SpatialRange,
}
impl RBInfo {
    /// A function that validates the [RBInfo].
    pub fn validate(&self) -> bool {
        if self.spatial_range.range > 0.0 {
            return true;
        }
        return false;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
/// Defines priority between two paths.
pub enum Priority {
    FirstPathHigher,
    SecondPathHigher,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Defines a set of paths that vehicles can take and the conflicting paths in the set of paths.
pub struct RoutingChart {
    /// A set of paths that vehicles can take.
    pub paths: VecDeque<Path>,
    /// A set of conflicting paths, with priority between them specified.
    pub conflicting_paths: HashMap<(Path, Path), Priority>,
}

impl RoutingChart {
    /// A function that validates the [RoutingChart].
    pub fn validate(&self) -> bool {
        // check if no paths
        if self.paths.len() == 0 {
            return false;
        }
        // check if conflicting paths are contained in paths
        for ((k, v), _) in &self.conflicting_paths {
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
/// Defines types of blocks in the Block Chain.
pub enum Block {
    Init(InitBlock),
    Phantom(PhantomBlock),
    RoutingChart(RoutingChartBlock),
    Decision(DecisionBlock),
}

impl Block {
    /// A function that fetches the current hash of the block.
    pub fn curr_hash(&self) -> &Sha256Hash {
        match self {
            Self::Init(block) => &block.nonce,
            Self::Phantom(block) => &block.curr_hash,
            Self::RoutingChart(block) => &block.curr_hash,
            Self::Decision(block) => &block.curr_hash,
        }
    }

    /// A function that fetches the signature of the block.
    pub fn get_signature(&self) -> &Signature {
        match self {
            Self::Init(block) => &block.signature,
            Self::Phantom(block) => &block.signature,
            Self::RoutingChart(block) => &block.signature,
            Self::Decision(block) => &block.signature,
        }
    }

    /// A function that fetches the timestamp of the block.
    pub fn get_timestamp(&self) -> Timestamp {
        match self {
            Self::Init(block) => block.timestamp.clone(),
            Self::Phantom(block) => block.timestamp.clone(),
            Self::RoutingChart(block) => block.timestamp.clone(),
            Self::Decision(block) => block.timestamp.clone(),
        }
    }

    /// A function that validates the [Block].
    pub fn validate(&self, last_block: &Block) -> bool {
        match self {
            Self::Init(block) => true,
            Self::Phantom(block) => true,
            Self::RoutingChart(block) => block.validate(last_block),
            Self::Decision(block) => block.validate(last_block),
        }
    }
}

/// The initial block in block chain.
///
/// This should always be the starting block in the chain before condensing.
pub struct InitBlock {
    /// Number used Once as the current hash for [InitBlock].
    pub nonce: Sha256Hash,
    /// The timestamp of the [InitBlock].
    pub timestamp: Timestamp,
    /// The signature on the `nonce` of the [InitBlock].
    pub signature: Signature,
}

/// The block used to represent several blocks when the chain is being condensed.
pub struct PhantomBlock {
    /// The hash of the current [PhantomBlock], obtained from the last block that is being condensed.
    pub curr_hash: Sha256Hash,
    /// The timestamp of the [PhantomBlock], obtained from the last block that is being condensed.
    pub timestamp: Timestamp,
    /// The signature on the `curr_hash` of the [PhantomBlock], obtained from the last block that is being condensed.
    pub signature: Signature,
}

/// The block that stores all [RoutingChart]s by appending new blocks that contains a modified [RoutingChart].
pub struct RoutingChartBlock {
    /// The hash of the current [RoutingChartBlock].
    pub curr_hash: Sha256Hash,
    /// The hash of the previous [RoutingChartBlock].
    pub prev_hash: Sha256Hash,
    /// The timestamp of the [RoutingChartBlock].
    pub timestamp: Timestamp,
    pub routing_chart: RoutingChart,
    /// The signature on the `curr_hash` of the [RoutingChartBlock].
    pub signature: Signature,
}

impl RoutingChartBlock {
    /// A function that validates the [RoutingChartBlock].
    ///
    /// * `last_block`: The last block on the chain.
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

    /// A function that creates a new [RoutingChartBlock].
    ///
    /// * `prev_hash` - The hash of the previous [RoutingChartBlock].
    /// * `timestamp` - The timestamp of the current [RoutingChartBlock].
    /// * `routing_chart` - The current [RoutingChart] to be stored in the [RoutingChartBlock].
    /// * `cert` - The certificate of the RSU for signing signatures.
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

/// The block that stores all the [Decision]s in a decision time window.
pub struct DecisionBlock {
    /// The signature on `curr_hash` of the [DecisionBlock].
    pub signature: Signature,
    /// The hash of the [DecisionBlock].
    pub curr_hash: Sha256Hash,
    /// The hash of the previous [DecisionBlock].
    pub prev_hash: Sha256Hash,
    /// The timestamp of the [DecisionBlock].
    pub timestamp: Timestamp,
    /// The [Decision]s collected in a decision time window.
    pub decision: Decision,
    /// The [RBInfo] for the next decision time window.
    pub rb_info: RBInfo,
    /// The hash of the [RoutingChartBlock] used in the [DecisionBlock].
    pub curr_routing_chart: Sha256Hash,
    /// The hash of he [RoutingChartBlock] that will be used in next [DecisionBlock].
    pub fut_routing_chart: Option<Sha256Hash>,
}

impl DecisionBlock {
    /// A function that validates the [DecisionBlock].
    ///
    /// * `last_block`: The last block on the chain.
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
    /// A function that creates a new [DecisionBlock].
    ///
    /// * `prev_hash` - The hash of the previous [DecisionBlock].
    /// * `timestamp` - The timestamp of the current [DecisionBlock].
    /// * `decision` - The [Decision]s collected in a decision time window.
    /// * `rb_info` - The [RBInfo] for the next reliable broadcast.
    /// * `cert` - The certificate of the RSU for signing signatures.
    /// * `curr_routing_chart`: The hash of the [RoutingChartBlock] used in the [DecisionBlock].
    /// * `fut_routing_chart`: The hash of the [RoutingChartBlock] that will be sed in the next [DecisionBlock].
    pub fn new(
        prev_hash: Sha256Hash,
        timestamp: Timestamp,
        decision: Decision,
        rb_info: RBInfo,
        cert: &Certificate,
        curr_routing_chart: Sha256Hash,
        fut_routing_chart: Option<Sha256Hash>,
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
            curr_routing_chart,
            fut_routing_chart,
        }
    }
}

/// Defines actions of intentions that vehicles can take
///
/// Path indicates a possible path
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Eq)]
pub enum Action {
    Stop,
    Proceed(Path),
}

#[derive(Debug, Serialize, Deserialize)]
/// A collection of decisions on what actions should each vehicle take with starting and ending time specified.
pub struct Decision {
    /// decision_map:  (vehicle_id) -> ([Action], start_time, end_time)
    pub decision_map: HashMap<usize, (Action, Timestamp, Timestamp)>,
    /// The routing chart used in the decision.
    pub curr_routing_chart: RoutingChart,
}
impl Decision {
    /// A function that validates the [Decision].
    pub fn validate(&self) -> bool {
        let len_decision_map = self.decision_map.len();
        for (decision_1, decision_2) in self.decision_map.iter().tuple_combinations() {
            let (id_1, (action_1, start_1, end_1)) = decision_1;
            let (id_2, (action_2, start_2, end_2)) = decision_2;
            if id_1 == id_2 {
                warn!("Multiple decisions from the same peer.");
                return false;
            }
            if (start_1 >= start_2 && start_1 <= end_2) || (end_1 >= start_2 && end_1 <= end_2) {
                if let Action::Proceed(path_1) = action_1 {
                    if let Action::Proceed(path_2) = action_2 {
                        if (self
                            .curr_routing_chart
                            .conflicting_paths
                            .contains_key(&(path_1.clone(), path_2.clone())))
                            || (self
                                .curr_routing_chart
                                .conflicting_paths
                                .contains_key(&(path_2.clone(), path_1.clone())))
                        {
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }
}

/// A collection of actions each vehicles propose to take, not yet merged into a non-conflicting [Decision].
pub struct PreDecision {
    pub proposals: Vec<Proposal>,
}

#[derive(Debug, Serialize, Deserialize)]
/// The action that an vehicle would like to take from a specific starting and ending time.
pub struct Proposal {
    /// The proposing vehicle.
    pub sender: String,
    /// The [Action] which the proposal want to take.
    pub action: Action,
    /// The starting time of the proposed [Action].
    pub start_time: Timestamp,
    /// The ending time of the proposed [Action].
    pub end_time: Timestamp,
    /// The latest [DecisionBlock] that the proposer have received from RSU.
    pub latest_decision_block: Sha256Hash,
    /// The latest [RoutingChartBlock] that the proposer have received from RSU.
    pub latest_route_block: Sha256Hash,
}

/// The block chain that chains [Block]s.
pub struct BlockChain {
    /// The vector that stores the hash of [Block]s in order.
    pub logs: Vec<Sha256Hash>,
    /// The set of [Block]s in the [BlockChain].
    pub set: HashMap<Sha256Hash, Block>,
}

impl BlockChain {
    /// A function that creates a new [BlockChain] with [InitBlock] in it.
    ///
    /// Returns a [BlockChain] instance.
    ///
    /// * `timestamp` - The timestamp for the [InitBlock].
    /// * `cert` - The certificate of the RSU.
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

    /// A function that appends a [Block] to the end of the [BlockChain].
    ///
    /// * `block` - The [Block] to append to the end of the [BlockChain].
    pub fn append_block(mut self, block: Block) -> Result<()> {
        let curr_block_hash = block.curr_hash();
        self.logs.push(curr_block_hash.clone());
        self.set.insert(curr_block_hash.clone(), block);
        Ok(())
    }

    /// A function that condense the [BlockChain] up to a certain block into a [PhantomBlock] and leave only a number of [Block]s in the [BlockChain].
    ///
    /// * `num_blocks_to_remain` - The number of blocks to be left in the [BlockChain].
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

    /// A function that returns the [Block] in the [BlockChain] given the hash of the [Block].
    /// If [Block] is not in the [BlockChain], `None` will be returned.
    /// * `block_hash`: The hash of the [Block] to look up in [BlockChain].
    pub fn get_block_by_hash(&self, block_hash: &Sha256Hash) -> Option<&Block> {
        if self.set.contains_key(block_hash) {
            return self.set.get(block_hash);
        } else {
            return None;
        }
    }

    /// A function that returns the last [Block] in the [BlockChain].
    /// `None` will be returned if the [BlockChain] contains no blocks.
    pub fn get_last_block(&self) -> Option<&Block> {
        if self.logs.len() == 0 {
            return None;
        } else {
            return self.get_block_by_hash(self.logs.last().unwrap());
        }
    }
}

/// An instance of Road Side Unit (RSU).
///
/// The RSU can create [BlockChain]s to store both [Decision] and [RoutingChart]. It also plays a role in monitoring the ongoing Reliable Broadcast and collects the [Proposal]s and generate [Decision] based on CRDT. The [Decision] will be available to vehicles so that they can verify if they have the same information os the RSU.
pub struct RSU {
    /// The [BlockChain] that contains [RoutingChart] information over time.
    route_chain: BlockChain,
    /// The [BlockChain] that contains [Decision] made over time.
    decision_chain: BlockChain,
    /// The Zenoh workspace that the RSU resides in.
    key: zenoh::Path,
    /// The certificate used to sign signatures for this RSU.
    cert: Certificate,
    /// The [HLC] instance used for timestamp generation.
    hlc_instance: HLC,
}

impl RSU {
    /// A function that creates a new RSU instance.
    ///
    /// * `cert` - The certificate for RSU to sign signatures with.
    /// * `key` - The Zenoh workspace specified for this RSU to reside in.
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

    /// A function that returns the certificate of the RSU for ID verification.
    pub fn cert(self) -> Certificate {
        self.cert.clone()
    }

    /// A function that returns blocks in [BlockChain]s for log query.
    pub fn query_log(duration: Duration) -> (Vec<DecisionBlock>, Vec<RoutingChartBlock>) {
        todo!("Add implementation");
    }

    /// A function that returns the latest [DecisionBlock] along with the latest [RoutingChartBlock].
    pub fn query_latest_blocks(self) -> (DecisionBlock, RoutingChartBlock) {
        todo!("Add implementation");
    }

    /// A function that turns possibly conflicting [PreDecision] into non-conflicting [Decision].
    pub fn combine(proposals: PreDecision) -> Decision {
        todo!("Add implementation");
    }

    /// A function that verifies if a [Decision] is not conflicting with previous [DecisionBlock].
    pub fn verify(decision: Decision, prev_block: DecisionBlock) -> Decision {
        todo!("Add implementation");
    }
}
