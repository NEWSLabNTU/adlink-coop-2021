use crate::common::*;

// pub mod serde_peer_id {
//     use super::*;

//     pub fn serialize<S>(id: &PeerId, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: Serializer,
//     {
//         format!("{}", id).serialize(serializer)
//     }

//     pub fn deserialize<'de, D>(deserializer: D) -> Result<PeerId, D::Error>
//     where
//         D: Deserializer<'de>,
//     {
//         let text = String::deserialize(deserializer)?;
//         let timestamp = PeerId::from_str(&text)
//             .map_err(|err| D::Error::custom(format!("invalid peer ID '{}': {:?}", text, err)))?;
//         Ok(timestamp)
//     }
// }
