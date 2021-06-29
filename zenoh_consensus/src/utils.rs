use crate::common::*;

pub async fn timeout_until<F, T>(until: Instant, f: F) -> Result<T, async_std::future::TimeoutError>
where
    F: Future<Output = T>,
{
    let timeout = until.saturating_duration_since(Instant::now());
    async_std::future::timeout(timeout, f).await
}

pub trait ValueExt {
    fn serialize_from<T>(value: &T) -> Result<Value>
    where
        T: Serialize;

    fn deserialize_to<'a, T>(&'a self) -> Result<T>
    where
        T: Deserialize<'a>;
}

impl ValueExt for Value {
    fn serialize_from<T>(value: &T) -> Result<Value>
    where
        T: Serialize,
    {
        let text = serde_json::to_string(value)?;
        Ok(Value::Json(text))
    }

    fn deserialize_to<'a, T>(&'a self) -> Result<T>
    where
        T: Deserialize<'a>,
    {
        let text = match self {
            Value::Json(text) => text,
            _ => bail!("invalid value"),
        };
        let value: T = serde_json::from_str(&text)?;
        Ok(value)
    }
}

pub trait NTP64Ext {
    fn from_u64(value: u64) -> Self;
}

impl NTP64Ext for uhlc::NTP64 {
    fn from_u64(value: u64) -> Self {
        let secs = value >> 32;
        let subsec_nanos = ((value & 0xFFFF_FFFFu64) * 1_000_000_000 / (1u64 << 32)) as u32;
        let duration = Duration::new(secs, subsec_nanos);
        duration.into()
    }
}

pub fn hash_uhlc_timestamp<H>(timestamp: &uhlc::Timestamp, state: &mut H)
where
    H: Hasher,
{
    timestamp.get_id().hash(state);
    hash_uhlc_ntp64(timestamp.get_time(), state);
}

pub fn hash_uhlc_ntp64<H>(timestamp: &uhlc::NTP64, state: &mut H)
where
    H: Hasher,
{
    timestamp.as_u64().hash(state);
}

pub mod serde_uhlc_timestamp {
    use super::*;

    pub fn serialize<S>(timestamp: &uhlc::Timestamp, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        format!("{}", timestamp).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<uhlc::Timestamp, D::Error>
    where
        D: Deserializer<'de>,
    {
        let text = String::deserialize(deserializer)?;
        let timestamp = uhlc::Timestamp::from_str(&text)
            .map_err(|err| D::Error::custom(format!("invalid timestamp '{}': {:?}", text, err)))?;
        Ok(timestamp)
    }
}

pub mod serde_uhlc_ntp64 {
    use super::*;

    pub fn serialize<S>(timestamp: &uhlc::NTP64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        timestamp.as_u64().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<uhlc::NTP64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        Ok(NTP64::from_u64(value))
    }
}

pub mod serde_uhlc_id {
    use super::*;

    pub fn serialize<S>(id: &uhlc::ID, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        id.as_slice().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<uhlc::ID, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        let len = bytes.len();

        if len > ID::MAX_SIZE {
            return Err(D::Error::custom(format!(
                "invalid ID: the length of ID is at most {} bytes",
                ID::MAX_SIZE
            )));
        }

        let mut array = [0; ID::MAX_SIZE];
        array[0..len].copy_from_slice(&bytes);

        Ok(ID::new(len, array))
    }
}
