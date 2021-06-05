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
