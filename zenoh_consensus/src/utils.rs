use crate::common::*;

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
