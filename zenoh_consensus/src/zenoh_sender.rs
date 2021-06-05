use crate::{common::*, utils::ValueExt};

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ZenohSender<T>
where
    T: Serialize,
{
    #[derivative(Debug = "ignore")]
    zenoh: Arc<Zenoh>,
    key: zenoh::Path,
    _phantom: PhantomData<T>,
}

impl<T> ZenohSender<T>
where
    T: Serialize,
{
    pub fn new(zenoh: Arc<Zenoh>, key: zenoh::Path) -> Self {
        Self {
            zenoh,
            key,
            _phantom: PhantomData,
        }
    }

    pub async fn send(&self, data: T) -> Result<()> {
        let msg = zenoh::Value::serialize_from(&data)?;
        let workspace = self.zenoh.workspace(None).await?;
        workspace.put(&self.key, msg).await?;
        Ok(())
    }
}

impl<T> Clone for ZenohSender<T>
where
    T: Serialize,
{
    fn clone(&self) -> Self {
        Self {
            zenoh: self.zenoh.clone(),
            key: self.key.clone(),
            _phantom: PhantomData,
        }
    }
}
