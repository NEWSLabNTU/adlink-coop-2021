use crate::{common::*, utils::ValueExt};

#[derive(Derivative)]
#[derivative(Debug)]
/// A structure that helps to send messages through zenoh.
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
    /// A function that creates a [ZenohSender].
    /// Returns a [ZenohSender].
    ///
    /// * `zenoh`: The [Zenoh] instance used to send the messages.
    /// * `key`: The [zenoh::Path] to send the messages to.
    pub fn new(zenoh: Arc<Zenoh>, key: zenoh::Path) -> Self {
        Self {
            zenoh,
            key,
            _phantom: PhantomData,
        }
    }
    /// The function that sends the messages.
    /// * `data`: The message payload.
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
