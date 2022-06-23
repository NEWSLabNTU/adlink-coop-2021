use crate::{common::*, message::MessageT, state::State};

#[derive(Clone)]
pub struct Sender<T>
where
    T: MessageT,
{
    pub(super) state: Arc<State<T>>,
}

impl<T> Sender<T>
where
    T: MessageT,
{
    pub fn id(&self) -> Uuid {
        self.state.my_id
    }

    pub async fn send(&self, data: T) -> Result<(), Error> {
        self.state.clone().broadcast(data).await?;
        Ok(())
    }

    pub fn into_sink(self) -> impl Sink<T, Error = Error> {
        sink::unfold(self, |sender, data| async move {
            sender.send(data).await.map(|()| sender)
        })
    }
}
