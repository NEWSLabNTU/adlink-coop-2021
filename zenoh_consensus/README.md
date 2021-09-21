# Zenoh Consensus

## Reliable Broadcast

### Prerequisite

Please check out chapter I to V of the paper: [Byzantine Agreement with Unknown Participants
and Failures](https://arxiv.org/abs/2102.10442) for the theory of Reliable Broadcast. The implementation is based on the algorithm provided in the paper.

### Sample Code

Please checkout the [mod test](https://github.com/eurc17/zenoh_library/blob/307e66325b2a405cc26956ec1f0cb78abb973a38/zenoh_consensus/src/reliable_broadcast.rs#L747) section in [reliable_broadcast.rs](https://github.com/eurc17/zenoh_library/blob/307e66325b2a405cc26956ec1f0cb78abb973a38/zenoh_consensus/src/reliable_broadcast.rs) for sample code.

### Usage

`reliable_broadcast::new(zenoh: Arc<Zenoh>, path: impl Borrow<zenoh::Path>, id: impl AsRef<str>, config: Config,) -> Result<(Sender<T>, Receiver<Msg<T>>)> ` :

Function:

This function will create a transmitter and receiver for a peer. You can operate the peer by sending and receiving messages using the obtained transmitter and receiver. The peer ID is set in the function. Note: Each peer under the **same workspace** should have an **UNIQUE** ID.

Input parameters:

1. `zenoh` : A `zenoh::Zenoh` instance wrapped inside `Arc`.
2. `path` : A `zenoh::Path` structure that points to the workspace the peer will be joining in.
3. `id` : A `String` that identifies the peer.
4. `config` : A `reliable_broadcast::Config`, which contains the following information:
   - `max_rounds` : Maximum number of rounds (see definition of round in the [paper](https://arxiv.org/abs/2102.10442)) for each Reliable Broadcast.
   - `extra_rounds` : Number of extra rounds for the peers that has accepted the message to continue sending echo(msg).
   - `recv_timeout` : The timeout for receiving the first 1/3 * Nv echoes. (see definition of Nv in the [paper](https://arxiv.org/abs/2102.10442)).
   - `round_timeout` : The timeout for the whole Reliable Broadcast. All peers should accept the message or reject the message before this timeout.

Return value:

`(tx, rx)` : A pair of transmitter and receiver for the peer, which sends and receives a `u8` instance. 

To send a message, please `await` on `tx.send(data)`, where `data` should be a `u8` instance.

To receive a message, please `await` on `rx.recv()`, which will return a `Result` that contains the received message. The message is of type `reliable_broadcast::Msg<u8>`, which contains the following fields:

* `sender` : The ID of the peer who sent the message.
* `seq` : The sequence number that identifies the message.
* `data` : The `u8` instance sent by Reliable Broadcast.

### Notes

*  Please ensure that all peers begins a round at approximately the same time so that the message is sent within the timeout of all peers.
* It is highly recommended to `await` on `rx.recv()` with a timeout, in case no messages are sent in a round.

### Suggestion

It is highly recommended to wrap the asynchronous operations of `tx` and `rx` into futures that can be polled. Please take some time going through the sample code for the above mentioned operation. 