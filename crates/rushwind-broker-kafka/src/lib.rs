//! Kafka engine for the RushWind broker, over `samsa`, the
//! pure-Rust Kafka protocol client (keeping the Rust side free of
//! librdkafka and its native build tooling).
//!
//! # The wire behavior
//!
//! Publishes go through one producer per topic — a
//! one-topic-one-writer shape — built on first use and cached by
//! topic name. The payload becomes the record value, the message
//! headers ride as record headers, and an empty key maps to an
//! absent record key. The partition choice is a
//! least-loaded-by-bytes balancer: a running byte count
//! per partition, pick the smallest, first on ties. Per-topic
//! partition lists come from cluster metadata, fetched once per
//! topic on first use and frozen; a mid-run partition expansion
//! goes unnoticed (the partition list is never refetched).
//!
//! Subscribes join a fresh consumer group per subscription, with a
//! random group id
//! minted at subscribe time. The samsa group machinery
//! (find-coordinator, join, sync, round-robin self-assignment of
//! the single member) carries the delivery pump: each fetched
//! record surfaces as an event on its own topic carrying its
//! payload, its key (lossily decoded into the contract's string
//! key), its partition, and its offset. A group stream that ends —
//! a coordinator error, a rebalance — rejoins after a doubling
//! backoff capped at thirty seconds. Offsets auto-commit per fetched
//! batch inside samsa rather than after the handler returns, so
//! this is an implicit-acknowledgment engine and
//! [`Event::ack`] is a no-op.
//!
//! # Divergences
//!
//! - Message headers are produced but not surfaced: samsa's fetch
//!   path carries no headers, so delivered messages have empty
//!   header maps.
//! - A fresh group's uncommitted partitions start at offset zero
//!   (earliest). The contract has no per-subscribe options, so the
//!   start offset is not configurable.
//! - SASL/TLS dialer knobs, per-publish balancer
//!   selection, producer batch knobs (size, timeout, acks,
//!   compression), tracers, and the auto-create-topic-on-subscribe
//!   option are not exposed: the constructor takes addresses only
//!   and samsa's producer defaults stand. An empty address list
//!   falls back to the `127.0.0.1:9092` default.
//! - Unsubscribe drops the pump without a leave-group request; the
//!   coordinator reaps the member at the session timeout.
//! - Writer recreation and retry on a cached
//!   writer's failure are out of scope: flush failures stay inside
//!   samsa's producer task and surface on its response channel,
//!   which this engine drains and discards.
//!
//! # Testing
//!
//! Live conformance tests run against a real Apache Kafka broker
//! (KRaft single node) via the `live` feature; there is no
//! embedded Kafka for CI's unit lanes.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use rushwind_broker::{BoxFuture, Broker, BrokerError, Event, Handler, Message, Subscriber};
use samsa::prelude::Error as SamsaError;
use samsa::prelude::{
    BrokerAddress, BrokerConnection, ClusterMetadata, ConsumerGroupBuilder, Header, ProduceMessage,
    Producer, ProducerBuilder, TcpConnection,
};
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;

/// The fallback bootstrap host when the settings list is empty.
const DEFAULT_HOST: &str = "127.0.0.1";
/// The fallback bootstrap port when the settings list is empty.
const DEFAULT_PORT: u16 = 9092;
/// The metadata client identity, matching samsa's internal
/// defaults.
const CORRELATION_ID: i32 = 1;
/// The metadata client id, matching samsa's internal default.
const CLIENT_ID: &str = "samsa";

/// The counter backing the group-id fallback when entropy fails.
static FALLBACK_COUNTER: AtomicU64 = AtomicU64::new(0);

/// The bootstrap factory's settings wire shape for
/// [`KafkaBroker::from_settings`].
#[derive(serde::Deserialize)]
pub struct KafkaSettings {
    /// Bootstrap addresses as `host:port`. An empty or absent list
    /// falls back to the default localhost bootstrap.
    #[serde(default)]
    pub addrs: Vec<String>,
}

/// Per-topic partition state: the metadata-fetched partition list
/// and the running byte counts the least-loaded balancer picks
/// from.
struct PartitionState {
    /// Partition ids from cluster metadata, frozen at fetch time.
    partitions: Vec<i32>,
    /// Cumulative produced bytes per partition.
    bytes: Vec<AtomicU64>,
}

impl PartitionState {
    /// Picks the index of the partition with the fewest produced
    /// bytes so far, first index on ties.
    fn least_loaded(&self) -> usize {
        let mut lowest = 0;
        let mut lowest_bytes = self.bytes[0].load(Ordering::Relaxed);
        for (index, counter) in self.bytes.iter().enumerate().skip(1) {
            let candidate = counter.load(Ordering::Relaxed);
            if candidate < lowest_bytes {
                lowest = index;
                lowest_bytes = candidate;
            }
        }
        lowest
    }
}

/// Wraps a samsa error as a broker failure.
fn kafka_err(error: SamsaError) -> BrokerError {
    BrokerError::Failed(format!("kafka: {error}"))
}

/// Mints a random group id for a subscription's fresh consumer
/// group. An entropy failure falls back to a time-and-counter pair,
/// which only needs to be unique per subscribe.
fn random_group_id() -> String {
    let mut bytes = [0u8; 16];
    if getrandom::fill(&mut bytes).is_err() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        return format!(
            "rushwind-group-fallback-{nanos}-{}",
            FALLBACK_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
    }
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Maps a fetched record onto a contract event. Headers don't
/// survive the samsa fetch path; the key decodes lossily into the
/// contract's string key. Kafka records carry no id of their own, so
/// the key doubles as one — handlers get a stable identifier without
/// the native id field (upstream mapped Key onto both ID and Key for
/// the same reason).
fn to_event(record: samsa::prelude::ConsumeMessage) -> Event {
    let mut message = Message::from_payload(record.value.to_vec());
    message.key = String::from_utf8_lossy(&record.key).into_owned();
    message.id = message.key.clone();
    message.partition = Some(i64::from(record.partition_index));
    message.offset = Some(record.offset as i64);
    Event::new(record.topic_name, message)
}

struct Inner {
    /// Bootstrap addresses as parsed `host:port` pairs.
    addrs: Vec<BrokerAddress>,
    /// Cluster metadata for partition enumeration; `None` while
    /// disconnected.
    metadata: Mutex<Option<ClusterMetadata<TcpConnection>>>,
    /// Per-topic producer senders — the
    /// one-topic-one-writer map. The producer task behind each
    /// sender owns its own metadata copy.
    producers: Mutex<HashMap<String, mpsc::Sender<ProduceMessage>>>,
    /// Partition state per topic, fetched from metadata on first
    /// use and frozen thereafter.
    partitions: Mutex<HashMap<String, Arc<PartitionState>>>,
    connected: AtomicBool,
}

/// A Kafka-backed broker over samsa's protocol client.
pub struct KafkaBroker {
    inner: Arc<Inner>,
}

impl KafkaBroker {
    /// Builds an engine for the Kafka cluster at `addrs`
    /// (`host:port` bootstrap addresses). Malformed entries are
    /// dropped; an empty result falls back to the
    /// default localhost bootstrap.
    pub fn new(addrs: Vec<String>) -> Self {
        let mut addrs: Vec<BrokerAddress> = addrs
            .iter()
            .filter_map(|addr| addr.split_once(':'))
            .filter_map(|(host, port)| {
                let port = port.parse::<u16>().ok()?;
                Some(BrokerAddress {
                    host: host.to_string(),
                    port,
                })
            })
            .collect();
        if addrs.is_empty() {
            addrs = vec![BrokerAddress {
                host: DEFAULT_HOST.to_string(),
                port: DEFAULT_PORT,
            }];
        }
        Self {
            inner: Arc::new(Inner {
                addrs,
                metadata: Mutex::new(None),
                producers: Mutex::new(HashMap::new()),
                partitions: Mutex::new(HashMap::new()),
                connected: AtomicBool::new(false),
            }),
        }
    }

    /// Constructs from the bootstrap factory's settings wire shape:
    /// `addrs` (optional; an empty list falls back to the
    /// default localhost bootstrap).
    pub fn from_settings(settings: serde_json::Value) -> Result<Self, BrokerError> {
        let settings: KafkaSettings = serde_json::from_value(settings)
            .map_err(|e| BrokerError::Failed(format!("settings parse: {e}")))?;
        Ok(Self::new(settings.addrs))
    }

    /// Returns the topic's partition state, fetching cluster
    /// metadata for the topic on first use. The frozen list means
    /// a mid-run partition expansion goes unnoticed — the
    /// divergence noted on the crate docs.
    async fn partition_state(&self, topic: &str) -> Result<Arc<PartitionState>, BrokerError> {
        if let Some(state) = self.inner.partitions.lock().await.get(topic) {
            return Ok(Arc::clone(state));
        }
        let mut guard = self.inner.metadata.lock().await;
        let Some(metadata) = guard.as_mut() else {
            return Err(BrokerError::NotConnected);
        };
        metadata.topic_names.push(topic.to_string());
        let conn = TcpConnection::new(self.inner.addrs.clone())
            .await
            .map_err(kafka_err)?;
        metadata.fetch(conn).await.map_err(kafka_err)?;
        let partitions = metadata
            .topics
            .iter()
            .find(|candidate| candidate.name == topic)
            .map(|topic| {
                topic
                    .partitions
                    .iter()
                    .map(|partition| partition.partition_index)
                    .collect::<Vec<i32>>()
            })
            .filter(|partitions| !partitions.is_empty())
            .ok_or_else(|| {
                BrokerError::Failed(format!("kafka topic has no partitions: {topic}"))
            })?;
        let counters = partitions
            .iter()
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<AtomicU64>>();
        let state = Arc::new(PartitionState {
            partitions,
            bytes: counters,
        });
        self.inner
            .partitions
            .lock()
            .await
            .insert(topic.to_string(), Arc::clone(&state));
        Ok(state)
    }
}

impl Broker for KafkaBroker {
    fn name(&self) -> &'static str {
        "kafka"
    }

    fn connect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            if self.inner.connected.load(Ordering::SeqCst) {
                return Ok(());
            }
            // Fetch the cluster's broker list: the engine-side
            // metadata view used for partition enumeration. The
            // samsa constructor dials every broker, so a reachable
            // cluster is proven here.
            let metadata: ClusterMetadata<TcpConnection> = ClusterMetadata::new(
                self.inner.addrs.clone(),
                CORRELATION_ID,
                CLIENT_ID.to_string(),
                vec![],
            )
            .await
            .map_err(kafka_err)?;
            *self.inner.metadata.lock().await = Some(metadata);
            self.inner.connected.store(true, Ordering::SeqCst);
            Ok(())
        })
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            self.inner.connected.store(false, Ordering::SeqCst);
            *self.inner.metadata.lock().await = None;
            // Dropping the senders closes the producer channels;
            // the producer and drainer tasks wind down.
            self.inner.producers.lock().await.clear();
            Ok(())
        })
    }

    fn publish<'a>(
        &'a self,
        topic: &'a str,
        message: Message,
    ) -> BoxFuture<'a, Result<(), BrokerError>> {
        Box::pin(async move {
            if !self.inner.connected.load(Ordering::SeqCst) {
                return Err(BrokerError::NotConnected);
            }
            let state = self.partition_state(topic).await?;
            // One producer per topic, built on first use — the
            // one-topic-one-writer map.
            let sender = {
                let mut producers = self.inner.producers.lock().await;
                if !producers.contains_key(topic) {
                    let producer = ProducerBuilder::<TcpConnection>::new(
                        self.inner.addrs.clone(),
                        vec![topic.to_string()],
                    )
                    .await
                    .map_err(kafka_err)?
                    .build()
                    .await;
                    let Producer {
                        sender,
                        mut receiver,
                    } = producer;
                    // The producer task parks every flush response
                    // on an unbounded channel nobody reads; drain
                    // and discard; nothing awaits the completion
                    // responses.
                    tokio::spawn(async move { while receiver.recv().await.is_some() {} });
                    producers.insert(topic.to_string(), sender);
                }
                producers
                    .get(topic)
                    .expect("producer just inserted")
                    .clone()
            };
            // Least-loaded partition choice:
            // least-loaded-by-bytes.
            let choice = state.least_loaded();
            state.bytes[choice].fetch_add(message.payload.len() as u64, Ordering::Relaxed);
            let record = ProduceMessage {
                topic: topic.to_string(),
                partition_id: state.partitions[choice],
                key: if message.key.is_empty() {
                    None
                } else {
                    Some(samsa::prelude::bytes::Bytes::from(message.key))
                },
                value: Some(samsa::prelude::bytes::Bytes::from(message.payload)),
                headers: message
                    .headers
                    .into_iter()
                    .map(|(name, value)| {
                        Header::new(name, samsa::prelude::bytes::Bytes::from(value))
                    })
                    .collect(),
            };
            sender
                .send(record)
                .await
                .map_err(|_| BrokerError::NotConnected)?;
            Ok(())
        })
    }

    fn subscribe<'a>(
        &'a self,
        topic: &'a str,
        handler: Handler,
    ) -> BoxFuture<'a, Result<Box<dyn Subscriber>, BrokerError>> {
        Box::pin(async move {
            if !self.inner.connected.load(Ordering::SeqCst) {
                return Err(BrokerError::NotConnected);
            }
            let state = self.partition_state(topic).await?;
            // A fresh consumer group per subscription, with a
            // random group id. The first join of a fresh group
            // races the broker's lazy coordinator election — every
            // attempt until the election resolves answers
            // GroupCoordinatorNotAvailable — so the builder retries
            // with backoff, as every long-lived kafka client does.
            let group_id = random_group_id();
            let mut group = None;
            let mut join_backoff_secs = 1u64;
            for _ in 0..6 {
                let attempt = async {
                    let builder = ConsumerGroupBuilder::<TcpConnection>::new(
                        self.inner.addrs.clone(),
                        group_id.clone(),
                        HashMap::from([(topic.to_string(), state.partitions.clone())]),
                    )
                    .await
                    .map_err(kafka_err)?;
                    builder.build().await.map_err(kafka_err)
                }
                .await;
                if let Ok(built) = attempt {
                    group = Some(built);
                    break;
                }
                if join_backoff_secs < 30 {
                    tokio::time::sleep(Duration::from_secs(join_backoff_secs)).await;
                    join_backoff_secs *= 2;
                }
            }
            let Some(group) = group else {
                return Err(BrokerError::Failed(
                    "kafka: the group join kept failing".to_string(),
                ));
            };
            let done = CancellationToken::new();
            let task_done = done.clone();
            let handler = Arc::clone(&handler);
            let task = tokio::spawn(async move {
                let mut backoff_secs = 1u64;
                loop {
                    // The group stream: fetch batches with per-batch
                    // auto-committed offsets, ending on coordinator
                    // errors and rebalances. Each fresh stream is a
                    // rejoin; the original group object stays with
                    // the task for the next one.
                    let stream = group.clone().into_stream();
                    tokio::pin!(stream);
                    let mut broken = false;
                    while !broken {
                        let batch = tokio::select! {
                            _ = task_done.cancelled() => return,
                            batch = stream.next() => batch,
                        };
                        match batch {
                            Some(Ok(records)) => {
                                for record in records {
                                    let event = to_event(record);
                                    tokio::spawn(handler(event));
                                }
                            }
                            Some(Err(_)) | None => broken = true,
                        }
                    }
                    tokio::select! {
                        _ = task_done.cancelled() => return,
                        _ = tokio::time::sleep(Duration::from_secs(backoff_secs)) => {}
                    }
                    backoff_secs = (backoff_secs * 2).min(30);
                }
            });
            Ok(Box::new(KafkaSubscriber {
                topic: topic.to_string(),
                done,
                task: Mutex::new(Some(task)),
            }) as Box<dyn Subscriber>)
        })
    }
}

/// The Kafka subscription handle: the token and task tear the
/// delivery pump down on unsubscribe or drop.
struct KafkaSubscriber {
    topic: String,
    done: CancellationToken,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Subscriber for KafkaSubscriber {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn unsubscribe(&mut self) -> BoxFuture<'_, Result<(), BrokerError>> {
        Box::pin(async move {
            self.done.cancel();
            if let Some(task) = self.task.lock().await.take() {
                let _ = task.await;
            }
            Ok(())
        })
    }
}

impl Drop for KafkaSubscriber {
    fn drop(&mut self) {
        // Best-effort: cancel and abort without awaiting the task.
        self.done.cancel();
        if let Ok(mut task) = self.task.try_lock() {
            if let Some(task) = task.take() {
                task.abort();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fetched record maps onto the contract with the key doubling
    /// as the message id (kafka records carry no native id), the
    /// lossy key decode, and the partition/offset surfacing.
    #[test]
    fn fetched_record_maps_onto_the_contract() {
        let record = samsa::prelude::ConsumeMessage {
            key: samsa::prelude::bytes::Bytes::from_static(b"order-7"),
            value: samsa::prelude::bytes::Bytes::from_static(b"body"),
            offset: 41,
            timestamp: 0,
            topic_name: "probe".to_string(),
            partition_index: 2,
        };
        let event = to_event(record);
        assert_eq!(event.topic(), "probe");
        let message = event.message();
        assert_eq!(message.payload, b"body".to_vec());
        assert_eq!(message.key, "order-7");
        assert_eq!(message.id, "order-7", "the key doubles as the id");
        assert_eq!(message.partition, Some(2));
        assert_eq!(message.offset, Some(41));
    }

    /// A key that is not valid UTF-8 decodes lossily instead of
    /// failing the delivery.
    #[test]
    fn invalid_utf8_key_decodes_lossily() {
        let record = samsa::prelude::ConsumeMessage {
            key: samsa::prelude::bytes::Bytes::from_static(&[0xff, b'k']),
            value: samsa::prelude::bytes::Bytes::from_static(b"body"),
            offset: 0,
            timestamp: 0,
            topic_name: "probe".to_string(),
            partition_index: 0,
        };
        let event = to_event(record);
        assert_eq!(event.message().key, "\u{fffd}k");
        assert_eq!(event.message().id, "\u{fffd}k");
    }
}
