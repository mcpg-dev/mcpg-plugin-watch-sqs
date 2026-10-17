//! Amazon SQS `watch_strategy` plugin (`dev.mcpg.watch.sqs`).
//!
//! The `sqs_queue` strategy long-polls a queue through the SDK's queue
//! helper. Each message is one `sqs.message` event; it is deleted once the
//! gateway has stored the event, and made visible again when the gateway
//! has not done so in `ack_timeout_ms`, so delivery is at-least-once and
//! the SQS message id, kept across redeliveries, lets the gateway take it
//! once. A received message stays invisible for longer than
//! `ack_timeout_ms`, so it does not come back while the gateway still has
//! it in hand.

use std::time::Duration;

use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_sqs::Client;
use aws_sdk_sqs::error::{DisplayErrorContext, ProvideErrorMetadata};
use aws_sdk_sqs::types::{Message, MessageSystemAttributeName, QueueAttributeName};
use base64::Engine as _;
use mcpg_plugin_protocol::backend::{WatchError, WatchEvent};
use mcpg_plugin_protocol::{PluginManifest, firstparty_manifest};
use mcpg_plugin_sdk::HostHandle;
use mcpg_plugin_sdk::ffi::{SyncWatchStrategyPlugin, WatchHandleBox};
use mcpg_plugin_sdk::queue::{
    Delivery, QueueOptions, QueueSource, acknowledge_queue_watch, cancel_queue_watch, defaults,
    spawn_queue_watch,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};

pub const PLUGIN_ID: &str = "dev.mcpg.watch.sqs";

/// The strategy discriminator this entity handles.
pub const WATCH_KIND: &str = "sqs_queue";

/// The event type each message makes.
pub const MESSAGE_EVENT_TYPE: &str = "sqs.message";

/// Largest `max_body_bytes`: the event must stay under the host's 256 KiB.
const MAX_BODY_BYTES: usize = 192 * 1024;

/// Most messages one `ReceiveMessage` returns.
const MAX_BATCH: usize = 10;

/// How much longer than `ack_timeout_ms` a received message stays
/// invisible, so the watch releases it before SQS would.
const VISIBILITY_MARGIN_SECS: u64 = 30;

/// Longest visibility timeout SQS allows: 12 hours.
const MAX_VISIBILITY_SECS: u64 = 43_200;

fn default_wait_time_secs() -> u64 {
    10
}

/// Static AWS credentials; without them the default chain applies (IRSA,
/// instance role, environment, profile).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StaticCredentials {
    access_key_id: String,
    secret_access_key: String,
    #[serde(default)]
    session_token: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WatchSpec {
    /// The queue URL, `https://sqs.{region}.amazonaws.com/{account}/{name}`.
    queue_url: String,
    /// AWS region; taken from an AWS queue URL when omitted.
    #[serde(default)]
    region: Option<String>,
    /// Another SQS-compatible endpoint (ElasticMQ, LocalStack).
    #[serde(default)]
    endpoint_url: Option<String>,
    #[serde(default)]
    credentials: Option<StaticCredentials>,
    /// Long-poll wait of one receive, seconds (1 to 20).
    #[serde(default = "default_wait_time_secs")]
    wait_time_secs: u64,
    /// Most messages received and not yet settled.
    #[serde(default = "defaults::max_in_flight")]
    max_in_flight: usize,
    /// How long a message waits for the gateway before it is made visible
    /// again.
    #[serde(default = "defaults::ack_timeout_ms")]
    ack_timeout_ms: u64,
    /// A body past this is left out of the event, with its size.
    #[serde(default = "defaults::max_body_bytes")]
    max_body_bytes: usize,
}

/// The region of an AWS queue URL: `sqs.{region}.amazonaws.com` or the
/// legacy `{region}.queue.amazonaws.com`.
fn region_of(queue_url: &str) -> Option<String> {
    let host = queue_url
        .strip_prefix("https://")
        .or_else(|| queue_url.strip_prefix("http://"))?
        .split('/')
        .next()?;
    let mut parts = host.split('.');
    match (parts.next()?, parts.next()?) {
        ("sqs", region) => Some(region.to_owned()),
        (region, "queue") => Some(region.to_owned()),
        _ => None,
    }
}

/// An SDK error with its code and message, without the request.
fn sdk_error<E: ProvideErrorMetadata + std::error::Error + 'static, R: std::fmt::Debug>(
    action: &str,
    error: aws_sdk_sqs::error::SdkError<E, R>,
) -> String {
    match (error.code(), error.message()) {
        (Some(code), Some(message)) => format!("SQS {action} failed: {code}: {message}"),
        (Some(code), None) => format!("SQS {action} failed: {code}"),
        _ => format!("SQS {action} failed: {}", DisplayErrorContext(&error)),
    }
}

/// The broker side of one `sqs_queue` watch.
struct SqsQueue {
    client: Client,
    queue_url: String,
    wait_time_secs: i32,
    visibility_secs: i32,
    max_body_bytes: usize,
    rt: tokio::runtime::Runtime,
}

impl SqsQueue {
    fn receipt(handle: &Value) -> Option<String> {
        handle.as_str().map(str::to_owned)
    }
}

impl QueueSource for SqsQueue {
    /// SQS keeps no connection; this checks the queue is there and readable,
    /// so a wrong URL or a missing permission is reported at once.
    fn connect(&self) -> Result<(), String> {
        self.rt
            .block_on(
                self.client
                    .get_queue_attributes()
                    .queue_url(&self.queue_url)
                    .attribute_names(QueueAttributeName::QueueArn)
                    .send(),
            )
            .map(|_| ())
            .map_err(|e| sdk_error("GetQueueAttributes", e))
    }

    fn receive(&self, max: usize, _wait: Duration) -> Result<Vec<Delivery>, String> {
        let batch = i32::try_from(max.min(MAX_BATCH)).unwrap_or(1);
        let received = self
            .rt
            .block_on(
                self.client
                    .receive_message()
                    .queue_url(&self.queue_url)
                    .max_number_of_messages(batch)
                    .wait_time_seconds(self.wait_time_secs)
                    .visibility_timeout(self.visibility_secs)
                    .message_system_attribute_names(MessageSystemAttributeName::All)
                    .message_attribute_names("All")
                    .send(),
            )
            .map_err(|e| sdk_error("ReceiveMessage", e))?;
        Ok(received
            .messages()
            .iter()
            .filter_map(|message| {
                let receipt = message.receipt_handle()?.to_owned();
                Some(Delivery {
                    event: message_event(&self.queue_url, message, self.max_body_bytes),
                    receipt: Value::from(receipt),
                })
            })
            .collect())
    }

    fn ack(&self, receipt: &Value) -> Result<(), String> {
        let Some(receipt) = Self::receipt(receipt) else {
            return Ok(());
        };
        self.rt
            .block_on(
                self.client
                    .delete_message()
                    .queue_url(&self.queue_url)
                    .receipt_handle(receipt)
                    .send(),
            )
            .map(|_| ())
            .map_err(|e| sdk_error("DeleteMessage", e))
    }

    fn release(&self, receipt: &Value) -> Result<(), String> {
        let Some(receipt) = Self::receipt(receipt) else {
            return Ok(());
        };
        self.rt
            .block_on(
                self.client
                    .change_message_visibility()
                    .queue_url(&self.queue_url)
                    .receipt_handle(receipt)
                    .visibility_timeout(0)
                    .send(),
            )
            .map(|_| ())
            .map_err(|e| sdk_error("ChangeMessageVisibility", e))
    }
}

/// The `sqs.message` event of one received message.
fn message_event(queue_url: &str, message: &Message, max_body_bytes: usize) -> WatchEvent {
    let mut data = Map::new();
    data.insert("queueUrl".into(), json!(queue_url));
    if let Some(id) = message.message_id() {
        data.insert("messageId".into(), json!(id));
    }
    let body = message.body().unwrap_or_default();
    if body.len() > max_body_bytes {
        data.insert("body".into(), Value::Null);
        data.insert("bodyEncoding".into(), Value::Null);
        data.insert("bodyTruncated".into(), json!(true));
        data.insert("bodySize".into(), json!(body.len()));
    } else {
        let (value, encoding) = mcpg_plugin_protocol::event_body(body.as_bytes());
        data.insert("body".into(), value);
        data.insert("bodyEncoding".into(), json!(encoding));
    }
    let system = message.attributes();
    let attribute = |name: MessageSystemAttributeName| system.and_then(|a| a.get(&name));
    let sent_at = attribute(MessageSystemAttributeName::SentTimestamp)
        .and_then(|ms| ms.parse::<i64>().ok())
        .and_then(mcpg_plugin_protocol::occurred_at_from_millis);
    if let Some(at) = &sent_at {
        data.insert("sentAt".into(), json!(at));
    }
    if let Some(count) = attribute(MessageSystemAttributeName::ApproximateReceiveCount)
        .and_then(|n| n.parse::<u64>().ok())
    {
        data.insert("receiveCount".into(), json!(count));
    }
    let named = [
        ("groupId", MessageSystemAttributeName::MessageGroupId),
        (
            "deduplicationId",
            MessageSystemAttributeName::MessageDeduplicationId,
        ),
        ("sequenceNumber", MessageSystemAttributeName::SequenceNumber),
        ("senderId", MessageSystemAttributeName::SenderId),
    ];
    for (field, name) in named {
        if let Some(value) = attribute(name) {
            data.insert(field.into(), json!(value));
        }
    }
    let attributes: Map<String, Value> = message
        .message_attributes()
        .map(|attributes| {
            attributes
                .iter()
                .map(|(name, value)| {
                    let mut attribute = Map::new();
                    attribute.insert("dataType".into(), json!(value.data_type()));
                    if let Some(text) = value.string_value() {
                        attribute.insert("value".into(), json!(text));
                    }
                    if let Some(bytes) = value.binary_value() {
                        attribute.insert(
                            "binaryValue".into(),
                            json!(base64::engine::general_purpose::STANDARD.encode(bytes.as_ref())),
                        );
                    }
                    (name.clone(), Value::Object(attribute))
                })
                .collect()
        })
        .unwrap_or_default();
    data.insert("messageAttributes".into(), Value::Object(attributes));
    WatchEvent {
        event_id: message.message_id().map(str::to_owned),
        event_type: Some(MESSAGE_EVENT_TYPE.to_owned()),
        occurred_at: sent_at,
        data: Some(Value::Object(data)),
        ..WatchEvent::default()
    }
}

/// `watch_strategy` entity. Stateless beyond its manifest — each watcher's
/// queue and credentials arrive on the per-watch spec.
pub struct SqsWatchPlugin {
    manifest: PluginManifest,
}

impl SqsWatchPlugin {
    /// Infallible cdylib factory; the watch carries no plugin-level config.
    pub fn from_host_config(_config_json: &str, _host: HostHandle) -> Self {
        Self {
            manifest: firstparty_manifest! {
                id: "dev.mcpg.watch.sqs",
                name: "Amazon SQS Watch Strategy",
                class: WatchStrategy,
            },
        }
    }
}

impl SyncWatchStrategyPlugin for SqsWatchPlugin {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn kind(&self) -> &str {
        WATCH_KIND
    }

    fn watch(
        &self,
        resource_uri: &str,
        spec: &Value,
        emit_event: Box<dyn Fn(&str) + Send + Sync + 'static>,
    ) -> Result<WatchHandleBox, WatchError> {
        let (spec, _) = mcpg_plugin_protocol::take_watch_resume(spec);
        let parsed: WatchSpec =
            serde_json::from_value(spec).map_err(|e| WatchError::InvalidSpec {
                message: format!("invalid sqs_queue watch spec: {e}"),
            })?;
        let invalid = |m: String| WatchError::InvalidSpec { message: m };
        if !parsed.queue_url.starts_with("https://") && !parsed.queue_url.starts_with("http://") {
            return Err(invalid("queue_url must be an http(s) URL".into()));
        }
        let region = parsed
            .region
            .clone()
            .or_else(|| region_of(&parsed.queue_url))
            .ok_or_else(|| invalid("region is required: the queue_url does not name one".into()))?;
        if let Some(credentials) = &parsed.credentials {
            let secrets = [
                Some(&credentials.access_key_id),
                Some(&credentials.secret_access_key),
                credentials.session_token.as_ref(),
            ];
            if secrets
                .into_iter()
                .flatten()
                .any(|s| s.starts_with("cred://"))
            {
                return Err(invalid(
                    "credentials must not be cred:// URIs — per-caller credentials are \
                     unsupported; use ${env.X} / ${secret.X} / vault:// (resolved before the \
                     watch starts)"
                        .into(),
                ));
            }
            if credentials.access_key_id.is_empty() || credentials.secret_access_key.is_empty() {
                return Err(invalid(
                    "credentials need access_key_id and secret_access_key".into(),
                ));
            }
        }
        if !(1..=20).contains(&parsed.wait_time_secs) {
            return Err(invalid("wait_time_secs must be 1 to 20".into()));
        }
        if !(1..=MAX_BODY_BYTES).contains(&parsed.max_body_bytes) {
            return Err(invalid(format!(
                "max_body_bytes must be 1 to {MAX_BODY_BYTES}"
            )));
        }
        let visibility_secs = parsed.ack_timeout_ms.div_ceil(1_000) + VISIBILITY_MARGIN_SECS;
        if visibility_secs > MAX_VISIBILITY_SECS {
            return Err(invalid(format!(
                "ack_timeout_ms must be at most {} (SQS keeps a message invisible for 12 hours at most)",
                (MAX_VISIBILITY_SECS - VISIBILITY_MARGIN_SECS) * 1_000
            )));
        }
        let options = QueueOptions {
            source: format!("sqs queue {}", parsed.queue_url),
            max_in_flight: parsed.max_in_flight,
            ack_timeout: Duration::from_millis(parsed.ack_timeout_ms),
            receive_wait: Duration::from_secs(parsed.wait_time_secs),
        };
        options.validate().map_err(invalid)?;

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("mcpg-sqs-watch")
            .enable_all()
            .build()
            .map_err(|e| WatchError::Subscribe {
                message: format!("sqs_queue: tokio runtime init failed: {e}"),
            })?;
        let mut config = aws_sdk_sqs::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(region.clone()));
        if let Some(endpoint) = &parsed.endpoint_url {
            config = config.endpoint_url(endpoint.clone());
        }
        if let Some(credentials) = parsed.credentials {
            config = config.credentials_provider(Credentials::new(
                credentials.access_key_id,
                credentials.secret_access_key,
                credentials.session_token,
                None,
                "mcpg-sqs-static",
            ));
        } else {
            let defaults = rt.block_on(
                aws_config::defaults(BehaviorVersion::latest())
                    .region(Region::new(region))
                    .load(),
            );
            if let Some(provider) = defaults.credentials_provider() {
                config = config.credentials_provider(provider);
            }
        }
        let source = SqsQueue {
            client: Client::from_conf(config.build()),
            queue_url: parsed.queue_url,
            wait_time_secs: i32::try_from(parsed.wait_time_secs).unwrap_or(10),
            visibility_secs: i32::try_from(visibility_secs).unwrap_or(i32::MAX),
            max_body_bytes: parsed.max_body_bytes,
            rt,
        };
        spawn_queue_watch(resource_uri, options, emit_event, source)
    }

    fn acknowledge(&self, watch_handle: &WatchHandleBox, position: &Value) {
        acknowledge_queue_watch(watch_handle, position);
    }

    fn cancel(&self, watch_handle: WatchHandleBox) {
        cancel_queue_watch(watch_handle);
    }
}

mcpg_plugin_sdk::declare_plugin! {
    plugin_id: "dev.mcpg.watch.sqs",
    plugin_version: env!("CARGO_PKG_VERSION"),
    descriptor_yaml: include_str!("../plugin.yaml"),
    capabilities: &[::mcpg_plugin_protocol::capability::Capability::NetworkOutbound],
    entities: [
        watch_strategy as watch {
            inner_name: "",
            plugin_type: SqsWatchPlugin,
            factory: |cfg: &str, host: ::mcpg_plugin_sdk::HostHandle|
                SqsWatchPlugin::from_host_config(cfg, host),
        },
    ],
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_sqs::types::MessageAttributeValue;

    fn stub_host() -> HostHandle {
        // SAFETY: `stub_host_ref` returns a process-static no-op host ref; the
        // factory ignores the host entirely.
        #[allow(unsafe_code)]
        unsafe {
            HostHandle::from_ffi(mcpg_plugin_sdk::testing::stub_host_ref())
        }
    }

    fn plugin() -> SqsWatchPlugin {
        SqsWatchPlugin::from_host_config("", stub_host())
    }

    fn refused(spec: Value) -> String {
        match plugin().watch("mcpg-event://jobs", &spec, Box::new(|_| {})) {
            Err(WatchError::InvalidSpec { message }) => message,
            Err(other) => panic!("not a spec error: {other:?}"),
            Ok(handle) => {
                plugin().cancel(handle);
                panic!("accepted {spec}");
            }
        }
    }

    const URL: &str = "https://sqs.eu-west-1.amazonaws.com/123456789012/jobs";

    #[test]
    fn manifest_and_kind_are_correct() {
        use mcpg_plugin_protocol::PluginClass;
        let p = plugin();
        assert_eq!(SyncWatchStrategyPlugin::manifest(&p).id, PLUGIN_ID);
        assert_eq!(
            SyncWatchStrategyPlugin::manifest(&p).plugin_class,
            PluginClass::WatchStrategy
        );
        assert_eq!(p.kind(), WATCH_KIND);
    }

    #[test]
    fn the_region_comes_from_an_aws_queue_url() {
        assert_eq!(region_of(URL).as_deref(), Some("eu-west-1"));
        assert_eq!(
            region_of("https://us-east-2.queue.amazonaws.com/1/q").as_deref(),
            Some("us-east-2")
        );
        assert_eq!(region_of("http://localhost:9324/000000000000/q"), None);
    }

    #[test]
    fn spec_parses_with_defaults() {
        let parsed: WatchSpec = serde_json::from_value(json!({ "queue_url": URL })).unwrap();
        assert_eq!(parsed.wait_time_secs, 10);
        assert_eq!(parsed.max_in_flight, 256);
        assert_eq!(parsed.ack_timeout_ms, 60_000);
        assert_eq!(parsed.max_body_bytes, 128 * 1024);
        assert!(parsed.credentials.is_none());
    }

    #[test]
    fn a_bad_spec_is_refused_before_anything_is_called() {
        let with = |extra: Value| {
            let mut spec = json!({ "queue_url": URL });
            for (k, v) in extra.as_object().unwrap() {
                spec[k] = v.clone();
            }
            spec
        };
        assert!(refused(with(json!({ "bogus": 1 }))).contains("unknown field"));
        assert!(refused(with(json!({ "queue_url": "sqs://jobs" }))).contains("queue_url"));
        assert!(
            refused(with(json!({ "queue_url": "http://localhost:9324/0/q" }))).contains("region")
        );
        assert!(
            refused(with(json!({ "credentials": {
                "access_key_id": "cred://vault/aws", "secret_access_key": "x" } })))
            .contains("cred://")
        );
        assert!(refused(with(json!({ "wait_time_secs": 0 }))).contains("wait_time_secs"));
        assert!(refused(with(json!({ "max_in_flight": 0 }))).contains("max_in_flight"));
        assert!(refused(with(json!({ "ack_timeout_ms": 0 }))).contains("ack_timeout_ms"));
        assert!(
            refused(with(json!({ "ack_timeout_ms": 50_000_000u64 }))).contains("ack_timeout_ms")
        );
        assert!(refused(with(json!({ "max_body_bytes": 300_000 }))).contains("max_body_bytes"));
    }

    #[test]
    fn a_message_becomes_an_sqs_message_event() {
        let message = Message::builder()
            .message_id("m-1")
            .receipt_handle("r-1")
            .body(r#"{"job":7}"#)
            .attributes(MessageSystemAttributeName::SentTimestamp, "1700000000123")
            .attributes(MessageSystemAttributeName::ApproximateReceiveCount, "2")
            .attributes(MessageSystemAttributeName::MessageGroupId, "g")
            .message_attributes(
                "tenant",
                MessageAttributeValue::builder()
                    .data_type("String")
                    .string_value("acme")
                    .build()
                    .unwrap(),
            )
            .build();
        let event = message_event(URL, &message, 1024);
        assert_eq!(event.event_id.as_deref(), Some("m-1"));
        assert_eq!(event.event_type.as_deref(), Some(MESSAGE_EVENT_TYPE));
        assert_eq!(
            event.occurred_at.as_deref(),
            Some("2023-11-14T22:13:20.123Z")
        );
        let data = event.data.unwrap();
        assert_eq!(data["body"], json!({ "job": 7 }));
        assert_eq!(data["bodyEncoding"], json!("json"));
        assert_eq!(data["receiveCount"], json!(2));
        assert_eq!(data["groupId"], json!("g"));
        assert_eq!(
            data["messageAttributes"]["tenant"],
            json!({ "dataType": "String", "value": "acme" })
        );

        let big = message_event(URL, &message, 4);
        let data = big.data.unwrap();
        assert_eq!(data["body"], Value::Null);
        assert_eq!(data["bodyTruncated"], json!(true));
        assert_eq!(data["bodySize"], json!(9));
    }
}
