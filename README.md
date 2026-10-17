# Amazon SQS Watch Strategy — `dev.mcpg.watch.sqs`

> class `watch_strategy` · `native` · package `mcpg-plugin-watch-sqs` · artifact `libmcpg_plugin_watch_sqs.so` · Apache-2.0

Consumes an Amazon SQS queue, or an SQS-compatible one such as ElasticMQ or
LocalStack, as MCP Events. The `sqs_queue` strategy long-polls the queue,
and each message is one `sqs.message` event:

- A message is **deleted** once the gateway has stored its event, and only
  then.
- A message the gateway has not acknowledged in `ack_timeout_ms` is **made
  visible again**, so it comes back: delivery is at-least-once. The SQS
  message id is the event id, so the gateway stores a redelivered message
  once.
- A received message stays invisible for `ack_timeout_ms` plus 30 seconds,
  so SQS does not hand it out again while the gateway still has it.
- When the watch stops, the messages it holds are made visible again at
  once.

Standard and FIFO queues both work; on a FIFO queue a message the gateway
has not taken holds back its message group until it is released.

## Configuration

```yaml
plugins:
  - id: dev.mcpg.watch.sqs
    class: watch_strategy
    kind: native
    source: { oci: "<registry>/plugins/watch-sqs:protocol-1" }

mcp:
  configurations:
    events:
      enabled: true
      store: { kind: file, dir: /var/lib/mcpg/events/subscriptions }
      log_store: { kind: file, dir: /var/lib/mcpg/events/log }
  capabilities:
    events:
      - name: jobs.queued
        description: A job message on the jobs queue.
        source:
          watch:
            strategy:
              type: plugin
              kind: sqs_queue
              queue_url: https://sqs.eu-west-1.amazonaws.com/123456789012/jobs
            event_type: sqs.message
```

| Field | Type | Default | Notes |
|---|---|---|---|
| `queue_url` | string (required) | — | The queue URL. |
| `region` | string | from `queue_url` | Required when the URL does not name an AWS region (an emulator). |
| `endpoint_url` | string | — | Another SQS-compatible endpoint. |
| `credentials` | `{access_key_id, secret_access_key, session_token?}` | default chain | Static credentials, resolved before the watch starts (`${env.X}`, `${secret.X}`, `vault://…`); `cred://` is rejected. Without them the AWS default chain applies: IRSA, instance role, environment, profile. |
| `wait_time_secs` | int | `10` | Long-poll wait of one receive (1 to 20). |
| `max_in_flight` | int | `256` | Most messages received and not yet deleted (1 to 1000). |
| `ack_timeout_ms` | int | `60000` | How long a message waits for the gateway before it is made visible again (at most 43 170 000). |
| `max_body_bytes` | int | `131072` | A larger body is left out of the event, with `bodyTruncated: true` and `bodySize` (at most 196608). |

The watch needs `sqs:ReceiveMessage`, `sqs:DeleteMessage`,
`sqs:ChangeMessageVisibility` and `sqs:GetQueueAttributes` on the queue.

## Events

Each `sqs.message` event's `data` is:

| Field | Meaning |
|---|---|
| `queueUrl`, `messageId` | Where it came from, and its SQS id (also the event id). |
| `body`, `bodyEncoding` | The body: its JSON when it parses, else its text. |
| `bodyTruncated`, `bodySize` | Set when the body was over `max_body_bytes`. |
| `sentAt` | When SQS received it, RFC 3339; also the event time. |
| `receiveCount` | How many times SQS has delivered it, about. |
| `groupId`, `deduplicationId`, `sequenceNumber`, `senderId` | The system attributes SQS reports. |
| `messageAttributes` | Each message attribute as `{dataType, value}`, or `{dataType, binaryValue}` in standard base64. |

A message an SNS topic delivers carries the SNS envelope as its body, unless
the subscription delivers raw messages.

## Build / test

```bash
cargo build  -p mcpg-plugin-watch-sqs
cargo test   -p mcpg-plugin-watch-sqs                              # unit tests
cargo test   -p mcpg-plugin-watch-sqs --features integration-tests # ElasticMQ (docker), live AWS if set
cargo clippy -p mcpg-plugin-watch-sqs --all-targets -- -D warnings
```

The integration suite runs the watch through a window of two, an
acknowledgement, a redelivery after the timeout, and a release on cancel.

`tests/default_chain.rs` runs offline in the plain test run. It gives the
watch no `credentials`, so the AWS default chain applies, and a loopback mock
serves both the container credentials (`AWS_CONTAINER_CREDENTIALS_FULL_URI`)
and SQS. The test checks that every SQS call is SigV4-signed with those
credentials and their session token.

`--features integration-tests` also builds `tests/sqs_live.rs`, which runs
against real AWS when `SQS_LIVE_QUEUE_URL` or `SQS_LIVE_FIFO_QUEUE_URL` is
set in your shell and skips when they are not. See
[Running against AWS](#running-against-aws).

## Running against AWS

`tests/sqs_live.rs` runs the same flow against real SQS queues, with the
AWS default credential chain and no `endpoint_url`. It uses two identities:

- **The watch**: the ambient chain, here the profile in `AWS_PROFILE`. It
  assumes a role that has only the four SQS actions the watch needs.
- **The driver**: the profile in `AWS_LIVE_DRIVER_PROFILE`, an
  administrator. It drains the queues, sends the messages and reads the
  queue counts.

Each test skips unless its variable is set: `SQS_LIVE_QUEUE_URL` for a
standard queue, `SQS_LIVE_FIFO_QUEUE_URL` for a FIFO queue. Each queue is for
this suite only. Each message carries the id of its run, and on the FIFO
queue the run id is also the message group. Before a test starts the watch,
it deletes what is on the queue and waits until SQS counts no message in
flight, so a message that a stopped run left behind cannot reach it.

### Set up

Run this once with the AWS CLI. It makes the two queues, the watch role
with its least-privilege policy, and a profile that assumes the role.

```bash
#!/usr/bin/env bash
set -euo pipefail
export AWS_PROFILE=${AWS_LIVE_DRIVER_PROFILE:?set it to an administrator profile}
REGION=${AWS_REGION:-eu-west-1}
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)

QUEUE=$(aws sqs create-queue --region "$REGION" --queue-name mcpg-live-sqs \
  --query QueueUrl --output text)
FIFO=$(aws sqs create-queue --region "$REGION" --queue-name mcpg-live-sqs.fifo \
  --attributes FifoQueue=true --query QueueUrl --output text)

trust=$(cat <<EOF
{ "Version": "2012-10-17",
  "Statement": [{ "Effect": "Allow",
    "Principal": { "AWS": "arn:aws:iam::$ACCOUNT:root" },
    "Action": "sts:AssumeRole" }] }
EOF
)
policy=$(cat <<EOF
{ "Version": "2012-10-17",
  "Statement": [{ "Effect": "Allow",
    "Action": [ "sqs:GetQueueAttributes", "sqs:ReceiveMessage",
                "sqs:DeleteMessage", "sqs:ChangeMessageVisibility" ],
    "Resource": [ "arn:aws:sqs:$REGION:$ACCOUNT:mcpg-live-sqs",
                  "arn:aws:sqs:$REGION:$ACCOUNT:mcpg-live-sqs.fifo" ] }] }
EOF
)
aws iam create-role --role-name mcpg-live-sqs-watch \
  --assume-role-policy-document "$trust" > /dev/null
aws iam put-role-policy --role-name mcpg-live-sqs-watch \
  --policy-name sqs-watch --policy-document "$policy"

aws configure set role_arn "arn:aws:iam::$ACCOUNT:role/mcpg-live-sqs-watch" \
  --profile mcpg-live-sqs-watch
aws configure set source_profile "$AWS_PROFILE" --profile mcpg-live-sqs-watch
aws configure set region "$REGION" --profile mcpg-live-sqs-watch

echo "export SQS_LIVE_QUEUE_URL=$QUEUE"
echo "export SQS_LIVE_FIFO_QUEUE_URL=$FIFO"
```

IAM can take some seconds before the new role can be assumed.

### Run

```bash
export AWS_PROFILE=mcpg-live-sqs-watch
export AWS_LIVE_DRIVER_PROFILE=<your administrator profile>
export SQS_LIVE_QUEUE_URL=... SQS_LIVE_FIFO_QUEUE_URL=...   # from the setup
cargo test -p mcpg-plugin-watch-sqs --features integration-tests --test sqs_live -- --nocapture
```

The suite reads these variables from the environment and skips when one it
needs is not set. The SDK finds the profiles in `~/.aws/config` and the SSO
cache through `HOME`.

### Clean up

Use the region of the setup.

```bash
export AWS_PROFILE=$AWS_LIVE_DRIVER_PROFILE
REGION=${AWS_REGION:-eu-west-1}
aws sqs delete-queue --queue-url "$SQS_LIVE_QUEUE_URL" --region "$REGION"
aws sqs delete-queue --queue-url "$SQS_LIVE_FIFO_QUEUE_URL" --region "$REGION"
aws iam delete-role-policy --role-name mcpg-live-sqs-watch --policy-name sqs-watch
aws iam delete-role --role-name mcpg-live-sqs-watch
```

Then remove the `[profile mcpg-live-sqs-watch]` section from
`~/.aws/config`.
