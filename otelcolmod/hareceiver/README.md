# hareceiver

Pulls journal logs from a Home Assistant instance over its HTTP API and emits OTLP logs.

Nothing is installed on the Home Assistant host — the receiver runs wherever odbagent runs.
See [`docs/home-assistant.md`](../../docs/home-assistant.md) for the design.

> [!WARNING]
> `/api/hassio/...` is an internal, admin-only, undocumented Home Assistant Core path. It is
> an allowlist in `homeassistant/components/hassio/http.py` with no compatibility promise; a
> refactor can move or reshape it without notice. The response format this receiver parses is
> likewise a frontend rendering, not an API.

## Configuration

```yaml
extensions:
  file_storage:
    directory: /var/lib/otelcol

receivers:
  hareceiver:
    endpoint: https://homeassistant.example:8123
    token: ${env:HA_TOKEN}
    poll_interval: 10s
    storage: file_storage
    severity_from_message: true
    sources:
      - kind: host
      - kind: core
      - kind: supervisor
      - kind: addon
        addon: core_ssh
```

| Setting | Default | Description |
|---|---|---|
| `endpoint` | — | Home Assistant base URL. Required. |
| `token` | — | Long-lived access token. Required, see below. |
| `sources` | — | Log streams to ingest. At least one required. |
| `poll_interval` | `10s` | Delay between cursor-advancing requests. |
| `batch_size` | `1000` | Maximum journal entries per request. |
| `parse_message` | `true` | Extract the level, thread and logger Core and Supervisor embed in the message, see below. |
| `recombine_window` | `1s` | Join journal entries that are fragments of one multi-line message, see below. `0` disables. |
| `severity_from_message` | `false` | Best-effort severity detection for messages `parse_message` does not recognize. |
| `storage` | — | Storage extension holding cursors. Without it, every start begins at the tail. |

The remaining settings come from `confighttp.ClientConfig` (TLS, timeouts, extra headers,
compression).

`kind` is one of `host`, `core`, `supervisor`, `dns`, `audio`, `multicast`, `cli`, `observer`,
or `addon`; `addon` also requires the add-on `slug`. Everything but `addon` maps to a path
Core allowlists at `/api/hassio/<kind>/logs`. Which plugins exist depends on the installation
— `cli` and `observer` are absent on some, and return 404.

### Token

The token must be a long-lived access token **belonging to an admin user**. The Core proxy
gates every path in this receiver on `is_admin`, and a non-admin token gets 401 on all of
them. An existing token that works for `/api/prometheus` is not evidence: that endpoint does
not require admin. A 401 or 403 is treated as permanent and stops the receiver rather than
retrying.

## What Home Assistant actually returns

Two constraints shape this receiver, both verified against Core and Supervisor source:

**Journal fields are not available.** Supervisor requests
`application/vnd.fdo.journal` from `systemd-journal-gatewayd`, parses it, and renders each
entry to **text** using a formatter that keeps only `__CURSOR`, `__REALTIME_TIMESTAMP`,
`_HOSTNAME`, `SYSLOG_IDENTIFIER`, `_PID` and `MESSAGE` — every other field is dropped before
the response is written. Requesting `application/json` does not help: Core's proxy never
forwards `Accept`, and Supervisor rejects any `Accept` other than `text/plain`/`text/x-log`
with a 400.

Consequently there is **no `PRIORITY`**, and therefore no faithful syslog severity mapping.
Severity is recovered from the message instead, in two ways.

### Application log structure

Core and Supervisor write a fixed format into `MESSAGE`, from their Python logging formatter:

```
2026-08-09 15:26:04.893 INFO (SyncWorker_2) [supervisor.backups.backup] Backing up folder ssl
└──── application ts ───┘ └level┘ └─ thread ─┘ └────── logger ──────┘ └───── message ─────┘
```

`parse_message` lifts that apart: the level becomes an **exact** severity (not a guess), the
logger and thread become attributes, and the body is reduced to the message. The leading
timestamp is discarded — it is the application's own, rendered in the instance's local
timezone, so it disagrees with `Timestamp` by the UTC offset and is redundant besides.
Nothing is lost: the prefix is fully described by the resulting fields.

The format is the one Core and Supervisor both configure in their `bootstrap.py`
(`%(asctime)s.%(msecs)03d %(levelname)s (%(threadName)s) [%(name)s] %(message)s`) — not
Python's default, which is why this lives here rather than in `internal/logparser`. The level
is mapped through `logparser.DeduceSeverity`, shared with the other parsers.

This applies to `core` and `supervisor`, and to add-ons that log through the same formatter.
Host, `dns` and other plugin logs are heterogeneous — systemd prose, `kernel`, CoreDNS
`[INFO]`, and `containerd`/`dockerd` **logfmt** — and are left untouched.

`odblogparser` exposes oteldb's shared parsers (`logfmt`, `generic-json`, `klog`,
`zap-development`) as a stanza operator, but stanza operators only run inside stanza-based
receivers such as `filelog`. This receiver is not stanza-based, and odbagent does not register
`logstransformprocessor`, so **`odblogparser` cannot currently be applied to this receiver's
output**. Structuring the remaining host formats needs either that processor or support here.

### Severity fallback

`severity_from_message` covers what the above does not recognize, by looking for an exact
upper-case level token (`ERROR`, `WARNING`, …) among the first few tokens. It is a heuristic,
off by default, and will not label logs that use another convention. Note that it does not
catch logfmt's `level=error`, which is lower-case and not a bare token.

The receiver sends `?verbose` to select the formatter that carries the timestamp, hostname
and identifier, and `?no_colors` so Supervisor strips ANSI escapes server-side.

**Only the first cursor of a response is returned.** Home Assistant exposes
`X-First-Cursor` and nothing else, so the read position cannot be a single cursor. It is
instead an anchor cursor plus the number of entries already consumed after it, sent as
`Range: entries=<anchor>:<skip>:<batch>`. Each response re-anchors on its own first entry, so
`skip` stays bounded by `batch_size`. A source with no stored cursor anchors at the tail with
`entries=:-1:2` and emits nothing.

The `2` matters: `entries=:-1:N` starts one entry *before* the last and returns N, so `:-1:1`
anchors on the second-to-last entry and the first poll re-emits the last one as if it were
new. Supervisor clamps its own `lines` parameter to a minimum of 2 for the same reason.

### Multi-line events

journald splits a multi-line message at every newline, so a Python traceback arrives as one
journal entry per line — each with a full envelope, the same identifier and PID, and
near-identical timestamps. Emitted verbatim that is one severity-less record per stack frame.

`recombine_window` joins them back: a fragment is appended to the preceding entry when it
comes from the same process, does not itself start an application log line, and follows
within the window. Only an application log line opens a block — otherwise sources that never
use the format, like systemd and CoreDNS, would have runs of unrelated entries merged.

In a 6000-entry sample the gap between fragments of one message was 7ms at p99, while
unrelated entries from the same process sat minutes apart, so the 1s default separates them
with a wide margin. Set it to `0` to disable and emit one record per journal entry.

Recombination never changes the journal entry count the cursor arithmetic depends on. A block
straddling a `batch_size` boundary is the one case it cannot join, and yields two records.

## Delivery

At-least-once. The cursor advances only after `ConsumeLogs` returns nil, and is persisted to
the storage extension in the same step. A failure re-reads the same window, so duplicates are
possible after a crash or a downstream error; gaps are not.

Without a `storage` extension, cursors live only in memory and every restart re-anchors at
the tail, losing everything in between.

## Emitted data

Resource attributes:

| Attribute | Source |
|---|---|
| `service.namespace` | always `homeassistant` |
| `service.name` | `SYSLOG_IDENTIFIER`, falling back to the configured source name |
| `host.name` | `_HOSTNAME`, omitted when absent |

Record fields:

| Field | Source |
|---|---|
| `Timestamp` | `__REALTIME_TIMESTAMP`, millisecond precision |
| `ObservedTimestamp` | receive time |
| `Body` | `MESSAGE`, with continuation lines rejoined and the application prefix removed when `parse_message` applies |
| `SeverityNumber`/`SeverityText` | the application level, else the `severity_from_message` heuristic |

Record attributes:

| Attribute | Description |
|---|---|
| `ha.source` | configured source kind |
| `ha.addon` | add-on slug, only for `addon` sources |
| `ha.logger` | application logger name, when `parse_message` applies |
| `ha.thread` | application thread name, when `parse_message` applies and it is non-empty |
| `process.pid` | `_PID`, omitted when absent |

No OpenTelemetry semantic convention exists for home automation, so `ha.*` is defined here.
Keep it internally consistent so it can be proposed upstream rather than reverse-engineered
later.

## Not implemented

**Metrics.** Home Assistant exposes `/api/prometheus`, which the stock `prometheusreceiver`
scrapes with a bearer token.

**Host log formats.** CoreDNS `[INFO]`, NetworkManager `<info>`, and `containerd`/`dockerd`
logfmt are passed through unparsed. `severity_from_message` does not catch them either — it
wants a bare upper-case token. Measured on a live instance, this leaves roughly half of all
records without a severity.

**The WebSocket API.** `system_log/list` over `/api/websocket` returns Core log entries fully
structured, with the whole traceback in one `exception` field and the source file and line —
strictly better data than the journal text. It is not used because as a transport it is
worse: `system_log` captures **WARNING and above only**, covers Core alone (Supervisor is a
separate process), dedupes by (logger, source, root cause) so repeated occurrences collapse
into a `count`, has no cursor so a dropped connection loses events, and live push needs
`system_log: fire_event: true` set in `configuration.yaml` on the instance — a host-side
change this receiver otherwise avoids. It is worth adding later as an optional enrichment
stream alongside the journal, not as a replacement.

**Automation traces.** Home Assistant's automation engine produces trace-shaped data
(`run_id`, parent context, per-step timings) in a bounded ring buffer. Not built.
