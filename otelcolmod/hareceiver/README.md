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
| `severity_from_message` | `false` | Best-effort severity detection for messages `parse_message` does not recognize. |
| `storage` | — | Storage extension holding cursors. Without it, every start begins at the tail. |

The remaining settings come from `confighttp.ClientConfig` (TLS, timeouts, extra headers,
compression).

`kind` is one of `host`, `core`, `supervisor`, or `addon`; `addon` also requires the add-on
`slug`.

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

This applies to `core` and `supervisor`, and to add-ons that log through the same formatter.
Host logs are heterogeneous — systemd prose, `kernel`, and `containerd`/`dockerd` **logfmt** —
and are left untouched. To structure those, put `odblogparser` downstream in the pipeline;
this receiver deliberately does not reimplement logfmt.

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

Because entries are counted, a multi-line `MESSAGE` must not be miscounted: a line that does
not start with a valid timestamp is treated as a continuation of the previous entry. A
continuation line that itself begins with a plausible `YYYY-MM-DD HH:MM:SS.mmm ` prefix and
contains `: ` would be over-counted and cause a gap. No Home Assistant log format does this
today.

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

Metrics — Home Assistant exposes `/api/prometheus`, which the stock `prometheusreceiver`
scrapes with a bearer token.

Automation traces — designed in §9 of the design doc, not built.
