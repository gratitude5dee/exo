# Exo run API (`exo agentd`)

`exo agentd` is the executor/streaming surface: it serves one exo agent over
the same HTTP contract Hermes' `api_server` exposes, so a control plane
(airv2) can create turns, follow them as SSE, stop them, and read session
transcripts without linking the executor in-process.

It is **not** the exoharness substrate transport described in
[`http.md`](./http.md). `exo serve` exposes exoharness primitives over a
loopback-only unary `POST /request`; `exo agentd` exposes agent turns over an
authenticated, network-reachable run API. The two never share a listener.

```
exo --root ~/.exo agentd --agent air --bind 0.0.0.0:8642
```

| Flag / env | Meaning |
| --- | --- |
| `--agent` / `EXO_AGENT` | Agent slug to serve. Every session id becomes a conversation slug on this agent. |
| `--bind` / `API_SERVER_HOST_PORT` | Listen address. Default `127.0.0.1:8642`. |
| `API_SERVER_KEY` | Bearer token clients must send as `Authorization: Bearer <key>`. Required for any non-loopback bind; on loopback it is optional. |
| `-v` | Extra tracing on the `exo::agentd` target. |

The loopback rule from `exo serve` is deliberately relaxed here and only
here: binding `0.0.0.0` is refused unless `API_SERVER_KEY` is set. Inside an
airv2 Box the daemon sits behind `host --private`, so the hosted-route token
gates the network path and the API key gates the API — both, not either.

## Model binding

The agent's model is whatever binding its config names. For a gateway-backed
box register the secret and model once at provision time, then point the
agent at it:

```
exo secret set GATEWAY_TOKEN --env GATEWAY_TOKEN
exo model register gateway --model <upstream-model> --secret GATEWAY_TOKEN \
  --base-url https://<control-plane>/api/gateway/v1
exo --harness exo agent create Air --slug air --module exo/harness.ts --model gateway
```

No provider key is stored in the box; the only credential is the gateway
token, and it lives in the exoharness secret store.

## Endpoints

All routes except `GET /health` require the bearer key when one is
configured. Errors are `{"error": "..."}`.

### `GET /health`

`200 {"status":"ok","agent":"<slug>","active_runs":N}`.

### `POST /v1/runs`

```json
{
  "input": "text of the user turn",
  "session_id": "air-main",
  "conversation_history": [{"role": "user", "content": "..."}],
  "metadata": {"channel": "imessage"}
}
```

- `session_id` resolves to the conversation with that slug; a missing
  conversation is created. Omit it to get a fresh single-use session.
- `conversation_history` is accepted for contract parity and ignored: exo
  conversations are durable, so the turn already sees prior messages.
- Returns `202 {"run_id": "...", "session_id": "..."}` immediately. The turn
  runs asynchronously through `HarnessConversation::send_stream`.

### `GET /v1/runs/{run_id}/events`

`text/event-stream`. Replays every event emitted so far, then follows live
until a terminal event. Each frame is

```
event: <name>
data: {"event": "<name>", ...}

```

| `event` | Payload | Source |
| --- | --- | --- |
| `run.started` | `run_id`, `session_id` | run accepted |
| `message.delta` | `delta` | `ExecutionStreamEvent::Chunk` text |
| `tool.started` | `tool_call_id`, `tool`, `arguments` | `ExecutionStreamEvent::ToolCall` |
| `tool.completed` | `tool_call_id`, `tool`, `result` | `ExecutionStreamEvent::ToolResult` |
| `run.completed` | `run_id`, `output` | stream ended; `output` is the concatenated deltas (or the final assistant message when nothing streamed) |
| `run.failed` | `run_id`, `error` | executor error, or the run was stopped |

`run.completed` and `run.failed` are terminal; the stream closes after them.

### `GET /v1/runs/{run_id}`

`{"run_id","session_id","status"}` with `status` one of `running`,
`completed`, `failed`, `stopped`.

### `POST /v1/runs/{run_id}/stop`

Cancels the executor's turn (the model call or tool dispatch stops at its
next await point) and the SSE relay. The turn record is still finished — the
`TurnStarted` event and user message that `begin_turn` persisted are closed
out with `TurnFinished`, and the trace is finalized — so the conversation
history stays consistent and the next `POST /v1/runs` on the same session
starts a fresh turn only after that cleanup releases the conversation send
lock (the lock outlives the relay, not just the stream consumer). Subscribers receive
`run.failed` with `"error":"run stopped"`; status becomes `stopped`.

### `POST /v1/runs/{run_id}/approval`

```json
{"approved": true, "tool_call_id": "call_1"}
```

Exo's executor has no human-in-the-loop gate, so approvals are recorded on
the run and never pause execution. `approved: false` stops the run. Returns
`{"run_id","approved","status"}`.

### `GET /api/sessions`

`[{"id","title","message_count"}]` — one row per conversation on the served
agent. `id` is the conversation slug.

### `POST /api/sessions`

`{"id": "air-main", "title": "Air"}` creates a conversation with that slug.
`201` with the session row, `409` if it already exists.

### `GET /api/sessions/{id}/messages`

`[{"role": "user" | "assistant", "content": "..."}]` projected from the
conversation's durable transcript. System, developer, and tool rows are
omitted.

## Run lifetime

Runs live in memory. Running runs are always kept; the 64 most recent
terminal runs are retained for late `/events` subscribers and older ones are
evicted (`404`). A subscriber that falls more than 1024 events behind is
disconnected instead of buffered without bound. Restarting the daemon
forgets run ids (a subscriber gets `404`), but conversations and their
transcripts persist in exoharness, so the next `POST /v1/runs` with the same
`session_id` continues where the previous turn left off.
