# APEX — autonomous agent backend (Grok edition)

APEX is a task-orchestration server: give it a goal, it breaks the goal into
subtasks, runs them with real AI workers, streams progress over WebSockets,
and saves real artifacts. A web UI is served at `/`.

## How it works

```
POST /task  {"goal": "..."}
   └─> planner (LLM) breaks the goal into subtasks: browser / data / file
   └─> workers run subtasks concurrently, streaming progress over /ws
   └─> artifacts land in workspace/<task-id>/
GET /task/{id}/status  → full task state, subtask results, logs
```

- **Brain** (`brain.go`): chat-completions LLM client. No fake work — when the
  brain is not configured, subtasks fail honestly instead of pretending.
- **Planner** (`planGoal`): asks the brain for a subtask plan as JSON; falls
  back to the legacy keyword planner if the brain is unreachable.
- **Hands** (`runSubTask`):
  - `file` — the brain writes real content; saved under `workspace/<task-id>/`.
  - `data` — the brain analyzes/transforms; result persisted as an artifact.
  - `browser` (v1) — fetches page HTML over HTTP and has the brain summarize
    it. This is fetch-and-summarize, not a driven browser; results are labeled.

## Configuration

All via environment variables — never commit secrets.

| Variable      | Default                                   | Notes                                    |
|---------------|-------------------------------------------|------------------------------------------|
| `HF_TOKEN`    | *(required)*                              | Bearer token for the inference provider. Without it the brain refuses to fake work. |
| `LLM_BASE_URL`| `https://router.huggingface.co/v1`        | OpenAI-compatible chat-completions base. |
| `LLM_MODEL`   | `zai-org/GLM-5.3`                         | Any chat model the endpoint serves.      |
| `LLM_TIMEOUT_S`| `120`                                    | Per-request timeout in seconds.          |
| `PORT`        | `8080`                                    | HTTP port.                               |

Get a free token at https://huggingface.co/settings/tokens (read-only is enough
for inference). On Render, set `HF_TOKEN` in the dashboard under Environment —
`render.yaml` already declares it with `sync: false` so it is never committed.

## Run locally

```bash
go build -o apex .
HF_TOKEN=your_token_here ./apex
# POST a goal:
curl -X POST localhost:8080/task -H 'Content-Type: application/json' \
  -d '{"goal":"Write a launch checklist for my course. Save it as checklist.md"}'
# Watch it:
curl localhost:8080/task/<task_id>/status
```

## Deploy

`render.yaml` builds and starts the service. After first deploy, set
`HF_TOKEN` in the Render dashboard and redeploy (or just restart).

## API

| Method | Path                  | Purpose                              |
|--------|-----------------------|--------------------------------------|
| GET    | `/`                   | Web UI                               |
| GET    | `/ws`                 | WebSocket: live logs + task updates   |
| POST   | `/task`               | Create a task: `{"goal":"..."}`      |
| GET    | `/tasks`              | Office history: task summaries, newest first |
| GET    | `/task/{id}/status`   | Task state, subtasks, artifacts      |
| POST   | `/task/{id}/stop`     | Stop a running task                  |
| GET    | `/task/{id}/artifact/{name}` | Download a finished file      |
| POST   | `/agent/communicate`  | Chat with the brain: `{"message":"…"}`|
| GET    | `/dashboard`          | All tasks (JSON)                     |

## Office history (the notebook)

Tasks used to live only in server memory and files on the ephemeral disk —
a redeploy wiped both. Now the office keeps a notebook:

- Every task snapshot (goal, status, plan, logs, artifact names) is written
  to the notebook on every update.
- Every finished file's contents are written to the notebook too, so
  downloads keep working after restarts and redeploys.
- On boot the office reloads everything. Tasks caught mid-flight are marked
  `failed` with an honest note instead of left "running" forever.

The notebook is Postgres. Set `DATABASE_URL` on the Render service (a Neon
free-tier database is plenty — office files are kilobytes) and the office
creates its two tables itself on boot (`office_tasks`, `office_artifacts`;
see `migrations/0001_office_history.sql`). With no `DATABASE_URL` the office
runs on memory and history does not survive restarts — the logs say which
notebook is in use at startup.

## Known stubs

- `POST /deploy-nft` returns a random fake transaction hash. It is a demo
  endpoint; wire a real chain client before using it for anything real.
- `browser` subtasks do plain HTTP fetches. Pages behind login, JS-heavy
  apps, or bot protection will not render — that needs a driven browser,
  which is a future milestone, not this one.
