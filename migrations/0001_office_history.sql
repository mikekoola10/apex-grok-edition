-- Office notebook: durable history for Nova's office.
-- The office applies this itself on boot (CREATE TABLE IF NOT EXISTS), so
-- running it by hand is optional — it exists here as the readable record of
-- what the office remembers.
--
-- How to use: create a free Postgres (Neon free tier is plenty — office files
-- are kilobytes), then set DATABASE_URL on the Render service to the
-- connection string. No DATABASE_URL = memory notebook = history vanishes on
-- restart (the old behavior).

CREATE TABLE IF NOT EXISTS office_tasks (
    id         TEXT PRIMARY KEY,
    goal       TEXT NOT NULL,
    status     TEXT NOT NULL,
    progress   INT NOT NULL DEFAULT 0,
    plan       JSONB NOT NULL DEFAULT '[]',
    logs       JSONB NOT NULL DEFAULT '[]',
    artifacts  TEXT[] NOT NULL DEFAULT '{}',
    sandbox_id TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS office_artifacts (
    task_id    TEXT NOT NULL REFERENCES office_tasks(id) ON DELETE CASCADE,
    filename   TEXT NOT NULL,
    content    TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, filename)
);
