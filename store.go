package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/lib/pq"
)

// ---------------------------------------------------------------------------
// TaskStore: the office notebook.
//
// The office used to keep everything in its head (the tasks map). When the
// building was rebuilt (redeploy / restart), the head went with it and
// finished work vanished. A TaskStore is the notebook that lives outside the
// building: every task and finished file is written down as it happens, and
// reloaded when the office boots.
//
// Two notebooks exist:
//   - memoryStore: the original volatile behavior. Used when DATABASE_URL is
//     unset. Work still disappears on restart — honestly documented.
//   - pgStore: Postgres (Neon on the free tier is plenty — office files are
//     kilobytes). Survives restarts and redeploys.
//
// The store works on taskRow snapshots. Callers snapshot under the global mu
// (see persistTask); the store itself never touches task locks, so DB I/O
// never blocks the office.
// ---------------------------------------------------------------------------

// TaskStore persists task snapshots and finished-file contents.
type TaskStore interface {
	UpsertTask(r taskRow) error
	AllTasks() ([]taskRow, error) // newest first
	SaveArtifact(taskID, filename, content string) error
	GetArtifact(taskID, filename string) (string, error)
}

// ErrArtifactNotFound is returned when the notebook has no such file.
var ErrArtifactNotFound = fmt.Errorf("artifact not found")

// taskRow is the flat, DB-friendly snapshot of a Task.
type taskRow struct {
	ID        string
	Goal      string
	Status    string
	Progress  int
	PlanJSON  string // SubTasks as JSON
	LogsJSON  string // Logs as JSON
	Artifacts []string
	SandboxID string
	CreatedAt time.Time
}

// snapshotTask flattens a Task. The caller must hold mu (read or write).
func snapshotTask(t *Task) taskRow {
	plan, _ := json.Marshal(t.SubTasks)
	logs, _ := json.Marshal(t.Logs)
	return taskRow{
		ID: t.ID, Goal: t.Goal, Status: t.Status, Progress: t.Progress,
		PlanJSON: string(plan), LogsJSON: string(logs),
		Artifacts: append([]string{}, t.Artifacts...),
		SandboxID: t.SandboxID, CreatedAt: t.CreatedAt,
	}
}

// toTask rebuilds a Task from a row.
func (r taskRow) toTask() *Task {
	t := &Task{
		ID: r.ID, Goal: r.Goal, Status: r.Status, Progress: r.Progress,
		Artifacts: append([]string{}, r.Artifacts...),
		SandboxID: r.SandboxID, CreatedAt: r.CreatedAt, UpdatedAt: time.Now(),
		Logs: []LogEntry{},
	}
	var plan []SubTask
	if err := json.Unmarshal([]byte(r.PlanJSON), &plan); err == nil {
		t.SubTasks = plan
	}
	var logs []LogEntry
	if err := json.Unmarshal([]byte(r.LogsJSON), &logs); err == nil {
		t.Logs = logs
	}
	return t
}

// settleInterrupted marks a task that was mid-flight when the office went down
// as failed with an honest note, instead of leaving it "running" forever.
// Pure function on the row: returns the settled row and whether it changed.
func settleInterrupted(r taskRow) (taskRow, bool) {
	if r.Status != "running" && r.Status != "pending" {
		return r, false
	}
	t := r.toTask()
	t.Status = "failed"
	t.Logs = append(t.Logs, LogEntry{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Action:    "Note",
		Agent:     "JARVIS",
		Details:   "The office restarted while this was running, so it was marked failed instead of left hanging.",
	})
	for i := range t.SubTasks {
		if t.SubTasks[i].Status == "pending" || t.SubTasks[i].Status == "running" {
			t.SubTasks[i].Status = "failed"
			t.SubTasks[i].Result = "office restarted mid-task"
		}
	}
	return snapshotTask(t), true
}

// ---------------- memoryStore: volatile, original behavior ----------------

type memoryStore struct {
	mu        sync.Mutex
	tasks     map[string]taskRow
	artifacts map[string]map[string]string // taskID -> filename -> content
}

func newMemoryStore() *memoryStore {
	return &memoryStore{
		tasks:     map[string]taskRow{},
		artifacts: map[string]map[string]string{},
	}
}

func (s *memoryStore) UpsertTask(r taskRow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[r.ID] = r
	return nil
}

func (s *memoryStore) AllTasks() ([]taskRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]taskRow, 0, len(s.tasks))
	for _, r := range s.tasks {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

func (s *memoryStore) SaveArtifact(taskID, filename, content string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.artifacts[taskID] == nil {
		s.artifacts[taskID] = map[string]string{}
	}
	s.artifacts[taskID][filename] = content
	return nil
}

func (s *memoryStore) GetArtifact(taskID, filename string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.artifacts[taskID][filename]; ok {
		return c, nil
	}
	return "", ErrArtifactNotFound
}

// ---------------- pgStore: durable Postgres notebook ----------------

const officeSchema = `
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
`

type pgStore struct {
	db *sql.DB
}

func newPostgresStore(dsn string) (*pgStore, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("postgres ping: %w", err)
	}
	if _, err := db.Exec(officeSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("postgres schema: %w", err)
	}
	return &pgStore{db: db}, nil
}

func (s *pgStore) UpsertTask(r taskRow) error {
	_, err := s.db.Exec(`
INSERT INTO office_tasks (id, goal, status, progress, plan, logs, artifacts, sandbox_id, created_at, updated_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9, now())
ON CONFLICT (id) DO UPDATE SET
    goal=EXCLUDED.goal, status=EXCLUDED.status, progress=EXCLUDED.progress,
    plan=EXCLUDED.plan, logs=EXCLUDED.logs, artifacts=EXCLUDED.artifacts,
    sandbox_id=EXCLUDED.sandbox_id, updated_at=now()`,
		r.ID, r.Goal, r.Status, r.Progress, r.PlanJSON, r.LogsJSON,
		pq.Array(r.Artifacts), r.SandboxID, r.CreatedAt)
	return err
}

func (s *pgStore) AllTasks() ([]taskRow, error) {
	rows, err := s.db.Query(`
SELECT id, goal, status, progress, plan::text, logs::text, artifacts, sandbox_id, created_at
FROM office_tasks ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []taskRow
	for rows.Next() {
		var r taskRow
		if err := rows.Scan(&r.ID, &r.Goal, &r.Status, &r.Progress, &r.PlanJSON,
			&r.LogsJSON, pq.Array(&r.Artifacts), &r.SandboxID, &r.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *pgStore) SaveArtifact(taskID, filename, content string) error {
	_, err := s.db.Exec(`
INSERT INTO office_artifacts (task_id, filename, content)
VALUES ($1,$2,$3)
ON CONFLICT (task_id, filename) DO UPDATE SET content=EXCLUDED.content`,
		taskID, filename, content)
	return err
}

func (s *pgStore) GetArtifact(taskID, filename string) (string, error) {
	var content string
	err := s.db.QueryRow(
		`SELECT content FROM office_artifacts WHERE task_id=$1 AND filename=$2`,
		taskID, filename).Scan(&content)
	if err == sql.ErrNoRows {
		return "", ErrArtifactNotFound
	}
	return content, err
}

// ---------------- construction ----------------

// NewTaskStore picks the notebook: Postgres when DATABASE_URL is set and
// reachable, otherwise the volatile memory store (with an honest log line).
func NewTaskStore() TaskStore {
	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		if pg, err := newPostgresStore(dsn); err == nil {
			log.Printf("[store] office notebook: postgres connected — history survives restarts")
			return pg
		} else {
			log.Printf("[store] postgres unreachable (%v) — memory notebook, history will NOT survive restarts", err)
		}
	} else {
		log.Printf("[store] DATABASE_URL not set — memory notebook, history will NOT survive restarts")
	}
	return newMemoryStore()
}
