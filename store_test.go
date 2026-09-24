package main

import (
	"testing"
	"time"
)

func sampleTask() *Task {
	return &Task{
		ID:        "hist-test-1",
		Goal:      "Write a field guide",
		Status:    "completed",
		Progress:  100,
		SubTasks:  []SubTask{{ID: "s1", Type: "file", Goal: "Write", Status: "completed", Result: "wrote 10 bytes"}},
		Artifacts: []string{"guide.md"},
		Logs:      []LogEntry{{Timestamp: "2026-09-24T00:00:00Z", Action: "Finalized", Agent: "JARVIS"}},
		SandboxID: "sandbox-abc",
		CreatedAt: time.Now(),
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	orig := sampleTask()
	row := snapshotTask(orig)
	back := row.toTask()
	if back.ID != "hist-test-1" || back.Goal != "Write a field guide" {
		t.Fatalf("identity lost: %+v", back)
	}
	if len(back.SubTasks) != 1 || back.SubTasks[0].Type != "file" {
		t.Fatalf("plan lost: %+v", back.SubTasks)
	}
	if len(back.Logs) != 1 || back.Logs[0].Agent != "JARVIS" {
		t.Fatalf("logs lost: %+v", back.Logs)
	}
	if len(back.Artifacts) != 1 || back.Artifacts[0] != "guide.md" {
		t.Fatalf("artifacts lost: %+v", back.Artifacts)
	}
	if !back.CreatedAt.Equal(orig.CreatedAt) {
		t.Fatalf("created_at lost: %v", back.CreatedAt)
	}
}

func TestSettleInterrupted(t *testing.T) {
	// A task caught mid-flight is marked failed with an honest note.
	running := snapshotTask(&Task{
		ID: "r1", Goal: "g", Status: "running", Progress: 50,
		SubTasks:  []SubTask{{ID: "s1", Type: "file", Goal: "w", Status: "running"}},
		CreatedAt: time.Now(),
	})
	settled, changed := settleInterrupted(running)
	if !changed || settled.Status != "failed" {
		t.Fatalf("running task not settled: %v %v", settled.Status, changed)
	}
	back := settled.toTask()
	if len(back.Logs) == 0 {
		t.Fatal("settle left no honest note in the log")
	}
	if back.SubTasks[0].Status != "failed" {
		t.Fatalf("running subtask not settled: %s", back.SubTasks[0].Status)
	}

	// Terminal tasks are untouched.
	done := snapshotTask(sampleTask())
	if _, changed := settleInterrupted(done); changed {
		t.Fatal("completed task was modified by settle")
	}
}

func TestMemoryStore(t *testing.T) {
	s := newMemoryStore()

	if _, err := s.GetArtifact("nope", "nope.md"); err != ErrArtifactNotFound {
		t.Fatalf("expected ErrArtifactNotFound, got %v", err)
	}

	if err := s.UpsertTask(snapshotTask(sampleTask())); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveArtifact("hist-test-1", "guide.md", "# guide"); err != nil {
		t.Fatal(err)
	}

	rows, err := s.AllTasks()
	if err != nil || len(rows) != 1 {
		t.Fatalf("AllTasks: %v %d", err, len(rows))
	}
	if rows[0].toTask().Goal != "Write a field guide" {
		t.Fatal("task not restored from memory store")
	}
	content, err := s.GetArtifact("hist-test-1", "guide.md")
	if err != nil || content != "# guide" {
		t.Fatalf("artifact not restored: %v %q", err, content)
	}

	// Newest first.
	older := snapshotTask(&Task{ID: "old", Goal: "old", Status: "completed", CreatedAt: time.Now().Add(-time.Hour)})
	if err := s.UpsertTask(older); err != nil {
		t.Fatal(err)
	}
	rows, _ = s.AllTasks()
	if len(rows) != 2 || rows[0].ID != "hist-test-1" {
		t.Fatalf("ordering wrong: %+v", rows)
	}

	// Upsert overwrites.
	upd := snapshotTask(sampleTask())
	upd.Status = "failed"
	if err := s.UpsertTask(upd); err != nil {
		t.Fatal(err)
	}
	rows, _ = s.AllTasks()
	for _, r := range rows {
		if r.ID == "hist-test-1" && r.Status != "failed" {
			t.Fatal("upsert did not overwrite")
		}
	}
}
