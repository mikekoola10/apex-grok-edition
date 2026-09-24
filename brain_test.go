package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// fakeBrain serves canned chat-completions responses for tests.
func fakeBrain(t *testing.T, reply string, status int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			t.Error("expected Authorization header")
		}
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if status != http.StatusOK {
			w.Write([]byte(`{"error":{"message":"boom"}}`))
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"choices": []map[string]interface{}{
				{"message": map[string]string{"content": reply}},
			},
		})
	}))
}

func withBrainEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	t.Setenv("LLM_BASE_URL", srv.URL)
	t.Setenv("LLM_MODEL", "test-model")
	t.Setenv("HF_TOKEN", "test-token")
}

func clearBrainEnv(t *testing.T) {
	t.Helper()
	t.Setenv("HF_TOKEN", "")
	os.Unsetenv("HF_TOKEN")
}

func TestBrainUnavailableWithoutToken(t *testing.T) {
	clearBrainEnv(t)
	if brainAvailable() {
		t.Error("brain should not be available without HF_TOKEN")
	}
}

func TestChatCompleteHonestError(t *testing.T) {
	clearBrainEnv(t)
	if _, err := chatComplete("s", "u"); err == nil || !strings.Contains(err.Error(), "HF_TOKEN") {
		t.Errorf("expected HF_TOKEN error, got %v", err)
	}

	srv := fakeBrain(t, "x", http.StatusInternalServerError)
	defer srv.Close()
	withBrainEnv(t, srv)
	if _, err := chatComplete("s", "u"); err == nil || !strings.Contains(err.Error(), "500") {
		t.Errorf("expected HTTP 500 error, got %v", err)
	}
}

func TestChatCompleteOK(t *testing.T) {
	srv := fakeBrain(t, "hello from brain", http.StatusOK)
	defer srv.Close()
	withBrainEnv(t, srv)
	out, err := chatComplete("sys", "user")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != "hello from brain" {
		t.Errorf("unexpected reply %q", out)
	}
}

func TestPlanGoalFallsBackWithoutBrain(t *testing.T) {
	clearBrainEnv(t)
	subs := planGoal("research nft")
	if len(subs) != 3 {
		t.Errorf("expected legacy 3-subtask plan, got %d", len(subs))
	}
}

func TestPlanGoalUsesBrain(t *testing.T) {
	plan := `{"subtasks":[{"type":"file","goal":"Write report.md summarizing findings"},{"type":"bogus","goal":"Do the thing"}]}`
	srv := fakeBrain(t, plan, http.StatusOK)
	defer srv.Close()
	withBrainEnv(t, srv)
	subs := planGoal("write me a report")
	if len(subs) != 2 {
		t.Fatalf("expected 2 subtasks, got %d", len(subs))
	}
	if subs[0].Type != "file" || subs[1].Type != "data" {
		t.Errorf("bad types: %q %q (bogus should default to data)", subs[0].Type, subs[1].Type)
	}
}

func TestPlanGoalBadJSONFallsBack(t *testing.T) {
	srv := fakeBrain(t, "not json at all", http.StatusOK)
	defer srv.Close()
	withBrainEnv(t, srv)
	subs := planGoal("research nft")
	if len(subs) != 3 {
		t.Errorf("expected legacy fallback plan, got %d", len(subs))
	}
}

func TestRunSubTaskHonestWithoutBrain(t *testing.T) {
	clearBrainEnv(t)
	task := &Task{ID: "t1", Goal: "g"}
	st := &SubTask{ID: "s1", Type: "file", Goal: "write notes.md"}
	if _, _, err := runSubTask(task, st); err == nil || !strings.Contains(err.Error(), "HF_TOKEN") {
		t.Errorf("expected honest brain-missing error, got %v", err)
	}
}

func TestGenerateFileWritesRealArtifact(t *testing.T) {
	srv := fakeBrain(t, "# Report\nReal content here.", http.StatusOK)
	defer srv.Close()
	withBrainEnv(t, srv)

	dir := t.TempDir()
	oldWd, _ := os.Getwd()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(oldWd)

	task := &Task{ID: "abc123", Goal: "summarize"}
	st := &SubTask{ID: "s1", Type: "file", Goal: "Write summary.md"}
	res, _, err := runSubTask(task, st)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	data, err := os.ReadFile("workspace/abc123/summary.md")
	if err != nil {
		t.Fatalf("artifact not written: %v", err)
	}
	if !strings.Contains(string(data), "Real content here") {
		t.Errorf("artifact has wrong content: %q (result: %q)", data, res)
	}
}
