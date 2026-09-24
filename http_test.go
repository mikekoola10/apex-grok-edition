package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHTTPServer(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(indexHTML))
	})
	mux.HandleFunc("GET /dashboard", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Dashboard"))
	})

	// Test root endpoint
	req, _ := http.NewRequest("GET", "/", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status OK, got %v", rr.Code)
	}

	cacheControl := rr.Header().Get("Cache-Control")
	if !strings.Contains(cacheControl, "no-cache") {
		t.Errorf("Expected Cache-Control header, got %v", cacheControl)
	}

	if !strings.Contains(rr.Body.String(), "APEX JARVIS") {
		t.Errorf("Expected body to contain 'APEX JARVIS'")
	}

	// Test dashboard endpoint
	req, _ = http.NewRequest("GET", "/dashboard", nil)
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status OK for /dashboard, got %v", rr.Code)
	}
}

// The office API must be callable from Nova's frontend (a different origin),
// and artifacts the office produced must be downloadable by file name.
func TestOfficeAPI_CORSAndArtifacts(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /task/{id}/artifact/{name}", getArtifact)
	handler := corsMiddleware(mux, "*")

	// Seed a finished task with a real artifact file.
	id := "test-task-1"
	dir := workspaceDir(id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("workspace")
	content := []byte("# hello office")
	if err := os.WriteFile(filepath.Join(dir, "note.md"), content, 0o644); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	tasks[id] = &Task{ID: id, Goal: "test", Status: "completed", Artifacts: []string{"note.md"}}
	mu.Unlock()
	defer func() {
		mu.Lock()
		delete(tasks, id)
		mu.Unlock()
	}()

	// 1. CORS preflight answers.
	req, _ := http.NewRequest(http.MethodOptions, "/task/"+id+"/artifact/note.md", nil)
	req.Header.Set("Origin", "https://koola10-studio-static.onrender.com")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected 204 preflight, got %v", rr.Code)
	}
	if rr.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("missing CORS allow-origin header")
	}

	// 2. Known artifact downloads with CORS headers.
	req, _ = http.NewRequest(http.MethodGet, "/task/"+id+"/artifact/note.md", nil)
	req.Header.Set("Origin", "https://koola10-studio-static.onrender.com")
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 for artifact, got %v", rr.Code)
	}
	if rr.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("missing CORS header on artifact response")
	}
	if !strings.Contains(rr.Body.String(), "hello office") {
		t.Fatalf("artifact body wrong: %q", rr.Body.String())
	}

	// 3. Unknown artifact name -> 404.
	req, _ = http.NewRequest(http.MethodGet, "/task/"+id+"/artifact/secret.txt", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown artifact, got %v", rr.Code)
	}

	// 4. Path traversal attempt -> 404 (name must match recorded artifact).
	req, _ = http.NewRequest(http.MethodGet, "/task/"+id+"/artifact/..%2F..%2Fetc%2Fpasswd", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for traversal, got %v", rr.Code)
	}
}
