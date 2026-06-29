package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"strings"
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
