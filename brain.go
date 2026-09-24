package main

// brain.go — Apex's muscle.
//
// The executor, planner, and chat handler below used to fake their work
// (sleep 1.5s, then "task finished successfully"). This file gives Apex a
// real brain: an LLM reached over an OpenAI-compatible chat-completions
// HTTP API, plus real hands for the "browser", "data", and "file" subtask
// types.
//
// Configuration (environment variables, never hardcoded secrets):
//
//	HF_TOKEN      Bearer token for the inference provider. REQUIRED for the
//	              brain to work. When empty, every subtask fails honestly with
//	              "brain not configured" instead of fake success.
//	LLM_BASE_URL  Default: https://router.huggingface.co/v1 (auto-routes to a
//	              serving provider)
//	LLM_MODEL     Default: zai-org/GLM-5.3 (swap freely)
//	LLM_TIMEOUT_S Default: 120

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ---------------------------------------------------------------------------
// Config + client
// ---------------------------------------------------------------------------

type brainCfg struct {
	baseURL string
	model   string
	token   string
	timeout time.Duration
}

func brainConfig() (brainCfg, bool) {
	cfg := brainCfg{
		baseURL: strings.TrimRight(os.Getenv("LLM_BASE_URL"), "/"),
		model:   os.Getenv("LLM_MODEL"),
		token:   os.Getenv("HF_TOKEN"),
		timeout: 120 * time.Second,
	}
	if cfg.baseURL == "" {
		cfg.baseURL = "https://router.huggingface.co/v1"
	}
	if cfg.model == "" {
		cfg.model = "zai-org/GLM-5.3"
	}
	if s := os.Getenv("LLM_TIMEOUT_S"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			cfg.timeout = time.Duration(n) * time.Second
		}
	}
	return cfg, cfg.token != ""
}

// brainAvailable reports whether the brain is configured. When false, Apex
// refuses to fake work and says so.
func brainAvailable() bool {
	_, ok := brainConfig()
	return ok
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model       string        `json:"model"`
	Messages    []chatMessage `json:"messages"`
	Temperature float64       `json:"temperature,omitempty"`
	MaxTokens   int           `json:"max_tokens,omitempty"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// chatComplete sends a chat-completions request and returns the assistant's
// text. It never fabricates an answer: any transport or API error is returned.
func chatComplete(system, user string) (string, error) {
	cfg, ok := brainConfig()
	if !ok {
		return "", fmt.Errorf("brain not configured: set the HF_TOKEN environment variable")
	}

	body, _ := json.Marshal(chatRequest{
		Model: cfg.model,
		Messages: []chatMessage{
			{Role: "system", Content: system},
			{Role: "user", Content: user},
		},
		Temperature: 0.7,
		MaxTokens:   2048,
	})

	req, err := http.NewRequest("POST", cfg.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cfg.token)

	client := &http.Client{Timeout: cfg.timeout}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("brain request failed: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("brain API error (HTTP %d): %s", resp.StatusCode, truncate(string(respBody), 300))
	}

	var parsed chatResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return "", fmt.Errorf("brain returned unreadable JSON: %w", err)
	}
	if parsed.Error != nil {
		return "", fmt.Errorf("brain API error: %s", parsed.Error.Message)
	}
	if len(parsed.Choices) == 0 {
		return "", fmt.Errorf("brain returned no choices")
	}
	text := strings.TrimSpace(parsed.Choices[0].Message.Content)
	if text == "" {
		return "", fmt.Errorf("brain returned an empty reply")
	}
	return text, nil
}

// ---------------------------------------------------------------------------
// Planner — real planning with legacy fallback
// ---------------------------------------------------------------------------

// planGoal asks the brain to break a goal into subtasks. Without a configured
// brain it falls back to the legacy keyword planner (decomposeGoal). A brain
// that errors also falls back, and the failure is logged by the caller.
func planGoal(goal string) []SubTask {
	if !brainAvailable() {
		return decomposeGoal(goal)
	}

	system := `You are the planner for APEX, an autonomous agent. Break the user's goal into a short ordered list of subtasks. Reply with ONLY valid JSON, no other text, in this exact shape:
{"subtasks":[{"type":"browser","goal":"..."},{"type":"data","goal":"..."},{"type":"file","goal":"..."}]}
Rules:
- "type" must be one of: browser (fetch and summarize web pages), data (analyze or transform information), file (generate a document, report, or code file).
- 1 to 5 subtasks. Each "goal" is a concrete instruction for the worker agent, phrased as an imperative.
- If the goal names a file to create (e.g. "write report.md"), use type "file" and include the filename in the goal.`
	out, err := chatComplete(system, "Goal: "+goal)
	if err != nil {
		return decomposeGoal(goal)
	}

	// The model sometimes wraps JSON in fences; strip them defensively.
	out = strings.TrimSpace(out)
	out = strings.TrimPrefix(out, "```json")
	out = strings.TrimPrefix(out, "```")
	out = strings.TrimSuffix(out, "```")

	var plan struct {
		Subtasks []struct {
			Type string `json:"type"`
			Goal string `json:"goal"`
		} `json:"subtasks"`
	}
	if err := json.Unmarshal([]byte(out), &plan); err != nil || len(plan.Subtasks) == 0 {
		return decomposeGoal(goal)
	}

	subtasks := make([]SubTask, 0, len(plan.Subtasks))
	for _, s := range plan.Subtasks {
		t := strings.ToLower(strings.TrimSpace(s.Type))
		if t != "browser" && t != "data" && t != "file" {
			t = "data"
		}
		g := strings.TrimSpace(s.Goal)
		if g == "" {
			continue
		}
		subtasks = append(subtasks, SubTask{ID: uuid.New().String(), Type: t, Goal: g, Status: "pending"})
	}
	if len(subtasks) == 0 {
		return decomposeGoal(goal)
	}
	return subtasks
}

// ---------------------------------------------------------------------------
// Hands — real subtask execution
// ---------------------------------------------------------------------------

var errBrainMissing = fmt.Errorf("brain not configured: set the HF_TOKEN environment variable on the server")

// runSubTask does the actual work for one subtask and returns a human-readable
// result plus the artifact file name it produced ("" when none). It returns
// an error instead of a fake success when it cannot work.
func runSubTask(task *Task, st *SubTask) (string, string, error) {
	if !brainAvailable() {
		return "", "", errBrainMissing
	}
	switch st.Type {
	case "file":
		return generateFile(task, st)
	case "browser":
		return browseAndSummarize(task, st)
	default: // "data" and anything unknown
		return analyzeData(task, st)
	}
}

// workspaceDir is where real artifacts land. It is git-ignored.
func workspaceDir(taskID string) string {
	return filepath.Join("workspace", taskID)
}

func safeFileName(goal string) string {
	// If the goal names an explicit file (report.md, main.py...), honor it.
	if m := regexp.MustCompile(`(?i)([\w\-.]+\.(md|txt|py|go|js|ts|json|yaml|yml|html|css|csv))`).FindStringSubmatch(goal); m != nil {
		return m[1]
	}
	slug := strings.ToLower(goal)
	slug = regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(slug, "-")
	slug = strings.Trim(slug, "-")
	if len(slug) > 40 {
		slug = slug[:40]
	}
	if slug == "" {
		slug = "artifact"
	}
	return slug + ".md"
}

// generateFile asks the brain to write real content and saves it as an artifact.
func generateFile(task *Task, st *SubTask) (string, string, error) {
	system := `You are a worker agent for APEX. Generate exactly what the user asked for: a complete document, report, or code file. Output ONLY the file content — no preamble, no fences unless the content itself is code that needs them, no commentary.`
	content, err := chatComplete(system, fmt.Sprintf("Task goal: %s\nFile request: %s", task.Goal, st.Goal))
	if err != nil {
		return "", "", err
	}

	dir := workspaceDir(task.ID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", "", fmt.Errorf("could not create workspace: %w", err)
	}
	name := safeFileName(st.Goal)
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return "", "", fmt.Errorf("could not write artifact: %w", err)
	}
	return fmt.Sprintf("wrote %d bytes to %s", len(content), path), name, nil
}

// analyzeData asks the brain to actually analyze or transform information.
func analyzeData(task *Task, st *SubTask) (string, string, error) {
	system := `You are a worker agent for APEX. Analyze, summarize, or transform exactly what is asked. Be concrete and useful. No filler.`
	out, err := chatComplete(system, fmt.Sprintf("Task goal: %s\nWork item: %s", task.Goal, st.Goal))
	if err != nil {
		return "", "", err
	}

	// Persist the analysis as an artifact too, so nothing is lost.
	name := safeFileName(st.Goal)
	dir := workspaceDir(task.ID)
	if err := os.MkdirAll(dir, 0o755); err == nil {
		_ = os.WriteFile(filepath.Join(dir, name), []byte(out), 0o644)
	}
	return truncate(out, 600), name, nil
}

var urlRe = regexp.MustCompile(`https?://[^\s"'<>]+`)

// browseAndSummarize fetches real pages (v1: plain HTTP GET + summarize).
// It does not drive a browser; results are labeled accordingly.
func browseAndSummarize(task *Task, st *SubTask) (string, string, error) {
	text := task.Goal + " " + st.Goal
	urls := urlRe.FindAllString(text, 3)

	var fetched strings.Builder
	if len(urls) == 0 {
		fetched.WriteString("(no URL given — brief written from the model's knowledge; verify independently)")
	} else {
		client := &http.Client{Timeout: 20 * time.Second}
		for _, u := range urls {
			if _, err := url.ParseRequestURI(u); err != nil {
				continue
			}
			req, _ := http.NewRequest("GET", u, nil)
			req.Header.Set("User-Agent", "ApexAgent/1.0")
			resp, err := client.Do(req)
			if err != nil {
				fmt.Fprintf(&fetched, "\n[fetch failed: %s: %v]\n", u, err)
				continue
			}
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 200*1024))
			resp.Body.Close()
			fmt.Fprintf(&fetched, "\n--- %s ---\n%s\n", u, truncate(stripHTML(string(body)), 6000))
		}
	}

	system := `You are a worker agent for APEX. Summarize the fetched page content below into a tight, useful brief. If a fetch failed or no URL was given, say so plainly and give the best knowledge-based brief you can, labeled as such.`
	out, err := chatComplete(system, fmt.Sprintf("Task goal: %s\nWork item: %s\n\nFetched content:\n%s", task.Goal, st.Goal, fetched.String()))
	if err != nil {
		return "", "", err
	}

	name := safeFileName(st.Goal)
	dir := workspaceDir(task.ID)
	if err := os.MkdirAll(dir, 0o755); err == nil {
		_ = os.WriteFile(filepath.Join(dir, name), []byte(out), 0o644)
	}
	return truncate(out, 600), name, nil
}

var tagRe = regexp.MustCompile(`(?s)<script.*?</script>|(?s)<style.*?</style>|(?s)<!--.*?-->|<[^>]+>`)

// stripHTML crudely extracts visible text for summarization.
func stripHTML(h string) string {
	t := tagRe.ReplaceAllString(h, " ")
	t = regexp.MustCompile(`\s+`).ReplaceAllString(t, " ")
	return strings.TrimSpace(t)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
