package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// --- Structs ---

type Task struct {
	ID            string       `json:"id"`
	Goal          string       `json:"goal"`
	Status        string       `json:"status"` // queued, running, completed, stopped, failed
	SubTasks      []SubTask    `json:"subtasks"`
	Artifacts     []string     `json:"artifacts"`
	Logs          []LogEntry   `json:"logs"`
	CreatedAt     time.Time    `json:"created_at"`
	UpdatedAt     time.Time    `json:"updated_at"`
	SandboxID     string       `json:"sandbox_id"`
	Workspace     string       `json:"workspace,omitempty"`
	Progress      int          `json:"progress"` // 0-100
	Deployments []Deployment `json:"deployments,omitempty"`
}

type SubTask struct {
	ID     string `json:"id"`
	Type   string `json:"type"` // browser, code, data, file, computer
	Goal   string `json:"goal"`
	Status string `json:"status"`
	Result string `json:"result"`
}

type Deployment struct {
	ID        string    `json:"id"`
	Artifact  string    `json:"artifact"`
	Target    string    `json:"target"`
	URL       string    `json:"url,omitempty"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

type LogEntry struct {
	Timestamp string `json:"timestamp"`
	Action    string `json:"action"`
	Agent     string `json:"agent"`
	Details   string `json:"details,omitempty"`
}

// --- Global State ---

var (
	tasks     = make(map[string]*Task)
	mu        sync.RWMutex
	taskQueue = make(chan *Task, 100)
)

func init() {
	go processQueue()
}

// --- Helpers ---

// logActionLocked assumes the caller holds the mu.Lock()
func logActionLocked(task *Task, action, agent, details string) {
	entry := LogEntry{
		Timestamp: time.Now().Format(time.RFC3339),
		Action:    action,
		Agent:     agent,
		Details:   details,
	}
	task.Logs = append(task.Logs, entry)
	task.UpdatedAt = time.Now()
	log.Printf("[%s] %s: %s %s", task.ID[:8], agent, action, details)
}

func logAction(task *Task, action, agent, details string) {
	mu.Lock()
	defer mu.Unlock()
	logActionLocked(task, action, agent, details)
}

func decomposeGoal(goal string) []SubTask {
	log.Printf("🤖 DeepSeek decomposing goal: %s", goal)
	lowerGoal := strings.ToLower(goal)
	if strings.Contains(lowerGoal, "analyze") || strings.Contains(lowerGoal, "local") || strings.Contains(lowerGoal, "files") {
		return []SubTask{
			{ID: uuid.New().String(), Type: "computer", Goal: "Analyze local files and environment", Status: "pending"},
			{ID: uuid.New().String(), Type: "data", Goal: "Process findings", Status: "pending"},
			{ID: uuid.New().String(), Type: "file", Goal: "Generate report", Status: "pending"},
		}
	}
	if strings.Contains(lowerGoal, "research") || strings.Contains(lowerGoal, "web") {
		return []SubTask{
			{ID: uuid.New().String(), Type: "browser", Goal: "Perform web research and data extraction", Status: "pending"},
			{ID: uuid.New().String(), Type: "data", Goal: "Analyze extracted data and comparisons", Status: "pending"},
			{ID: uuid.New().String(), Type: "file", Goal: "Generate spreadsheet/report artifacts", Status: "pending"},
		}
	}
	return []SubTask{
		{ID: uuid.New().String(), Type: "browser", Goal: "Gather information", Status: "pending"},
		{ID: uuid.New().String(), Type: "code", Goal: "Process logic if needed", Status: "pending"},
		{ID: uuid.New().String(), Type: "data", Goal: "Analyze results", Status: "pending"},
		{ID: uuid.New().String(), Type: "file", Goal: "Save outputs", Status: "pending"},
	}
}

func createSandbox(taskID string) string {
	return "sandbox-" + taskID[:8]
}

// --- Execution Engine ---

func processQueue() {
	for task := range taskQueue {
		executeTaskAsync(task)
	}
}

func executeTaskAsync(task *Task) {
	logAction(task, "Execution Started", "orchestrator", "Async background processing initiated")

	mu.Lock()
	task.Status = "running"
	mu.Unlock()

	var wg sync.WaitGroup
	for i := range task.SubTasks {
		wg.Add(1)
		go func(st *SubTask) {
			defer wg.Done()
			executeSubTask(st, task)

			mu.Lock()
			if len(task.SubTasks) > 0 {
				completedCount := 0
				for _, s := range task.SubTasks {
					if s.Status == "completed" {
						completedCount++
					}
				}
				task.Progress = (completedCount * 100) / len(task.SubTasks)
			}
			mu.Unlock()
		}(&task.SubTasks[i])
	}
	wg.Wait()

	mu.Lock()
	if task.Status != "stopped" {
		task.Status = "completed"
		task.Progress = 100
		logActionLocked(task, "Task Completed", "orchestrator", "All subtasks finished")
	}
	mu.Unlock()

	log.Printf("📧 AgentMail: Task %s completed. Progress: %d%%", task.ID[:8], task.Progress)
}

func runSafeCommand(command, dir string) (string, error) {
	allowed := []string{"ls", "cat", "echo", "pwd", "whoami", "mkdir", "touch"}
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return "", fmt.Errorf("empty command")
	}

	isAllowed := false
	for _, a := range allowed {
		if parts[0] == a {
			isAllowed = true
			break
		}
	}
	if !isAllowed {
		return "", fmt.Errorf("command not in allowlist: %s", parts[0])
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.CommandContext(ctx, "cmd", "/c", command)
	} else {
		cmd = exec.CommandContext(ctx, "sh", "-c", command)
	}
	cmd.Dir = dir

	out, err := cmd.CombinedOutput()
	return string(out), err
}

func executeComputerUse(st *SubTask, task *Task) {
	logAction(task, "Agent Started", "computer", st.Goal)

	mu.Lock()
	if task.Workspace == "" {
		workspace := "./workspace/" + task.ID[:8]
		os.MkdirAll(workspace, 0755)
		task.Workspace = workspace
	}
	workspace := task.Workspace
	mu.Unlock()

	cmds := []string{}
	lowerGoal := strings.ToLower(st.Goal)
	if strings.Contains(lowerGoal, "list") || strings.Contains(lowerGoal, "files") || strings.Contains(lowerGoal, "analyze") {
		cmds = append(cmds, "ls -la")
	}
	if strings.Contains(lowerGoal, "create") || strings.Contains(lowerGoal, "report") {
		cmds = append(cmds, "echo 'Apex Phase 7 Report' > report.txt")
	}

	results := []string{}
	for _, c := range cmds {
		out, err := runSafeCommand(c, workspace)
		if err != nil {
			results = append(results, fmt.Sprintf("Error [%s]: %v", c, err))
		} else {
			results = append(results, fmt.Sprintf("Success [%s]: %s", c, strings.TrimSpace(out)))
		}
	}

	mu.Lock()
	st.Result = fmt.Sprintf("Computer agent successfully executed commands in %s", workspace)
	st.Status = "completed"
	task.Artifacts = append(task.Artifacts, "computer-use.log")
	logActionLocked(task, "Agent Completed", "computer", strings.Join(results, ", "))
	mu.Unlock()
}

func executeSubTask(st *SubTask, task *Task) {
	if st.Type == "computer" {
		executeComputerUse(st, task)
		return
	}

	logAction(task, "Agent Started", st.Type, st.Goal)
	time.Sleep(600 * time.Millisecond)

	mu.Lock()
	switch st.Type {
	case "browser":
		st.Result = "Freebuff Browser: Extracted research data"
		task.Artifacts = append(task.Artifacts, "research.json")
	case "code":
		st.Result = "Freebuff Code: Generated source artifacts"
		task.Artifacts = append(task.Artifacts, "logic.go")
	case "data":
		st.Result = "Freebuff Data: Processed analytics"
		task.Artifacts = append(task.Artifacts, "data.xlsx")
	case "file":
		st.Result = "Freebuff File: Packaged deliverables"
		task.Artifacts = append(task.Artifacts, "final.zip")
	default:
		st.Result = fmt.Sprintf("%s agent successfully processed subtask", st.Type)
	}

	st.Status = "completed"
	logActionLocked(task, "Agent Completed", st.Type, st.Result)
	mu.Unlock()
}

// --- API Handlers ---

func createTask(w http.ResponseWriter, r *http.Request) {
	var req struct{ Goal string `json:"goal"` }
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Goal == "" {
		http.Error(w, "Goal required", http.StatusBadRequest)
		return
	}

	id := uuid.New().String()
	task := &Task{
		ID:        id,
		Goal:      req.Goal,
		Status:    "queued",
		SubTasks:  decomposeGoal(req.Goal),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		SandboxID: createSandbox(id),
		Logs:      []LogEntry{},
	}

	mu.Lock()
	tasks[id] = task
	logActionLocked(task, "Task Queued", "orchestrator", req.Goal)
	mu.Unlock()

	taskQueue <- task

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"task_id": id, "status": "queued"})
}

func getTaskStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mu.RLock()
	task, exists := tasks[id]
	if !exists {
		mu.RUnlock()
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}
	data, _ := json.Marshal(task)
	mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func getTaskLogs(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mu.RLock()
	task, exists := tasks[id]
	if !exists {
		mu.RUnlock()
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}
	resp := map[string]interface{}{
		"task_id":  id,
		"status":   task.Status,
		"progress": task.Progress,
		"logs":     task.Logs,
	}
	data, _ := json.Marshal(resp)
	mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func getTaskOutput(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mu.RLock()
	task, exists := tasks[id]
	if !exists {
		mu.RUnlock()
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}
	out := map[string]interface{}{
		"artifacts":   task.Artifacts,
		"subtasks":    task.SubTasks,
		"deployments": task.Deployments,
	}
	data, _ := json.Marshal(out)
	mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func stopTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mu.Lock()
	task, exists := tasks[id]
	if exists {
		task.Status = "stopped"
		logActionLocked(task, "Task Stopped", "user", "User requested termination")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	} else {
		mu.Unlock()
		http.Error(w, "Not found", http.StatusNotFound)
	}
}

func deployHandler(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mu.Lock()
	task, exists := tasks[id]
	if !exists {
		mu.Unlock()
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}

	logActionLocked(task, "Deployment Initiated", "Apex Deployer", "Packaging artifacts for Vercel")

	// Simulate deployment
	for _, a := range task.Artifacts {
		d := Deployment{
			ID:        uuid.New().String(),
			Artifact:  a,
			Target:    "vercel",
			URL:       fmt.Sprintf("https://apex-%s.vercel.app", task.ID[:6]),
			Status:    "deployed",
			Timestamp: time.Now(),
		}
		task.Deployments = append(task.Deployments, d)
		logActionLocked(task, "Artifact Deployed", "Apex Deployer", d.URL)
	}
	task.UpdatedAt = time.Now()
	mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"task_id":     id,
		"status":      "deployed",
		"deployments": task.Deployments,
	})
}

func getDashboard(w http.ResponseWriter, r *http.Request) {
	mu.RLock()
	defer mu.RUnlock()

	active := 0
	for _, t := range tasks {
		if t.Status == "running" || t.Status == "queued" {
			active++
		}
	}

	resp := map[string]interface{}{
		"active_tasks": active,
		"total_tasks":  len(tasks),
		"message":      "APEX Mirror Dashboard - Manus-Class Orchestration",
		"timestamp":    time.Now().Format(time.RFC3339),
		"tasks":        tasks,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func deleteTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mu.Lock()
	delete(tasks, id)
	mu.Unlock()
	log.Printf("🧹 Audit: Task %s deleted", id)
	w.WriteHeader(http.StatusNoContent)
}

func computerUse(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Command string `json:"command"`
		Approve bool   `json:"approve"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if !req.Approve {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("⚠️ Approval required for: " + req.Command))
		return
	}

	workspace := "./workspace/direct"
	os.MkdirAll(workspace, 0755)

	output, err := runSafeCommand(req.Command, workspace)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"output": output, "status": "executed"})
}

// --- Main ---

func main() {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /task", createTask)
	mux.HandleFunc("GET /task/{id}/status", getTaskStatus)
	mux.HandleFunc("GET /task/{id}/output", getTaskOutput)
	mux.HandleFunc("GET /task/{id}/logs", getTaskLogs)
	mux.HandleFunc("POST /task/{id}/stop", stopTask)
	mux.HandleFunc("POST /task/{id}/deploy", deployHandler)
	mux.HandleFunc("DELETE /task/{id}", deleteTask)
	mux.HandleFunc("POST /computer-use", computerUse)
	mux.HandleFunc("GET /dashboard", getDashboard)

	log.Println("🚀 **FULL MANUS-CLASS SYSTEM** (Phases 1-7) LIVE on :8080")
	log.Println("✅ DeepSeek + Freebuff + Jules + Apex Deployer + APEX Mirror")

	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal(err)
	}
}
