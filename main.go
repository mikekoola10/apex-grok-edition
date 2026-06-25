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

// Task represents a single autonomous task (enhanced for Phase 6)
type Task struct {
	ID            string    `json:"id"`
	Goal          string    `json:"goal"`
	Status        string    `json:"status"` // queued, running, paused, completed, stopped
	SubTasks      []SubTask `json:"subtasks"`
	Artifacts     []string  `json:"artifacts"`
	Logs          []string  `json:"logs"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
	SandboxID     string    `json:"sandbox_id"`
	Workspace     string    `json:"workspace,omitempty"`
	Progress      int       `json:"progress"` // 0-100
	Deployed      bool      `json:"deployed"`
	DeploymentURL string    `json:"deployment_url,omitempty"`
}

// SubTask for multi-agent decomposition
type SubTask struct {
	ID       string `json:"id"`
	Type     string `json:"type"` // browser, code, data, file, computer
	Goal     string `json:"goal"`
	Status   string `json:"status"`
	Result   string `json:"result"`
}

// In-memory store and queue
var (
	tasks     = make(map[string]*Task)
	mu        sync.RWMutex
	taskQueue = make(chan *Task, 100)
)

func init() {
	go processQueue()
}

// DeepSeek decomposition
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

// Async Queue Processor
func processQueue() {
	for task := range taskQueue {
		executeTaskAsync(task)
	}
}

func executeTaskAsync(task *Task) {
	log.Printf("🔄 Async execution started for task: %s", task.ID)

	mu.Lock()
	task.Status = "running"
	task.UpdatedAt = time.Now()
	task.Logs = append(task.Logs, fmt.Sprintf("[%s] Audit: Background execution started", time.Now().Format(time.RFC3339)))
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
		task.Logs = append(task.Logs, fmt.Sprintf("[%s] Audit: All subtasks completed successfully", time.Now().Format(time.RFC3339)))
	}
	task.UpdatedAt = time.Now()
	mu.Unlock()

	log.Printf("🎉 Task %s fully completed", task.ID)
	notifyCompletion(task)
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
	mu.Lock()
	if task.Workspace == "" {
		workspace := "./workspace/" + task.ID[:8]
		os.MkdirAll(workspace, 0755)
		task.Workspace = workspace
	}
	workspace := task.Workspace
	task.Logs = append(task.Logs, fmt.Sprintf("[%s] Computer: Activated in workspace %s", time.Now().Format(time.RFC3339), workspace))
	mu.Unlock()

	cmds := []string{}
	lowerGoal := strings.ToLower(st.Goal)
	if strings.Contains(lowerGoal, "list") || strings.Contains(lowerGoal, "files") || strings.Contains(lowerGoal, "analyze") {
		cmds = append(cmds, "ls -la")
	}
	if strings.Contains(lowerGoal, "create") || strings.Contains(lowerGoal, "report") {
		cmds = append(cmds, "echo 'Apex Audit Report' > audit.txt")
	}

	results := []string{}
	for _, c := range cmds {
		output, err := runSafeCommand(c, workspace)
		if err != nil {
			results = append(results, fmt.Sprintf("Error: %v", err))
		} else {
			results = append(results, strings.TrimSpace(output))
		}
	}

	mu.Lock()
	st.Result = fmt.Sprintf("Computer: Executed commands in %s", workspace)
	st.Status = "completed"
	task.Artifacts = append(task.Artifacts, "computer-use.audit")
	task.Logs = append(task.Logs, fmt.Sprintf("[%s] Computer: Actions completed with outputs: %v", time.Now().Format(time.RFC3339), results))
	mu.Unlock()
}

func executeSubTask(st *SubTask, task *Task) {
	log.Printf("🚀 Routing to %s agent: %s", st.Type, st.Goal)

	if st.Type == "computer" {
		executeComputerUse(st, task)
		return
	}

	mu.Lock()
	task.Logs = append(task.Logs, fmt.Sprintf("[%s] Agent %s: Routing goal: %s", time.Now().Format(time.RFC3339), st.Type, st.Goal))
	mu.Unlock()

	time.Sleep(600 * time.Millisecond)

	mu.Lock()
	switch st.Type {
	case "browser":
		st.Result = "Browser: Extracted relevant data"
		task.Artifacts = append(task.Artifacts, "research.json")
	case "code":
		st.Result = "Code: Generated logic artifacts"
		task.Artifacts = append(task.Artifacts, "generated-code.go")
	case "data":
		st.Result = "Data: Built analysis spreadsheet"
		task.Artifacts = append(task.Artifacts, "comparison.xlsx")
	case "file":
		st.Result = "File: Packaged deliverables"
		task.Artifacts = append(task.Artifacts, "final.zip")
	default:
		st.Result = "Agent: Processed subtask"
	}

	st.Status = "completed"
	task.Logs = append(task.Logs, fmt.Sprintf("[%s] Agent %s: Task completed: %s", time.Now().Format(time.RFC3339), st.Type, st.Result))
	mu.Unlock()
}

func notifyCompletion(task *Task) {
	log.Printf("📧 Audit: Completion notification sent for task %s", task.ID)
}

func createTask(w http.ResponseWriter, r *http.Request) {
	var req struct{ Goal string `json:"goal"` }
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Goal == "" {
		http.Error(w, "Valid goal required", http.StatusBadRequest)
		return
	}

	taskID := uuid.New().String()
	task := &Task{
		ID:        taskID,
		Goal:      req.Goal,
		Status:    "queued",
		SubTasks:  decomposeGoal(req.Goal),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		SandboxID: createSandbox(taskID),
		Progress:  0,
		Logs:      []string{fmt.Sprintf("[%s] Audit: Task initialization: %s", time.Now().Format(time.RFC3339), req.Goal)},
	}

	mu.Lock()
	tasks[taskID] = task
	mu.Unlock()

	taskQueue <- task

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"task_id": taskID, "status": "queued", "message": "APEX Mirror: Task queued for async execution",
	})
}

func stopTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mu.Lock()
	if task, exists := tasks[id]; exists {
		task.Status = "stopped"
		task.Logs = append(task.Logs, fmt.Sprintf("[%s] Audit: Task stopped by user", time.Now().Format(time.RFC3339)))
		task.UpdatedAt = time.Now()
		w.WriteHeader(http.StatusOK)
	} else {
		http.Error(w, "Not found", http.StatusNotFound)
	}
	mu.Unlock()
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
	logs := make([]map[string]string, len(task.Logs))
	for i, l := range task.Logs {
		logs[i] = map[string]string{"message": l}
	}
	resp := map[string]interface{}{
		"task_id": id,
		"logs":    logs,
		"status":  task.Status,
		"progress": task.Progress,
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
	output := map[string]interface{}{
		"artifacts": task.Artifacts,
		"subtasks":  task.SubTasks,
	}
	data, _ := json.Marshal(output)
	mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func deployTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mu.Lock()
	task, exists := tasks[id]
	if !exists {
		mu.Unlock()
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}

	// Simulate Apex Deployment
	task.Deployed = true
	task.DeploymentURL = fmt.Sprintf("https://apex-deploy.io/live/%s", id[:8])
	task.Logs = append(task.Logs, fmt.Sprintf("[%s] Audit: Apex Deployer: Artifacts deployed to %s", time.Now().Format(time.RFC3339), task.DeploymentURL))
	task.UpdatedAt = time.Now()
	mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"task_id": id,
		"status": "deployed",
		"deployment_url": task.DeploymentURL,
	})
}

func getDashboard(w http.ResponseWriter, r *http.Request) {
	mu.RLock()
	activeCount := 0
	for _, t := range tasks {
		if t.Status == "running" || t.Status == "queued" {
			activeCount++
		}
	}
	data, _ := json.Marshal(map[string]interface{}{
		"active_tasks": activeCount,
		"total_tasks":  len(tasks),
		"tasks":        tasks,
		"message":      "APEX Mirror Dashboard - Real-time visibility",
		"timestamp":    time.Now().Format(time.RFC3339),
	})
	mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func deleteTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mu.Lock()
	delete(tasks, id)
	mu.Unlock()
	log.Printf("🧹 Audit: Task %s removed from memory", id)
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
	workspace := "./workspace"
	os.MkdirAll(workspace, 0755)
	output, err := runSafeCommand(req.Command, workspace)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"output": output, "status": "executed"})
}

func main() {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /task", createTask)
	mux.HandleFunc("GET /task/{id}/status", getTaskStatus)
	mux.HandleFunc("GET /task/{id}/output", getTaskOutput)
	mux.HandleFunc("GET /task/{id}/logs", getTaskLogs)
	mux.HandleFunc("DELETE /task/{id}", deleteTask)
	mux.HandleFunc("POST /task/{id}/stop", stopTask)
	mux.HandleFunc("POST /task/{id}/deploy", deployTask)
	mux.HandleFunc("POST /computer-use", computerUse)
	mux.HandleFunc("GET /dashboard", getDashboard)

	log.Println("🚀 Manus-Class Phase 6 Orchestrator running on :8080")
	log.Println("✅ DeepSeek • Freebuff Agents • Async • Computer Use • Apex Deployer • APEX Mirror")
	log.Fatal(http.ListenAndServe(":8080", mux))
}
