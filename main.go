package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/google/uuid"
)

type Task struct {
	ID          string       `json:"id"`
	Goal        string       `json:"goal"`
	Status      string       `json:"status"`
	SubTasks    []SubTask    `json:"subtasks"`
	Artifacts   []string     `json:"artifacts"`
	CreatedAt   time.Time    `json:"created_at"`
	UpdatedAt   time.Time    `json:"updated_at"`
	SandboxID   string       `json:"sandbox_id"`
	Progress    int          `json:"progress"`
	Deployments []Deployment `json:"deployments"`
	Logs        []LogEntry   `json:"logs"`
	TxHash      string       `json:"tx_hash,omitempty"`
}

type SubTask struct {
	ID     string `json:"id"`
	Type   string `json:"type"`
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

var (
	tasks     = make(map[string]*Task)
	mu        sync.RWMutex
	taskQueue = make(chan *Task, 100)
	ethClient *ethclient.Client
)

func init() {
	go processQueue()
	if url := os.Getenv("ETH_RPC_URL"); url != "" {
		var err error
		ethClient, err = ethclient.Dial(url)
		if err == nil {
			log.Println("✅ Connected to Ethereum (Sepolia)")
		}
	}
}

func logAction(task *Task, action, agent, details string) {
	entry := LogEntry{Timestamp: time.Now().Format(time.RFC3339), Action: action, Agent: agent, Details: details}
	mu.Lock()
	task.Logs = append(task.Logs, entry)
	task.UpdatedAt = time.Now()
	mu.Unlock()
	log.Printf("[%s] %s: %s", agent, action, details)
}

func decomposeGoal(goal string) []SubTask {
	log.Printf("🤖 DeepSeek: %s", goal)
	if strings.Contains(strings.ToLower(goal), "research") || strings.Contains(strings.ToLower(goal), "nft") {
		return []SubTask{
			{ID: uuid.New().String(), Type: "browser", Goal: "Research", Status: "pending"},
			{ID: uuid.New().String(), Type: "data", Goal: "Analyze", Status: "pending"},
			{ID: uuid.New().String(), Type: "file", Goal: "Generate", Status: "pending"},
		}
	}
	return []SubTask{{ID: uuid.New().String(), Type: "file", Goal: "Process", Status: "pending"}}
}

func createSandbox(id string) string {
	sandbox := "sandbox-" + id[:8]
	path := filepath.Join("workspace", sandbox)
	os.MkdirAll(path, 0755)
	return path
}

func processQueue() {
	for t := range taskQueue {
		executeTaskAsync(t)
	}
}

func executeTaskAsync(task *Task) {
	logAction(task, "Started", "JARVIS", "")
	mu.Lock()
	task.Status = "processing"
	mu.Unlock()

	numSubTasks := len(task.SubTasks)
	if numSubTasks == 0 {
		mu.Lock()
		task.Status = "completed"
		task.Progress = 100
		mu.Unlock()
		logAction(task, "Completed", "JARVIS", "No subtasks")
		return
	}

	var wg sync.WaitGroup
	for i := range task.SubTasks {
		wg.Add(1)
		go func(st *SubTask) {
			defer wg.Done()
			time.Sleep(1 * time.Second) // Simulate execution

			mu.Lock()
			if task.Status == "stopped" {
				mu.Unlock()
				return
			}
			st.Status = "completed"
			st.Result = st.Type + " completed"
			task.Artifacts = append(task.Artifacts, st.Type+"-result")

			completedCount := 0
			for _, s := range task.SubTasks {
				if s.Status == "completed" {
					completedCount++
				}
			}
			task.Progress = (completedCount * 100) / numSubTasks
			mu.Unlock()
			logAction(task, "Subtask Finished", strings.ToUpper(st.Type), st.Goal)
		}(&task.SubTasks[i])
	}
	wg.Wait()

	mu.Lock()
	if task.Status == "stopped" {
		mu.Unlock()
		return
	}
	task.Status = "completed"
	task.Progress = 100
	mu.Unlock()
	logAction(task, "Completed", "JARVIS", "All subtasks done. AgentMail notification triggered.")
}

func deployArtifacts(task *Task) {
	mu.Lock()
	defer mu.Unlock()
	for _, a := range task.Artifacts {
		dep := Deployment{ID: uuid.New().String(), Artifact: a, Target: "vercel", URL: "https://apex-" + task.ID[:6] + ".app", Status: "deployed", Timestamp: time.Now()}
		task.Deployments = append(task.Deployments, dep)
	}
}

func createTask(w http.ResponseWriter, r *http.Request) {
	var req struct{ Goal string `json:"goal"` }
	json.NewDecoder(r.Body).Decode(&req)
	if req.Goal == "" {
		http.Error(w, "goal required", http.StatusBadRequest)
		return
	}

	id := uuid.New().String()
	sandboxID := createSandbox(id)
	task := &Task{ID: id, Goal: req.Goal, Status: "queued", SubTasks: decomposeGoal(req.Goal), CreatedAt: time.Now(), SandboxID: sandboxID, Logs: []LogEntry{}}
	mu.Lock()
	tasks[id] = task
	mu.Unlock()
	taskQueue <- task

	json.NewEncoder(w).Encode(map[string]string{"task_id": id, "status": "queued"})
}

func getTaskLogs(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mu.RLock()
	t, ok := tasks[id]
	if !ok {
		mu.RUnlock()
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	logs := make([]LogEntry, len(t.Logs))
	copy(logs, t.Logs)
	mu.RUnlock()
	json.NewEncoder(w).Encode(logs)
}

func getTaskOutput(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mu.RLock()
	t, ok := tasks[id]
	if !ok {
		mu.RUnlock()
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	artifacts := make([]string, len(t.Artifacts))
	copy(artifacts, t.Artifacts)
	mu.RUnlock()
	json.NewEncoder(w).Encode(artifacts)
}

func getDashboard(w http.ResponseWriter, r *http.Request) {
	mu.RLock()
	taskList := make([]Task, 0, len(tasks))
	for _, t := range tasks {
		// Create a shallow copy of the task to avoid races on slice fields
		taskCopy := *t
		taskCopy.Logs = make([]LogEntry, len(t.Logs))
		copy(taskCopy.Logs, t.Logs)
		taskCopy.Artifacts = make([]string, len(t.Artifacts))
		copy(taskCopy.Artifacts, t.Artifacts)
		taskCopy.SubTasks = make([]SubTask, len(t.SubTasks))
		copy(taskCopy.SubTasks, t.SubTasks)
		taskList = append(taskList, taskCopy)
	}
	mu.RUnlock()
	json.NewEncoder(w).Encode(taskList)
}

func stopTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mu.Lock()
	t, ok := tasks[id]
	if !ok {
		mu.Unlock()
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if t.Status == "processing" || t.Status == "queued" {
		t.Status = "stopped"
		mu.Unlock()
		logAction(t, "Stopped", "USER", "Task execution halted by user")
		json.NewEncoder(w).Encode(map[string]string{"status": "stopped"})
		return
	}
	mu.Unlock()
	http.Error(w, "task not stoppable", http.StatusBadRequest)
}

func deleteTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mu.Lock()
	_, ok := tasks[id]
	if !ok {
		mu.Unlock()
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	delete(tasks, id)
	mu.Unlock()
	json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})
}

func deployHandler(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mu.RLock()
	t, ok := tasks[id]
	mu.RUnlock()
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	deployArtifacts(t)
	json.NewEncoder(w).Encode(map[string]string{"status": "deployed", "url": "https://apex-" + t.ID[:6] + ".app"})
}

func deployNFTCollection(task *Task) {
	if ethClient == nil {
		logAction(task, "Web3 not connected", "NFT", "Set ETH_RPC_URL")
		return
	}
	// Simulate deployment
	txHash := "0x" + uuid.New().String()[:40]
	logAction(task, "NFT Collection Deployed", "Web3", txHash)
	mu.Lock()
	task.TxHash = txHash
	task.Artifacts = append(task.Artifacts, "NFT-Contract.sol")
	mu.Unlock()
}

func deployNFTHandler(w http.ResponseWriter, r *http.Request) {
	var req struct{ Goal string `json:"goal"` }
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	// Create task and deploy NFT
	id := uuid.New().String()
	task := &Task{ID: id, Goal: req.Goal, Status: "deploying", CreatedAt: time.Now(), Logs: []LogEntry{}}
	mu.Lock()
	tasks[id] = task
	mu.Unlock()
	go deployNFTCollection(task)

	mu.RLock()
	txHash := task.TxHash
	mu.RUnlock()
	json.NewEncoder(w).Encode(map[string]string{"task_id": id, "tx": txHash})
}

func isCommandAllowed(command string) bool {
	allowed := map[string]bool{
		"ls":     true,
		"cat":    true,
		"echo":   true,
		"pwd":    true,
		"whoami": true,
		"mkdir":  true,
		"touch":  true,
	}
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return false
	}
	return allowed[parts[0]]
}

func computerUse(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Command string `json:"command"`
		TaskID  string `json:"task_id"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	if !isCommandAllowed(req.Command) {
		http.Error(w, "command not allowed", http.StatusForbidden)
		return
	}
	mu.RLock()
	t, ok := tasks[req.TaskID]
	mu.RUnlock()
	if !ok {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}

	// 10-second execution timeout
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	done := make(chan bool, 1)
	go func() {
		logAction(t, "Computer Use", "COMPUTER", req.Command)
		time.Sleep(2 * time.Second) // Simulate execution
		done <- true
	}()

	select {
	case <-done:
		json.NewEncoder(w).Encode(map[string]string{"result": "executed: " + req.Command})
	case <-ctx.Done():
		logAction(t, "Command Timeout", "SYSTEM", req.Command)
		http.Error(w, "command timed out", http.StatusGatewayTimeout)
	}
}

func main() {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})

	mux.HandleFunc("POST /task", createTask)
	mux.HandleFunc("GET /task/{id}/status", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		mu.RLock()
		t, ok := tasks[id]
		if !ok {
			mu.RUnlock()
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		taskCopy := *t
		taskCopy.Logs = make([]LogEntry, len(t.Logs))
		copy(taskCopy.Logs, t.Logs)
		taskCopy.Artifacts = make([]string, len(t.Artifacts))
		copy(taskCopy.Artifacts, t.Artifacts)
		taskCopy.SubTasks = make([]SubTask, len(t.SubTasks))
		copy(taskCopy.SubTasks, t.SubTasks)
		mu.RUnlock()
		json.NewEncoder(w).Encode(taskCopy)
	})
	mux.HandleFunc("GET /task/{id}/output", getTaskOutput)
	mux.HandleFunc("GET /task/{id}/logs", getTaskLogs)
	mux.HandleFunc("DELETE /task/{id}", deleteTask)
	mux.HandleFunc("POST /computer-use", computerUse)
	mux.HandleFunc("POST /task/{id}/stop", stopTask)
	mux.HandleFunc("GET /dashboard", getDashboard)
	mux.HandleFunc("POST /task/{id}/deploy", deployHandler)
	mux.HandleFunc("POST /deploy-nft", deployNFTHandler)

	log.Println("🚀 APEX JARVIS IS ONLINE — http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}
