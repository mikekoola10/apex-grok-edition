package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

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
)

func init() { go processQueue() }

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

func createSandbox(id string) string { return "sandbox-" + id[:8] }

func processQueue() {
	for t := range taskQueue {
		executeTaskAsync(t)
	}
}

func executeTaskAsync(task *Task) {
	logAction(task, "Started", "JARVIS", "")
	var wg sync.WaitGroup
	for i := range task.SubTasks {
		wg.Add(1)
		go func(st *SubTask) {
			defer wg.Done()
			time.Sleep(700 * time.Millisecond)
			st.Status = "completed"
			st.Result = st.Type + " completed"
			task.Artifacts = append(task.Artifacts, st.Type+"-result")
		}(&task.SubTasks[i])
	}
	wg.Wait()
	mu.Lock()
	task.Status = "completed"
	task.Progress = 100
	mu.Unlock()
	logAction(task, "Completed", "JARVIS", "")
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
	task := &Task{ID: id, Goal: req.Goal, Status: "queued", SubTasks: decomposeGoal(req.Goal), CreatedAt: time.Now(), SandboxID: createSandbox(id), Logs: []LogEntry{}}
	mu.Lock()
	tasks[id] = task
	mu.Unlock()
	taskQueue <- task

	json.NewEncoder(w).Encode(map[string]string{"task_id": id, "status": "queued"})
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
		mu.RUnlock()
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(t)
	})

	log.Println("🚀 APEX JARVIS IS ONLINE — http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}
