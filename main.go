package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Task represents a single autonomous task
type Task struct {
	ID          string    `json:"id"`
	Goal        string    `json:"goal"`
	Status      string    `json:"status"`
	SubTasks    []SubTask `json:"subtasks"`
	Artifacts   []string  `json:"artifacts"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	SandboxID   string    `json:"sandbox_id"`
}

// SubTask for multi-agent decomposition
type SubTask struct {
	ID       string `json:"id"`
	Type     string `json:"type"` // browser, code, data, file
	Goal     string `json:"goal"`
	Status   string `json:"status"`
	Result   string `json:"result"`
}

// In-memory store (replace with DB in prod)
var (
	tasks = make(map[string]*Task)
	mu    sync.RWMutex
)

// DeepSeek placeholder for goal decomposition
func decomposeGoal(goal string) []SubTask {
	// TODO: Replace with real DeepSeek API call in Phase 2
	log.Printf("Decomposing goal with DeepSeek: %s", goal)
	return []SubTask{
		{ID: uuid.New().String(), Type: "browser", Goal: "Research relevant data", Status: "pending"},
		{ID: uuid.New().String(), Type: "data", Goal: "Analyze and compare", Status: "pending"},
		{ID: uuid.New().String(), Type: "file", Goal: "Generate spreadsheet", Status: "pending"},
	}
}

// Simulate sandbox spin-up (Docker ready)
func createSandbox(taskID string) string {
	sandboxID := "sandbox-" + taskID[:8]
	log.Printf("✅ Spun up isolated Docker sandbox: %s for task %s", sandboxID, taskID)
	return sandboxID
}

// Simulate agent execution
func executeSubTask(st *SubTask, task *Task) {
	time.Sleep(2 * time.Second) // simulate work
	st.Status = "completed"
	st.Result = fmt.Sprintf("%s completed successfully", st.Type)
	log.Printf("Agent %s completed: %s", st.Type, st.Goal)

	mu.Lock()
	task.Artifacts = append(task.Artifacts, st.Type+".artifact")
	task.UpdatedAt = time.Now()
	mu.Unlock()
}

// POST /task - Create new task
func createTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Goal string `json:"goal"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	taskID := uuid.New().String()
	sandboxID := createSandbox(taskID)

	task := &Task{
		ID:        taskID,
		Goal:      req.Goal,
		Status:    "running",
		SubTasks:  decomposeGoal(req.Goal),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		SandboxID: sandboxID,
	}

	mu.Lock()
	tasks[taskID] = task
	mu.Unlock()

	// Async parallel execution (Jules-ready)
	go func() {
		var wg sync.WaitGroup
		for i := range task.SubTasks {
			wg.Add(1)
			go func(st *SubTask) {
				defer wg.Done()
				executeSubTask(st, task)
			}(&task.SubTasks[i])
		}
		wg.Wait()

		mu.Lock()
		task.Status = "completed"
		task.UpdatedAt = time.Now()
		mu.Unlock()
		log.Printf("✅ Task %s completed. Artifacts: %v", taskID, task.Artifacts)
	}()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"task_id": taskID, "status": "running", "sandbox": sandboxID})
}

// GET /task/{id}/status
func getTaskStatus(w http.ResponseWriter, r *http.Request, id string) {
	mu.RLock()
	task, exists := tasks[id]
	if !exists {
		mu.RUnlock()
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}

	// Marshalling while holding the lock to avoid data race
	data, err := json.Marshal(task)
	mu.RUnlock()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// GET /task/{id}/output
func getTaskOutput(w http.ResponseWriter, r *http.Request, id string) {
	mu.RLock()
	task, exists := tasks[id]
	if !exists {
		mu.RUnlock()
		http.Error(w, "Task not found", http.StatusNotFound)
		return
	}

	// Copy data while holding the lock to avoid data race
	output := struct {
		Artifacts []string  `json:"artifacts"`
		SubTasks  []SubTask `json:"subtasks"`
	}{
		Artifacts: append([]string(nil), task.Artifacts...),
		SubTasks:  append([]SubTask(nil), task.SubTasks...),
	}
	mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(output)
}

// DELETE /task/{id}
func deleteTask(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	mu.Lock()
	delete(tasks, id)
	mu.Unlock()
	log.Printf("🧹 Cleaned up sandbox for task %s", id)
	w.WriteHeader(http.StatusNoContent)
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /task", createTask)
	mux.HandleFunc("GET /task/{id}/status", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		getTaskStatus(w, r, id)
	})
	mux.HandleFunc("GET /task/{id}/output", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		getTaskOutput(w, r, id)
	})
	mux.HandleFunc("DELETE /task/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		deleteTask(w, r, id)
	})

	log.Println("🚀 Manus-Class Orchestrator (Phase 1) running on :8080")
	log.Println("Ready for DeepSeek + Freebuff integration in Phase 2")
	log.Fatal(http.ListenAndServe(":8080", mux))
}
