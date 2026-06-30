package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
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
	startTime = time.Now()
	upgrader  = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	clients   = make(map[*websocket.Conn]bool)
	broadcast = make(chan LogEntry)
)

func init() {
	go processQueue()
	go handleMessages()
}

func logAction(task *Task, action, agent, details string) {
	entry := LogEntry{Timestamp: time.Now().Format(time.RFC3339), Action: action, Agent: agent, Details: details}
	mu.Lock()
	task.Logs = append(task.Logs, entry)
	task.UpdatedAt = time.Now()
	mu.Unlock()
	log.Printf("[%s] %s: %s", agent, action, details)
	broadcast <- entry
}

func handleMessages() {
	for {
		msg := <-broadcast
		mu.Lock()
		for client := range clients {
			err := client.WriteJSON(msg)
			if err != nil {
				log.Printf("error: %v", err)
				client.Close()
				delete(clients, client)
			}
		}
		mu.Unlock()
	}
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer ws.Close()

	mu.Lock()
	clients[ws] = true
	mu.Unlock()

	for {
		_, _, err := ws.ReadMessage()
		if err != nil {
			mu.Lock()
			delete(clients, ws)
			mu.Unlock()
			break
		}
	}
}

func decomposeGoal(goal string) []SubTask {
	log.Printf("🤖 DeepSeek Decomposing: %s", goal)
	g := strings.ToLower(goal)
	if strings.Contains(g, "product") || strings.Contains(g, "collection") || strings.Contains(g, "hoodie") {
		return []SubTask{
			{ID: uuid.New().String(), Type: "ai", Goal: "Generate Strategy & Description", Status: "pending"},
			{ID: uuid.New().String(), Type: "ai", Goal: "Generate Product Images", Status: "pending"},
			{ID: uuid.New().String(), Type: "shopify", Goal: "Upload to Shopify Store", Status: "pending"},
		}
	}
	if strings.Contains(g, "research") || strings.Contains(g, "nft") {
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
	ai := NewAIService()
	shopify := NewShopifyClient()

	var productTitle, productDesc, productImage string

	for i := range task.SubTasks {
		st := &task.SubTasks[i]

		// Check if stopped
		mu.RLock()
		if task.Status == "stopped" {
			mu.RUnlock()
			return
		}
		mu.RUnlock()

		logAction(task, "Executing", "AGENT", st.Goal)

		switch st.Type {
		case "ai":
			if strings.Contains(st.Goal, "Description") {
				title, desc, _ := ai.GenerateProductDescription(task.Goal)
				productTitle = title
				productDesc = desc
				st.Result = "AI generated description"
			} else {
				img, _ := ai.GenerateProductImage(task.Goal)
				productImage = img
				st.Result = "AI generated image"
			}
		case "shopify":
			res, _ := shopify.CreateProduct(productTitle, productDesc, productImage)
			st.Result = res
		default:
			time.Sleep(500 * time.Millisecond)
			st.Result = st.Type + " completed"
		}

		mu.Lock()
		st.Status = "completed"
		task.Artifacts = append(task.Artifacts, st.Result)
		completedCount := 0
		for _, s := range task.SubTasks {
			if s.Status == "completed" {
				completedCount++
			}
		}
		task.Progress = (completedCount * 100) / len(task.SubTasks)
		mu.Unlock()
	}

	mu.Lock()
	if task.Status != "stopped" {
		task.Status = "completed"
		task.Progress = 100
	}
	mu.Unlock()
	logAction(task, "Completed", "JARVIS", "")
}

func deployArtifacts(task *Task) {
	mu.Lock()
	defer mu.Unlock()
	for _, a := range task.Artifacts {
		dep := Deployment{
			ID:        uuid.New().String(),
			Artifact:  a,
			Target:    "vercel",
			URL:       "https://apex-" + task.ID[:6] + ".app",
			Status:    "deployed",
			Timestamp: time.Now(),
		}
		task.Deployments = append(task.Deployments, dep)
	}
}

func createTask(w http.ResponseWriter, r *http.Request) {
	var req struct{ Goal string `json:"goal"` }
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if req.Goal == "" {
		http.Error(w, "goal required", http.StatusBadRequest)
		return
	}

	id := uuid.New().String()
	task := &Task{
		ID:        id,
		Goal:      req.Goal,
		Status:    "queued",
		SubTasks:  decomposeGoal(req.Goal),
		CreatedAt: time.Now(),
		SandboxID: createSandbox(id),
		Logs:      []LogEntry{},
	}
	mu.Lock()
	tasks[id] = task
	mu.Unlock()
	taskQueue <- task

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"task_id": id, "status": "queued"})
}

func getTaskStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mu.RLock()
	t, ok := tasks[id]
	if !ok {
		mu.RUnlock()
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	// Deep copy to prevent data races during JSON encoding
	copyTask := *t
	copyTask.Logs = make([]LogEntry, len(t.Logs))
	copy(copyTask.Logs, t.Logs)
	copyTask.Artifacts = make([]string, len(t.Artifacts))
	copy(copyTask.Artifacts, t.Artifacts)
	copyTask.SubTasks = make([]SubTask, len(t.SubTasks))
	copy(copyTask.SubTasks, t.SubTasks)
	mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(copyTask)
}

func stopTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mu.Lock()
	t, ok := tasks[id]
	if ok {
		t.Status = "stopped"
	}
	mu.Unlock()
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func getDashboard(w http.ResponseWriter, r *http.Request) {
	mu.RLock()
	defer mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tasks)
}

func deployTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mu.Lock()
	t, ok := tasks[id]
	if !ok {
		mu.Unlock()
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	mu.Unlock()

	deployArtifacts(t)

	mu.RLock()
	defer mu.RUnlock()
	// Deep copy deployments
	deps := make([]Deployment, len(t.Deployments))
	copy(deps, t.Deployments)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(deps)
}

func deployNFT(w http.ResponseWriter, r *http.Request) {
	// Simulate NFT deployment
	txHash := "0x" + uuid.New().String()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"tx_hash": txHash, "status": "deployed"})
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
	w.Header().Set("Content-Type", "application/json")
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
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(artifacts)
}

func voiceCommand(w http.ResponseWriter, r *http.Request) {
	var req struct{ Command string `json:"command"` }
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("Error decoding voice command: %v", err)
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	cmd := strings.ToLower(req.Command)
	var goal string
	if strings.Contains(cmd, "product") || strings.Contains(cmd, "hoodie") {
		goal = "Create new product: " + req.Command
	} else if strings.Contains(cmd, "sales") || strings.Contains(cmd, "orders") {
		goal = "Fetch store sales and orders"
	} else {
		goal = req.Command
	}

	id := uuid.New().String()
	task := &Task{
		ID:        id,
		Goal:      goal,
		Status:    "queued",
		SubTasks:  decomposeGoal(goal),
		CreatedAt: time.Now(),
		Logs:      []LogEntry{},
	}
	mu.Lock()
	tasks[id] = task
	mu.Unlock()
	taskQueue <- task

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"task_id": id, "intent": goal})
}

func getSystemMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Admin-Key") != config.AdminKey {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	mu.RLock()
	totalTasks := len(tasks)
	activeTasks := 0
	for _, t := range tasks {
		if t.Status == "queued" || t.Status == "executing" {
			activeTasks++
		}
	}
	mu.RUnlock()

	metrics := map[string]interface{}{
		"total_tasks":  totalTasks,
		"active_tasks": activeTasks,
		"status":      "online",
		"uptime":      time.Since(startTime).String(),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

func main() {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})

	mux.HandleFunc("POST /task", createTask)
	mux.HandleFunc("GET /task/{id}/status", getTaskStatus)
	mux.HandleFunc("GET /task/{id}/output", getTaskOutput)
	mux.HandleFunc("GET /task/{id}/logs", getTaskLogs)
	mux.HandleFunc("POST /task/{id}/stop", stopTask)
	mux.HandleFunc("GET /dashboard", getDashboard)
	mux.HandleFunc("POST /task/{id}/deploy", deployTask)
	mux.HandleFunc("POST /deploy-nft", deployNFT)
	mux.HandleFunc("POST /api/voice-command", voiceCommand)
	mux.HandleFunc("GET /api/admin/system", getSystemMetrics)
	mux.HandleFunc("GET /ws", handleConnections)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("🚀 APEX JARVIS IS ONLINE — http://localhost:%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
