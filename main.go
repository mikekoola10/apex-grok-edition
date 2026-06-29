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

	clients   = make(map[*websocket.Conn]bool)
	clientsMu sync.Mutex
	upgrader  = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
)

func init() { go processQueue() }

func logAction(task *Task, action, agent, details string) {
	entry := LogEntry{Timestamp: time.Now().Format(time.RFC3339), Action: action, Agent: agent, Details: details}
	mu.Lock()
	if task != nil {
		task.Logs = append(task.Logs, entry)
		task.UpdatedAt = time.Now()
	}
	mu.Unlock()
	log.Printf("[%s] %s: %s", agent, action, details)
	broadcastLog(entry)
}

func broadcastLog(entry LogEntry) {
	clientsMu.Lock()
	defer clientsMu.Unlock()
	for client := range clients {
		err := client.WriteJSON(entry)
		if err != nil {
			log.Printf("error: %v", err)
			client.Close()
			delete(clients, client)
		}
	}
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade failed: %v", err)
		return
	}
	defer ws.Close()

	clientsMu.Lock()
	clients[ws] = true
	clientsMu.Unlock()

	log.Printf("New WebSocket connection")

	for {
		var msg interface{}
		err := ws.ReadJSON(&msg)
		if err != nil {
			clientsMu.Lock()
			delete(clients, ws)
			clientsMu.Unlock()
			break
		}
	}
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

			// Check if stopped before starting
			mu.RLock()
			if task.Status == "stopped" {
				mu.RUnlock()
				return
			}
			mu.RUnlock()

			time.Sleep(700 * time.Millisecond)

			mu.Lock()
			defer mu.Unlock()
			if task.Status == "stopped" {
				return
			}
			st.Status = "completed"
			st.Result = st.Type + " completed"
			task.Artifacts = append(task.Artifacts, st.Type+"-result")

			// Update progress
			completedCount := 0
			for _, s := range task.SubTasks {
				if s.Status == "completed" {
					completedCount++
				}
			}
			task.Progress = (completedCount * 100) / len(task.SubTasks)
		}(&task.SubTasks[i])
	}
	wg.Wait()

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

	id := strings.ReplaceAll(uuid.New().String(), "-", "")
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
	// Simulate NFT deployment with Solidity contract generation
	contractSource := `// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "@openzeppelin/contracts/token/ERC721/ERC721.sol";

contract ApexNFT is ERC721 {
    constructor() ERC721("ApexGrok", "APX") {}
}`
	err := os.WriteFile("NFT-Contract.sol", []byte(contractSource), 0644)
	if err != nil {
		http.Error(w, "failed to create contract file", http.StatusInternalServerError)
		return
	}

	txHash := "0x" + strings.ReplaceAll(uuid.New().String(), "-", "")
	logAction(nil, "Deploy NFT", "WEB3", "Contract generated and deployed to Sepolia. Tx: "+txHash)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"tx_hash":  txHash,
		"status":   "deployed",
		"contract": "NFT-Contract.sol",
		"network":  "Sepolia Testnet",
	})
}

func communicateAgents(w http.ResponseWriter, r *http.Request) {
	var req struct {
		From    string `json:"from"`
		To      string `json:"to"`
		Message string `json:"message"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	logAction(nil, "Communication", req.From, "To "+req.To+": "+req.Message)
	w.WriteHeader(http.StatusOK)
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
	mux.HandleFunc("POST /agent/communicate", communicateAgents)
	mux.HandleFunc("GET /ws", handleConnections)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("🚀 APEX JARVIS IS ONLINE — http://localhost:%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
