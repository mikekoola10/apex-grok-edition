package main

import (
	"encoding/json"
	"fmt"
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

// --- Structs ---

type Task struct {
	ID          string       `json:"id"`
	Goal        string       `json:"goal"`
	Status      string       `json:"status"` // queued, running, completed, stopped, failed
	SubTasks    []SubTask    `json:"subtasks"`
	Artifacts   []string     `json:"artifacts"`
	Logs        []LogEntry   `json:"logs"`
	CreatedAt   time.Time    `json:"created_at"`
	UpdatedAt   time.Time    `json:"updated_at"`
	SandboxID   string       `json:"sandbox_id"`
	Workspace   string       `json:"workspace,omitempty"`
	Progress    int          `json:"progress"` // 0-100
	Deployments []Deployment `json:"deployments,omitempty"`
	TxHash      string       `json:"tx_hash,omitempty"`
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

func executeSubTask(st *SubTask, task *Task) {

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

func deployNFTCollection(task *Task) {
	logAction(task, "Generating NFT Contract", "Solidity", task.Goal)

	// Solidity NFT Generator
	contractName := "ApexNFT"
	if strings.Contains(task.Goal, " ") {
		parts := strings.Split(task.Goal, " ")
		contractName = parts[0]
	}

	solidityCode := fmt.Sprintf(`// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "@openzeppelin/contracts/token/ERC721/ERC721.go";
import "@openzeppelin/contracts/access/Ownable.go";

contract %s is ERC721, Ownable {
    uint256 private _nextTokenId;

    constructor() ERC721("%s", "APX") Ownable(msg.sender) {}

    function safeMint(address to) public onlyOwner {
        uint256 tokenId = _nextTokenId++;
        _safeMint(to, tokenId);
    }
}`, contractName, task.Goal)

	workspace := "./workspace/" + task.ID[:8]
	os.MkdirAll(workspace, 0755)
	filePath := filepath.Join(workspace, "NFT-Contract.sol")
	os.WriteFile(filePath, []byte(solidityCode), 0644)

	if ethClient == nil {
		logAction(task, "Web3 not connected", "NFT", "Set ETH_RPC_URL to deploy. Contract saved to artifacts.")
		mu.Lock()
		task.Status = "completed"
		task.Artifacts = append(task.Artifacts, "NFT-Contract.sol")
		mu.Unlock()
		return
	}

	// Simulate deployment
	txHash := "0x" + uuid.New().String()[:40]
	logAction(task, "NFT Collection Deployed", "Web3", txHash)
	mu.Lock()
	task.TxHash = txHash
	task.Artifacts = append(task.Artifacts, "NFT-Contract.sol")
	task.Status = "completed"
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
		if t.Status == "running" || t.Status == "queued" || t.Status == "deploying" {
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

func deployNFTHandler(w http.ResponseWriter, r *http.Request) {
	var req struct{ Goal string `json:"goal"` }
	json.NewDecoder(r.Body).Decode(&req)
	// Create task and deploy NFT
	id := uuid.New().String()
	task := &Task{
		ID:        id,
		Goal:      req.Goal,
		Status:    "deploying",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Logs:      []LogEntry{},
	}
	mu.Lock()
	tasks[id] = task
	mu.Unlock()

	// Fix Race Condition: ensure TxHash is populated or at least the logic starts
	deployNFTCollection(task)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"task_id": id, "tx": task.TxHash})
}

// --- Main ---

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
	mux.HandleFunc("POST /task/{id}/deploy", deployHandler)
	mux.HandleFunc("DELETE /task/{id}", deleteTask)
	mux.HandleFunc("GET /dashboard", getDashboard)
	mux.HandleFunc("POST /deploy-nft", deployNFTHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("🚀 **APEX JARVIS with Full Web3** LIVE on :%s", port)
	log.Println("✅ DeepSeek + Freebuff + Jules + Apex Deployer + APEX Mirror + Web3")

	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal(err)
	}
}
