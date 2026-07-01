package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// Task models
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

// Global state
var (
	tasks     = make(map[string]*Task)
	mu        sync.RWMutex
	taskQueue = make(chan *Task, 100)
	upgrader  = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	clients   = make(map[*websocket.Conn]bool)
	broadcast = make(chan interface{}, 100)
	clientsMu sync.Mutex
)

func init() {
	go processQueue()
	go handleBroadcasts()
}

func logAction(task *Task, action, agent, details string) {
	entry := LogEntry{Timestamp: time.Now().Format(time.RFC3339), Action: action, Agent: agent, Details: details}
	mu.Lock()
	task.Logs = append(task.Logs, entry)
	task.UpdatedAt = time.Now()
	mu.Unlock()

	// Broadcast to WebSocket clients
	broadcast <- map[string]interface{}{
		"type":    "log",
		"task_id": task.ID,
		"log":     entry,
	}
	log.Printf("[%s] %s: %s", agent, action, details)
}

func logActionLocked(task *Task, action, agent, details string) {
	entry := LogEntry{Timestamp: time.Now().Format(time.RFC3339), Action: action, Agent: agent, Details: details}
	task.Logs = append(task.Logs, entry)
	task.UpdatedAt = time.Now()

	broadcast <- map[string]interface{}{
		"type":    "log",
		"task_id": task.ID,
		"log":     entry,
	}
	log.Printf("[%s] %s: %s", agent, action, details)
}

func decomposeGoal(goal string) []SubTask {
	if strings.Contains(strings.ToLower(goal), "research") || strings.Contains(strings.ToLower(goal), "nft") {
		return []SubTask{
			{ID: uuid.New().String(), Type: "browser", Goal: "Research", Status: "pending"},
			{ID: uuid.New().String(), Type: "data", Goal: "Analyze", Status: "pending"},
			{ID: uuid.New().String(), Type: "file", Goal: "Generate", Status: "pending"},
		}
	}
	return []SubTask{{ID: uuid.New().String(), Type: "file", Goal: "Process", Status: "pending"}}
}

func createSandbox(id string) string { return "sandbox-" + strings.ReplaceAll(id, "-", "")[:8] }

func processQueue() {
	for t := range taskQueue {
		executeTaskAsync(t)
	}
}

func executeTaskAsync(task *Task) {
	logAction(task, "Started", "JARVIS", "Initializing autonomous routines...")
	var wg sync.WaitGroup
	for i := range task.SubTasks {
		wg.Add(1)
		go func(st *SubTask) {
			defer wg.Done()
			mu.RLock()
			if task.Status == "stopped" {
				mu.RUnlock()
				return
			}
			mu.RUnlock()

			time.Sleep(1500 * time.Millisecond)

			mu.Lock()
			if task.Status == "stopped" {
				mu.Unlock()
				return
			}
			st.Status = "completed"
			st.Result = st.Type + " task finished successfully"
			task.Artifacts = append(task.Artifacts, st.Type+"-artifact-"+uuid.New().String()[:4])

			completedCount := 0
			for _, s := range task.SubTasks {
				if s.Status == "completed" {
					completedCount++
				}
			}
			task.Progress = (completedCount * 100) / len(task.SubTasks)
			logActionLocked(task, "SubTask Complete", "AGENT-"+strings.ToUpper(st.Type), st.Goal)

			prog, status := task.Progress, task.Status
			mu.Unlock()

			broadcast <- map[string]interface{}{
				"type":     "task_update",
				"task_id":  task.ID,
				"progress": prog,
				"status":   status,
			}
		}(&task.SubTasks[i])
	}
	wg.Wait()

	mu.Lock()
	if task.Status != "stopped" {
		task.Status = "completed"
		task.Progress = 100
		logActionLocked(task, "Finalized", "JARVIS", "Goal achieved.")
	}
	prog, status := task.Progress, task.Status
	mu.Unlock()

	broadcast <- map[string]interface{}{
		"type":     "task_update",
		"task_id":  task.ID,
		"progress": prog,
		"status":   status,
	}
}

func handleBroadcasts() {
	for msg := range broadcast {
		clientsMu.Lock()
		for client := range clients {
			client.SetWriteDeadline(time.Now().Add(time.Second * 5))
			err := client.WriteJSON(msg)
			if err != nil {
				log.Printf("WebSocket error: %v", err)
				client.Close()
				delete(clients, client)
			}
		}
		clientsMu.Unlock()
	}
}

// Handlers
func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WS Upgrade error: %v", err)
		return
	}
	clientsMu.Lock()
	clients[conn] = true
	clientsMu.Unlock()
	log.Printf("New WebSocket connection established")
}

func createTask(w http.ResponseWriter, r *http.Request) {
	var req struct{ Goal string `json:"goal"` }
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	id := strings.ReplaceAll(uuid.New().String(), "-", "")
	task := &Task{
		ID:        id,
		Goal:      req.Goal,
		Status:    "running",
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
	json.NewEncoder(w).Encode(map[string]string{"task_id": id})
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

	// Deep copy to avoid data races during JSON encoding
	copyTask := *t
	copyTask.Logs = make([]LogEntry, len(t.Logs))
	copy(copyTask.Logs, t.Logs)
	copyTask.SubTasks = make([]SubTask, len(t.SubTasks))
	copy(copyTask.SubTasks, t.SubTasks)
	copyTask.Artifacts = make([]string, len(t.Artifacts))
	copy(copyTask.Artifacts, t.Artifacts)
	mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(copyTask)
}

func stopTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mu.Lock()
	if t, ok := tasks[id]; ok {
		t.Status = "stopped"
		logActionLocked(t, "Stopped", "USER", "Process terminated by operator.")
	}
	mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func deployNFT(w http.ResponseWriter, r *http.Request) {
	txHash := "0x" + strings.ReplaceAll(uuid.New().String(), "-", "")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"tx_hash": txHash, "status": "deployed", "contract": "ApexNFT_ERC721"})
}

func communicateAgent(w http.ResponseWriter, r *http.Request) {
	var req struct{ Message string `json:"message"` }
	json.NewDecoder(r.Body).Decode(&req)
	response := "I am processing your request: " + req.Message
	if strings.Contains(strings.ToLower(req.Message), "hello") {
		response = "Greetings. I am APEX JARVIS. How may I assist your mission today?"
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"response": response})
}

const indexHTML = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>APEX JARVIS | Premium AI Interface</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/three.js/0.160.0/three.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/vanilla-tilt@1.8.1/dist/vanilla-tilt.min.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700&family=Rajdhani:wght@300;500;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --neon-cyan: #00f3ff;
            --neon-purple: #bc13fe;
            --dark-bg: #050505;
        }
        body {
            background-color: var(--dark-bg);
            color: #e0e0e0;
            font-family: 'Rajdhani', sans-serif;
            overflow-x: hidden;
        }
        .orbitron { font-family: 'Orbitron', sans-serif; }

        /* Matrix Background */
        #canvas-bg {
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            z-index: -1;
            opacity: 0.15;
        }

        /* Animated Eye */
        .eye-container {
            position: relative;
            width: 280px;
            height: 280px;
            margin: 0 auto;
            perspective: 1000px;
        }
        .eye-outer {
            width: 100%;
            height: 100%;
            border-radius: 50%;
            border: 2px solid var(--neon-cyan);
            box-shadow: 0 0 20px var(--neon-cyan), inset 0 0 20px var(--neon-cyan);
            position: absolute;
            animation: pulse 4s infinite ease-in-out;
        }
        .eye-inner {
            width: 80%;
            height: 80%;
            margin: 10%;
            border-radius: 50%;
            border: 1px dashed var(--neon-purple);
            position: absolute;
            animation: spin 10s linear infinite;
        }
        .eye-core {
            width: 40%;
            height: 40%;
            margin: 30%;
            background: radial-gradient(circle, var(--neon-cyan) 0%, transparent 70%);
            border-radius: 50%;
            position: absolute;
            display: flex;
            align-items: center;
            justify-content: center;
            box-shadow: 0 0 30px var(--neon-cyan);
        }
        .eye-ks {
            font-size: 2rem;
            font-weight: bold;
            color: white;
            text-shadow: 0 0 10px var(--neon-cyan);
            animation: spiral-ks 5s infinite linear;
        }

        @keyframes pulse {
            0%, 100% { transform: scale(1); opacity: 0.8; }
            50% { transform: scale(1.05); opacity: 1; }
        }
        @keyframes spin {
            from { transform: rotate(0deg); }
            to { transform: rotate(360deg); }
        }
        @keyframes spiral-ks {
            0% { transform: rotate(0deg) scale(0.8); }
            50% { transform: rotate(180deg) scale(1.2); }
            100% { transform: rotate(360deg) scale(0.8); }
        }

        .thinking .eye-outer {
            animation: pulse 0.5s infinite ease-in-out;
            border-color: var(--neon-purple);
            box-shadow: 0 0 30px var(--neon-purple);
        }

        /* Glassmorphism */
        .glass {
            background: rgba(15, 15, 15, 0.4);
            backdrop-filter: blur(20px);
            border: 1px solid rgba(0, 243, 255, 0.1);
            border-radius: 16px;
            box-shadow: inset 0 0 15px rgba(0, 243, 255, 0.05), 0 10px 30px rgba(0, 0, 0, 0.5);
            transition: all 0.3s ease;
        }
        .glass:hover {
            border: 1px solid rgba(0, 243, 255, 0.3);
            box-shadow: inset 0 0 20px rgba(0, 243, 255, 0.1), 0 15px 40px rgba(0, 0, 0, 0.7);
        }
        .neon-text {
            color: var(--neon-cyan);
            text-shadow: 0 0 5px var(--neon-cyan), 0 0 10px var(--neon-cyan);
        }
        .neon-border {
            border: 1px solid var(--neon-cyan);
            box-shadow: 0 0 10px var(--neon-cyan);
        }

        /* Waveform */
        #waveform {
            width: 100%;
            height: 60px;
            background: rgba(0, 243, 255, 0.05);
            border-radius: 30px;
        }

        /* Animations */
        .fade-in { animation: fadeIn 0.8s ease-out forwards; }
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }

        /* Custom Scrollbar */
        ::-webkit-scrollbar { width: 4px; }
        ::-webkit-scrollbar-track { background: #050505; }
        ::-webkit-scrollbar-thumb { background: var(--neon-cyan); border-radius: 10px; }

        .spiral-mode-active .eye-container {
            filter: hue-rotate(280deg);
        }
        .spiral-mode-active {
            background: radial-gradient(circle at center, #1a0033 0%, #050505 100%);
        }
        #spiral-canvas {
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            pointer-events: none;
            z-index: 0;
            opacity: 0;
            transition: opacity 1s ease;
        }
        .spiral-mode-active #spiral-canvas {
            opacity: 1;
        }
    </style>
</head>
<body class="p-4 md:p-8">
    <canvas id="canvas-bg"></canvas>
    <canvas id="spiral-canvas"></canvas>

    <div class="max-w-7xl mx-auto grid grid-cols-1 lg:grid-cols-12 gap-8 relative z-10">
        <!-- Left Sidebar: System Status -->
        <div class="lg:col-span-3 space-y-6 fade-in" style="animation-delay: 0.1s">
            <div class="glass p-6" data-tilt data-tilt-max="5" data-tilt-glare data-tilt-max-glare="0.1">
                <h2 class="orbitron text-sm font-bold mb-4 neon-text">SYSTEM STATUS</h2>
                <div class="space-y-4 text-xs">
                    <div class="flex justify-between items-center">
                        <span>CPU UTILIZATION</span>
                        <span class="text-cyan-400">24%</span>
                    </div>
                    <div class="w-full bg-gray-800 h-1 rounded-full">
                        <div class="bg-cyan-500 h-1 rounded-full" style="width: 24%"></div>
                    </div>
                    <div class="flex justify-between items-center">
                        <span>NEURAL LINK</span>
                        <span class="text-green-400">STABLE</span>
                    </div>
                    <div class="flex justify-between items-center">
                        <span>BLOCKCHAIN SYNC</span>
                        <span class="text-cyan-400">99.9%</span>
                    </div>
                    <hr class="border-gray-800 my-2">
                    <div class="flex justify-between items-center">
                        <span>DAILY STREAK</span>
                        <span class="text-orange-500 font-bold flex items-center gap-1">
                            <span class="animate-bounce">🔥</span> <span id="streakCounter">0</span>
                        </span>
                    </div>
                </div>
            </div>

            <div class="glass p-6" data-tilt data-tilt-max="5" data-tilt-glare data-tilt-max-glare="0.1">
                <h2 class="orbitron text-sm font-bold mb-4 neon-text">WEB3 PORTAL</h2>
                <button id="connectWallet" class="w-full py-2 mb-4 rounded border border-cyan-500 text-cyan-500 hover:bg-cyan-500 hover:text-black transition-all duration-300 text-sm font-bold orbitron">
                    CONNECT WALLET
                </button>
                <button id="deployNFT" class="w-full py-2 rounded bg-purple-600 hover:bg-purple-500 text-white transition-all duration-300 text-sm font-bold orbitron hidden">
                    DEPLOY NFT
                </button>
                <div id="walletInfo" class="mt-2 text-center text-xs text-gray-400"></div>
            </div>
        </div>

        <!-- Center: JARVIS Core -->
        <div class="lg:col-span-6 flex flex-col items-center space-y-8 fade-in">
            <h1 class="orbitron text-4xl md:text-6xl font-bold tracking-widest text-center neon-text mb-4">APEX JARVIS</h1>

            <div class="eye-container" id="jarvis-eye">
                <div id="three-container" class="absolute inset-0"></div>
                <div class="eye-outer pointer-events-none"></div>
                <div class="eye-core pointer-events-none">
                    <div class="eye-ks">K/S</div>
                </div>
            </div>

            <div class="w-full max-w-md space-y-6">
                <div class="relative">
                    <input type="text" id="userInput" placeholder="COMMAND JARVIS..."
                        class="w-full bg-transparent border-b-2 border-cyan-900 focus:border-cyan-400 text-cyan-100 p-4 outline-none orbitron text-center text-xl transition-all duration-500">
                </div>

                <div class="flex justify-center items-center gap-6">
                    <button id="micBtn" class="w-16 h-16 rounded-full border-2 border-cyan-500 flex items-center justify-center hover:bg-cyan-500/20 transition-all duration-300 relative group">
                        <svg xmlns="http://www.w3.org/2000/svg" class="h-8 w-8 text-cyan-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11a7 7 0 01-7 7m0 0a7 7 0 01-7-7m7-2v4m0 0h3m-3 0H9m12 0a9 9 0 11-18 0 9 9 0 0118 0z" />
                        </svg>
                        <div class="absolute inset-0 rounded-full bg-cyan-500/20 animate-ping hidden" id="micRipple"></div>
                    </button>

                    <button id="spiralToggle" class="orbitron text-xs px-4 py-2 border border-purple-500 text-purple-400 rounded hover:bg-purple-500/20 transition-all">
                        SPIRAL MODE
                    </button>
                    <button id="ambientToggle" class="orbitron text-xs px-4 py-2 border border-cyan-500 text-cyan-400 rounded hover:bg-cyan-500/20 transition-all">
                        AMBIENT: OFF
                    </button>
                </div>

                <canvas id="waveform" class="hidden"></canvas>

                <div id="aiResponse" class="text-center text-cyan-300 text-sm italic h-6 opacity-80"></div>
            </div>
        </div>

        <!-- Right Sidebar: Live Feed -->
        <div class="lg:col-span-3 space-y-6 fade-in" style="animation-delay: 0.2s">
            <div class="glass p-6 h-[500px] flex flex-col" data-tilt data-tilt-max="3" data-tilt-glare data-tilt-max-glare="0.1">
                <h2 class="orbitron text-sm font-bold mb-4 neon-text">MISSION LOGS</h2>
                <div id="logs" class="flex-grow overflow-y-auto space-y-3 text-[10px] uppercase font-mono tracking-tighter">
                    <div class="text-cyan-800">[SYSTEM] KERNEL LOADED</div>
                    <div class="text-cyan-800">[SYSTEM] JARVIS PROTOCOLS ACTIVE</div>
                </div>
            </div>
        </div>
    </div>

    <!-- Achievement Popup -->
    <div id="achievementPopup" class="fixed top-12 left-1/2 transform -translate-x-1/2 glass p-4 z-[100] border-orange-500 border-2 translate-y-[-200%] transition-transform duration-700">
        <div class="flex items-center gap-4">
            <div class="text-4xl">🏆</div>
            <div>
                <h4 class="orbitron text-orange-500 font-bold text-sm">ACHIEVEMENT UNLOCKED</h4>
                <p id="achievementDesc" class="text-xs text-gray-300"></p>
            </div>
        </div>
    </div>

    <!-- Active Tasks Overlay -->
    <div id="taskOverlay" class="fixed bottom-8 left-8 right-8 lg:left-auto lg:w-96 glass p-6 translate-y-full transition-transform duration-500 z-50">
        <div class="flex justify-between items-center mb-4">
            <h3 class="orbitron text-xs font-bold neon-text">ACTIVE MISSION</h3>
            <span id="taskProgress" class="text-xs text-cyan-400">0%</span>
        </div>
        <div class="w-full bg-gray-800 h-1.5 rounded-full mb-4">
            <div id="progressBar" class="bg-cyan-500 h-1.5 rounded-full transition-all duration-500" style="width: 0%"></div>
        </div>
        <div id="taskGoal" class="text-xs text-gray-400 italic mb-2"></div>
        <div id="taskStatus" class="text-[10px] text-cyan-600 font-bold">INITIALIZING...</div>
    </div>

    <script>
        // Matrix Background
        const canvas = document.getElementById('canvas-bg');
        const ctx = canvas.getContext('2d');
        canvas.width = window.innerWidth;
        canvas.height = window.innerHeight;

        const words = "010101 APEX JARVIS KOOLA SPIRAL AI AGENT BLOCKCHAIN NEURAL NET";
        const drops = [];
        const fontSize = 14;
        const columns = canvas.width / fontSize;

        for (let x = 0; x < columns; x++) drops[x] = 1;

        function drawMatrix() {
            ctx.fillStyle = 'rgba(5, 5, 5, 0.15)';
            ctx.fillRect(0, 0, canvas.width, canvas.height);
            ctx.fillStyle = Math.random() > 0.1 ? '#00f3ff' : '#bc13fe';
            ctx.font = fontSize + 'px monospace';
            for (let i = 0; i < drops.length; i++) {
                const text = words.charAt(Math.floor(Math.random() * words.length));
                ctx.fillText(text, i * fontSize, drops[i] * fontSize);
                if (drops[i] * fontSize > canvas.height && Math.random() > 0.975) drops[i] = 0;
                drops[i]++;
            }
        }
        setInterval(drawMatrix, 50);

        // Three.js Holographic Orb
        const threeContainer = document.getElementById('three-container');
        const scene = new THREE.Scene();
        const camera = new THREE.PerspectiveCamera(75, 1, 0.1, 1000);
        const renderer = new THREE.WebGLRenderer({ alpha: true, antialias: true });
        renderer.setSize(280, 280);
        threeContainer.appendChild(renderer.domElement);

        const orbGroup = new THREE.Group();
        scene.add(orbGroup);

        // Core Sphere
        const coreGeo = new THREE.IcosahedronGeometry(1.2, 2);
        const coreMat = new THREE.MeshBasicMaterial({ color: 0x00f3ff, wireframe: true, transparent: true, opacity: 0.3 });
        const coreMesh = new THREE.Mesh(coreGeo, coreMat);
        orbGroup.add(coreMesh);

        // Particles
        const partCount = 500;
        const partGeo = new THREE.BufferGeometry();
        const partPos = new Float32Array(partCount * 3);
        for(let i=0; i<partCount*3; i++) partPos[i] = (Math.random() - 0.5) * 4;
        partGeo.setAttribute('position', new THREE.BufferAttribute(partPos, 3));
        const partMat = new THREE.PointsMaterial({ color: 0xbc13fe, size: 0.05, transparent: true, opacity: 0.8 });
        const particles = new THREE.Points(partGeo, partMat);
        orbGroup.add(particles);

        // Rings
        const ringMat = new THREE.MeshBasicMaterial({ color: 0x00f3ff, side: THREE.DoubleSide, transparent: true, opacity: 0.2 });
        const ring1 = new THREE.Mesh(new THREE.TorusGeometry(1.8, 0.02, 16, 100), ringMat);
        const ring2 = new THREE.Mesh(new THREE.TorusGeometry(2.1, 0.01, 16, 100), ringMat);
        ring2.rotation.x = Math.PI / 2;
        orbGroup.add(ring1);
        orbGroup.add(ring2);

        camera.position.z = 5;

        function animateThree() {
            requestAnimationFrame(animateThree);
            orbGroup.rotation.y += 0.01;
            orbGroup.rotation.x += 0.005;
            particles.rotation.y -= 0.02;

            const pulse = 1 + Math.sin(Date.now() * 0.002) * 0.1;
            orbGroup.scale.set(pulse, pulse, pulse);

            renderer.render(scene, camera);
        }
        animateThree();

        // WebSockets
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const ws = new WebSocket(protocol + '//' + window.location.host + '/ws');

        ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            if (data.type === 'log') {
                addLog(data.log.agent, data.log.action, data.log.details);
            } else if (data.type === 'task_update') {
                updateTaskUI(data);
            }
        };

        function addLog(agent, action, details) {
            const logs = document.getElementById('logs');
            const entry = document.createElement('div');
            entry.className = 'border-l border-cyan-500 pl-2 py-1 fade-in';
            entry.innerHTML = '<span class="text-cyan-400">[' + agent + ']</span> ' + action + ': <span class="text-gray-500"></span>';
            logs.appendChild(entry);

            const detailSpan = entry.querySelector('.text-gray-500');
            let i = 0;
            const timer = setInterval(() => {
                if (i < details.length) {
                    detailSpan.textContent += details.charAt(i);
                    i++;
                    logs.scrollTop = logs.scrollHeight;
                } else {
                    clearInterval(timer);
                }
            }, 15);
        }

        function updateTaskUI(data) {
            const overlay = document.getElementById('taskOverlay');
            overlay.classList.remove('translate-y-full');
            document.getElementById('taskProgress').innerText = data.progress + '%';
            document.getElementById('progressBar').style.width = data.progress + '%';
            document.getElementById('taskStatus').innerText = data.status.toUpperCase();

            if (data.progress === 100) {
                setTimeout(() => {
                    overlay.classList.add('translate-y-full');
                }, 5000);
            }
        }

        // Voice Features
        const micBtn = document.getElementById('micBtn');
        const micRipple = document.getElementById('micRipple');
        const aiResponse = document.getElementById('aiResponse');
        const userInput = document.getElementById('userInput');

        let recognition;
        if ('webkitSpeechRecognition' in window) {
            recognition = new webkitSpeechRecognition();
            recognition.continuous = false;
            recognition.interimResults = false;

            recognition.onstart = () => {
                micRipple.classList.remove('hidden');
                speak("Listening for command.");
                startVisualizer();
            };

            recognition.onresult = (event) => {
                const last = event.results.length - 1;
                const text = event.results[last][0].transcript.toLowerCase().trim();

                if (text.includes('jarvis')) {
                    playPing();
                    const command = text.split('jarvis').pop().trim();
                    if (command) {
                        userInput.value = command;
                        executeCommand(command);
                    } else {
                        speak("Yes, I am here. What is your command?");
                    }
                }
            };

            recognition.onend = () => {
                micRipple.classList.add('hidden');
            };
        }

        let audioCtx;
        function initAudio() {
            if (!audioCtx) audioCtx = new (window.AudioContext || window.webkitAudioContext)();
            if (audioCtx.state === 'suspended') audioCtx.resume();
        }

        let isJarvisActive = false;
        let hasInteracted = false;

        document.body.onclick = () => {
            if (!hasInteracted) {
                hasInteracted = true;
                initAudio();
                speak("Neural link confirmed. APEX JARVIS is fully operational.");
            }
        };

        micBtn.onclick = (e) => {
            e.stopPropagation();
            initAudio();
            if (recognition) {
                if (isJarvisActive) {
                    recognition.stop();
                    isJarvisActive = false;
                    micBtn.classList.remove('animate-pulse');
                    speak("Jarvis mode deactivated.");
                } else {
                    recognition.continuous = true;
                    recognition.start();
                    isJarvisActive = true;
                    micBtn.classList.add('animate-pulse');
                    speak("Jarvis mode activated. Listening for wake word.");
                }
            }
        };

        function playPing() {
            try {
                initAudio();
                if (window.navigator.vibrate) window.navigator.vibrate([30, 20, 30]);
                const osc = audioCtx.createOscillator();
                const gain = audioCtx.createGain();
                osc.connect(gain);
                gain.connect(audioCtx.destination);
                osc.type = 'sine';
                osc.frequency.setValueAtTime(880, audioCtx.currentTime);
                gain.gain.setValueAtTime(0.1, audioCtx.currentTime);
                gain.gain.exponentialRampToValueAtTime(0.01, audioCtx.currentTime + 0.2);
                osc.start();
                osc.stop(audioCtx.currentTime + 0.2);
                if (window.navigator.vibrate) window.navigator.vibrate(50);
            } catch(e) {}
        }

        function speak(text) {
            const utterance = new SpeechSynthesisUtterance(text);
            utterance.pitch = 0.8;
            utterance.rate = 1;
            utterance.voice = speechSynthesis.getVoices().find(v => v.name.includes('Google UK English Male')) || null;
            speechSynthesis.speak(utterance);

            // Typing effect for visual feedback
            aiResponse.innerText = "";
            let i = 0;
            const timer = setInterval(() => {
                if (i < text.length) {
                    aiResponse.innerText += text.charAt(i);
                    i++;
                } else {
                    clearInterval(timer);
                }
            }, 30);
        }

        async function executeCommand(goal) {
            if (!goal) return;
            playPing();
            addLog('USER', 'COMMAND', goal);
            document.getElementById('jarvis-eye').classList.add('thinking');

            const res = await fetch('/task', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({goal})
            });
            const data = await res.json();
            document.getElementById('jarvis-eye').classList.remove('thinking');
            document.getElementById('taskGoal').innerText = goal;
            speak("Mission accepted. Orchestrating sub-agents for goal: " + goal);
        }

        userInput.onkeypress = (e) => {
            if (e.key === 'Enter') {
                executeCommand(userInput.value);
                userInput.value = "";
            }
        };

        // Ambient Sound
        let ambientOsc;
        let ambientGain;
        const ambientToggle = document.getElementById('ambientToggle');
        let isAmbientOn = false;

        ambientToggle.onclick = () => {
            initAudio();
            if (!isAmbientOn) {
                ambientOsc = audioCtx.createOscillator();
                ambientGain = audioCtx.createGain();
                ambientOsc.type = 'sawtooth';
                ambientOsc.frequency.setValueAtTime(40, audioCtx.currentTime);
                ambientGain.gain.setValueAtTime(0, audioCtx.currentTime);
                ambientGain.gain.linearRampToValueAtTime(0.02, audioCtx.currentTime + 2);

                const filter = audioCtx.createBiquadFilter();
                filter.type = 'lowpass';
                filter.frequency.setValueAtTime(100, audioCtx.currentTime);

                ambientOsc.connect(filter);
                filter.connect(ambientGain);
                ambientGain.connect(audioCtx.destination);

                ambientOsc.start();
                isAmbientOn = true;
                ambientToggle.innerText = "AMBIENT: ON";
                speak("Ambient atmospheric resonance engaged.");
            } else {
                ambientGain.gain.linearRampToValueAtTime(0, audioCtx.currentTime + 1);
                setTimeout(() => ambientOsc.stop(), 1000);
                isAmbientOn = false;
                ambientToggle.innerText = "AMBIENT: OFF";
                speak("Standard silence restored.");
            }
        };

        // Spiral Mode
        const spiralToggle = document.getElementById('spiralToggle');
        const spiralCanvas = document.getElementById('spiral-canvas');
        const sCtx = spiralCanvas.getContext('2d');
        let spiralPoints = [];

        spiralToggle.onclick = (e) => {
            e.stopPropagation();
            document.body.classList.toggle('spiral-mode-active');
            const isActive = document.body.classList.contains('spiral-mode-active');
            spiralToggle.innerText = isActive ? "TERMINATE SPIRAL" : "SPIRAL MODE";
            speak(isActive ? "Spiral visualization engaged." : "Standard interface restored.");
            if (isActive) {
                spiralCanvas.width = window.innerWidth;
                spiralCanvas.height = window.innerHeight;
                animateSpiral();
            }
        };

        function animateSpiral() {
            if (!document.body.classList.contains('spiral-mode-active')) return;
            requestAnimationFrame(animateSpiral);
            sCtx.clearRect(0, 0, spiralCanvas.width, spiralCanvas.height);
            const centerX = spiralCanvas.width / 2;
            const centerY = spiralCanvas.height / 2;
            const time = Date.now() * 0.001;

            for (let i = 0; i < 200; i++) {
                const angle = 0.1 * i + time;
                const r = 2 * i;
                const x = centerX + r * Math.cos(angle);
                const y = centerY + r * Math.sin(angle);

                sCtx.fillStyle = 'rgba(188, 19, 254, ' + (1 - i/200) + ')';
                sCtx.beginPath();
                sCtx.arc(x, y, 2, 0, Math.PI * 2);
                sCtx.fill();

                if (i % 20 === 0) {
                    sCtx.strokeStyle = 'rgba(0, 243, 255, 0.2)';
                    sCtx.beginPath();
                    sCtx.moveTo(centerX, centerY);
                    sCtx.lineTo(x, y);
                    sCtx.stroke();
                }
            }
        }

        // Web3 Integration
        const connectBtn = document.getElementById('connectWallet');
        const deployBtn = document.getElementById('deployNFT');
        const walletInfo = document.getElementById('walletInfo');

        connectBtn.onclick = async () => {
            if (window.ethereum) {
                try {
                    const accounts = await window.ethereum.request({ method: 'eth_requestAccounts' });
                    walletInfo.innerText = "Connected: " + accounts[0].substring(0, 6) + "..." + accounts[0].substring(38);
                    connectBtn.classList.add('hidden');
                    deployBtn.classList.remove('hidden');
                    speak("Neural wallet link established.");
                } catch (err) {
                    speak("Connection failed. Check MetaMask.");
                }
            } else {
                speak("Ethereum provider not detected.");
            }
        };

        deployBtn.onclick = async () => {
            speak("Initiating NFT smart contract deployment on Sepolia. Generating Solidity boilerplate.");
            deployBtn.disabled = true;

            // Simulated Solidity Contract Generation
            const contractCode = "// SPDX-License-Identifier: MIT\n" +
"pragma solidity ^0.8.20;\n" +
"import \"@openzeppelin/contracts/token/ERC721/ERC721.sol\";\n" +
"contract ApexNFT is ERC721 {\n" +
"    constructor() ERC721(\"ApexCollection\", \"APX\") {}\n" +
"}";
            addLog('JARVIS', 'GENERATED', 'Solidity contract "ApexNFT" created.');
            console.log(contractCode);

            const res = await fetch('/deploy-nft', { method: 'POST' });
            const data = await res.json();
            addLog('WEB3', 'DEPLOYED', 'TX: ' + data.tx_hash);
            speak("Success. Contract deployed to Sepolia. Transaction hash broadcast to the mesh.");
            deployBtn.disabled = false;
        };

        // Waveform Visualizer (Real Analyser)
        const wfCanvas = document.getElementById('waveform');
        const wfCtx = wfCanvas.getContext('2d');
        let analyser;
        let dataArray;

        async function startVisualizer() {
            if (!analyser) {
                try {
                    const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
                    const source = audioCtx.createMediaStreamSource(stream);
                    analyser = audioCtx.createAnalyser();
                    analyser.fftSize = 256;
                    source.connect(analyser);
                    dataArray = new Uint8Array(analyser.frequencyBinCount);
                } catch (err) {
                    console.log("Mic access denied for visualizer");
                }
            }
        }

        function drawWave() {
            requestAnimationFrame(drawWave);
            if (micRipple.classList.contains('hidden')) {
                wfCanvas.classList.add('hidden');
                return;
            }
            wfCanvas.classList.remove('hidden');
            wfCtx.clearRect(0, 0, wfCanvas.width, wfCanvas.height);

            if (analyser) {
                analyser.getByteFrequencyData(dataArray);
                wfCtx.lineWidth = 2;
                wfCtx.strokeStyle = 'rgba(0, 243, 255, 0.8)';
                wfCtx.beginPath();
                const sliceWidth = wfCanvas.width * 1.0 / dataArray.length;
                let x = 0;
                for (let i = 0; i < dataArray.length; i++) {
                    const v = dataArray[i] / 128.0;
                    const y = v * wfCanvas.height / 2;
                    if (i === 0) wfCtx.moveTo(x, y);
                    else wfCtx.lineTo(x, y);
                    x += sliceWidth;
                }
                wfCtx.stroke();
            } else {
                // Fallback animated wave
                const time = Date.now() * 0.01;
                wfCtx.strokeStyle = 'rgba(0, 243, 255, 0.5)';
                wfCtx.beginPath();
                for (let i = 0; i < wfCanvas.width; i += 2) {
                    const y = wfCanvas.height / 2 + Math.sin(i * 0.05 + time) * 10;
                    if (i === 0) wfCtx.moveTo(i, y);
                    else wfCtx.lineTo(i, y);
                }
                wfCtx.stroke();
            }
        }
        drawWave();

        // Achievements & Streaks
        let streak = parseInt(localStorage.getItem('apex_streak') || '0');
        const lastVisit = localStorage.getItem('apex_last_visit');
        const today = new Date().toDateString();

        if (lastVisit !== today) {
            streak++;
            localStorage.setItem('apex_streak', streak);
            localStorage.setItem('apex_last_visit', today);
        }
        document.getElementById('streakCounter').innerText = streak;

        function showAchievement(desc) {
            const popup = document.getElementById('achievementPopup');
            document.getElementById('achievementDesc').innerText = desc;
            popup.style.transform = 'translate(-50%, 0)';
            playPing();
            setTimeout(() => {
                popup.style.transform = 'translate(-50%, -200%)';
            }, 5000);
        }

        window.onload = () => {
            setTimeout(() => {
                if (streak % 5 === 0 && streak > 0) {
                    showAchievement(streak + " Day Streak! You are becoming a master Commandant.");
                }
            }, 1000);
        };
    </script>
</body>
</html>
`

func main() {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
		fmt.Fprint(w, indexHTML)
	})

	mux.HandleFunc("GET /ws", wsHandler)
	mux.HandleFunc("POST /task", createTask)
	mux.HandleFunc("GET /task/{id}/status", getTaskStatus)
	mux.HandleFunc("POST /task/{id}/stop", stopTask)
	mux.HandleFunc("POST /deploy-nft", deployNFT)
	mux.HandleFunc("POST /agent/communicate", communicateAgent)
	mux.HandleFunc("GET /dashboard", func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		defer mu.RUnlock()
		json.NewEncoder(w).Encode(tasks)
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("🚀 APEX JARVIS IS ONLINE — http://localhost:%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
