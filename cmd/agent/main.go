package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"time"
)

type StartRequest struct {
	ReplicaID string          `json:"replica_id"`
	Config    json.RawMessage `json:"config"` // конфиг не на диске
}

type StopRequest struct {
	ReplicaID string `json:"replica_id"`
}

type ReplicaInfo struct {
	ReplicaID string    `json:"replica_id"`
	PID       int       `json:"pid"`
	StartedAt time.Time `json:"started_at"`
	Status    string    `json:"status"` // running, stopped, exited, exited_error
	Load      float64   `json:"load"`
}

type AgentStatus struct {
	Hostname string        `json:"hostname"`
	Replicas []ReplicaInfo `json:"replicas"`
}

type runningProcess struct {
	info     ReplicaInfo
	cmd      *exec.Cmd
	load     float64
	loadStop chan struct{}
}

type Agent struct {
	mu       sync.Mutex
	replicas map[string]*runningProcess
}

func NewAgent() *Agent {
	return &Agent{
		replicas: make(map[string]*runningProcess),
	}
}

func (a *Agent) StartReplica(req StartRequest) (ReplicaInfo, error) {
	if req.ReplicaID == "" {
		return ReplicaInfo{}, errors.New("empty replica_id")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if _, exists := a.replicas[req.ReplicaID]; exists {
		return ReplicaInfo{}, fmt.Errorf("replica %s already exists", req.ReplicaID)
	}

	log.Printf("Agent: starting replica %s with config: %s", req.ReplicaID, string(req.Config))

	cmd := exec.Command("./payload")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return ReplicaInfo{}, fmt.Errorf("failed to start payload: %w", err)
	}

	info := ReplicaInfo{
		ReplicaID: req.ReplicaID,
		PID:       cmd.Process.Pid,
		StartedAt: time.Now(),
		Status:    "running",
		Load:      0.0,
	}

	rp := &runningProcess{
		info:     info,
		cmd:      cmd,
		load:     0.0,
		loadStop: make(chan struct{}),
	}
	a.replicas[req.ReplicaID] = rp

	go a.waitProcess(req.ReplicaID, cmd)

	go a.loadLoop(req.ReplicaID, rp.loadStop)

	log.Printf("Agent: started payload replica %s (pid=%d)", req.ReplicaID, info.PID)

	return info, nil
}

func (a *Agent) waitProcess(id string, cmd *exec.Cmd) {
	err := cmd.Wait()

	a.mu.Lock()
	defer a.mu.Unlock()

	rp, ok := a.replicas[id]
	if !ok {
		return
	}

	if err != nil {
		log.Printf("Agent: payload %s exited with error: %v", id, err)
		rp.info.Status = "exited_error"
	} else {
		log.Printf("Agent: payload %s exited normally", id)
		rp.info.Status = "exited"
	}
	close(rp.loadStop)
}

func (a *Agent) loadLoop(id string, stop <-chan struct{}) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		select {
		case <-stop:
			return
		case <-time.After(2 * time.Second):
			a.mu.Lock()
			rp, ok := a.replicas[id]
			if !ok {
				a.mu.Unlock()
				return
			}
			delta := (r.Float64() - 0.5) * 0.2
			rp.load += delta
			if rp.load < 0 {
				rp.load = 0
			}
			if rp.load > 1 {
				rp.load = 1
			}
			rp.info.Load = rp.load
			a.replicas[id] = rp
			a.mu.Unlock()
		}
	}
}

func (a *Agent) StopReplica(req StopRequest) error {
	if req.ReplicaID == "" {
		return errors.New("empty replica_id")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	rp, ok := a.replicas[req.ReplicaID]
	if !ok {
		return fmt.Errorf("replica %s not found", req.ReplicaID)
	}

	if rp.cmd.Process != nil {
		if err := rp.cmd.Process.Kill(); err != nil {
			return fmt.Errorf("failed to kill process: %w", err)
		}
	}

	rp.info.Status = "stopped"
	close(rp.loadStop)
	delete(a.replicas, req.ReplicaID)

	log.Printf("Agent: stopped replica %s", req.ReplicaID)
	return nil
}

func (a *Agent) Status() AgentStatus {
	a.mu.Lock()
	defer a.mu.Unlock()

	var list []ReplicaInfo
	for _, rp := range a.replicas {
		// обновляем uptime на лету
		rp.info.Load = rp.load
		list = append(list, rp.info)
	}
	hostname, _ := os.Hostname()
	return AgentStatus{
		Hostname: hostname,
		Replicas: list,
	}
}

//http регистрация

func main() {
	rand.Seed(time.Now().UnixNano())
	agent := NewAgent()

	port := os.Getenv("AGENT_PORT")
	if port == "" {
		port = "9000"
	}
	agentURL := os.Getenv("AGENT_URL")

	controllerURL := os.Getenv("CONTROLLER_URL")
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, agent.Status())
	})

	mux.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var req StartRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		info, err := agent.StartReplica(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, info)
	})

	mux.HandleFunc("/stop", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var req StopRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := agent.StopReplica(req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "stopped"})
	})

	go func() {
		time.Sleep(2 * time.Second)
		if controllerURL != "" && agentURL != "" {
			registerInController(controllerURL, agentURL)
		}
	}()

	log.Printf("Agent listening on :%s, url=%s, controller=%s", port, agentURL, controllerURL)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("agent failed: %v", err)
	}
}

func registerInController(controllerURL, agentURL string) {
	type regReq struct {
		URL string `json:"url"`
	}
	body, _ := json.Marshal(regReq{URL: agentURL})
	resp, err := http.Post(controllerURL+"/register", "application/json", bytesReader(body))
	if err != nil {
		log.Printf("Agent: failed to register in controller: %v", err)
		return
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("Agent: controller responded with %s on register", resp.Status)
		return
	}
	log.Printf("Agent: registered in controller %s", controllerURL)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Println("writeJSON error:", err)
	}
}

func bytesReader(b []byte) *bytes.Reader {
	return bytes.NewReader(b)
}
