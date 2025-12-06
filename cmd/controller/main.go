package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ReplicaInfo struct {
	ReplicaID string    `json:"replica_id"`
	PID       int       `json:"pid"`
	AgentURL  string    `json:"agent_url"`
	StartedAt time.Time `json:"started_at"`
	Status    string    `json:"status"` // running, stopped, exited, exited_error, lost, agent_down
	Load      float64   `json:"load"`
}

type AgentStatus struct {
	Hostname string        `json:"hostname"`
	Replicas []ReplicaInfo `json:"replicas"`
}

type StartRequest struct {
	ReplicaID string          `json:"replica_id"`
	Config    json.RawMessage `json:"config"`
}

type StopRequest struct {
	ReplicaID string `json:"replica_id"`
}

type ClusterStatus struct {
	DesiredReplicas int           `json:"desired_replicas"`
	Replicas        []ReplicaInfo `json:"replicas"`
	Agents          []string      `json:"agents"`
	AverageLoad     float64       `json:"average_load"`
}

type Controller struct {
	mu              sync.Mutex
	agents          map[string]bool
	replicas        map[string]ReplicaInfo
	desiredReplicas int
	rrIndex         int
	lastScaleTime   time.Time
	minReplicas     int
	maxReplicas     int
}

func NewController(initialAgents []string, initialDesired int) *Controller {
	agents := make(map[string]bool)
	for _, a := range initialAgents {
		if strings.TrimSpace(a) == "" {
			continue
		}
		agents[strings.TrimSpace(a)] = true
	}
	return &Controller{
		agents:          agents,
		replicas:        make(map[string]ReplicaInfo),
		desiredReplicas: initialDesired,
		minReplicas:     1,
		maxReplicas:     20,
	}
}

func (c *Controller) Status() ClusterStatus {
	c.mu.Lock()
	defer c.mu.Unlock()

	var list []ReplicaInfo
	var sumLoad float64
	var countLoad int

	for _, r := range c.replicas {
		list = append(list, r)
		if r.Status == "running" {
			sumLoad += r.Load
			countLoad++
		}
	}

	avgLoad := 0.0
	if countLoad > 0 {
		avgLoad = sumLoad / float64(countLoad)
	}

	var agents []string
	for a := range c.agents {
		agents = append(agents, a)
	}

	return ClusterStatus{
		DesiredReplicas: c.desiredReplicas,
		Replicas:        list,
		Agents:          agents,
		AverageLoad:     avgLoad,
	}
}

func (c *Controller) registerAgent(url string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if url == "" {
		return
	}
	if !c.agents[url] {
		log.Printf("Controller: new agent registered: %s", url)
		c.agents[url] = true
	}
}

func (c *Controller) setDesiredReplicas(n int) {
	if n < c.minReplicas {
		n = c.minReplicas
	}
	if n > c.maxReplicas {
		n = c.maxReplicas
	}
	c.mu.Lock()
	c.desiredReplicas = n
	c.mu.Unlock()

	go c.reconcile()
}

func (c *Controller) scaleUp() {
	c.mu.Lock()
	if c.desiredReplicas < c.maxReplicas {
		c.desiredReplicas++
	}
	newDesired := c.desiredReplicas
	c.mu.Unlock()
	log.Printf("Controller: manual scale up, desired=%d", newDesired)
	go c.reconcile()
}

func (c *Controller) scaleDown() {
	c.mu.Lock()
	if c.desiredReplicas > c.minReplicas {
		c.desiredReplicas--
	}
	newDesired := c.desiredReplicas
	c.mu.Unlock()
	log.Printf("Controller: manual scale down, desired=%d", newDesired)
	go c.reconcile()
}

func (c *Controller) pickAgentForNewReplica() string {
	if len(c.agents) == 0 {
		return ""
	}

	counts := make(map[string]int)
	for a := range c.agents {
		counts[a] = 0
	}
	for _, r := range c.replicas {
		counts[r.AgentURL]++
	}

	var bestAgent string
	bestCount := int(^uint(0) >> 1) // max int

	for agent := range c.agents {
		if counts[agent] < bestCount {
			bestCount = counts[agent]
			bestAgent = agent
		}
	}

	return bestAgent
}

func (c *Controller) reconcile() {
	c.mu.Lock()
	defer c.mu.Unlock()

	currentRunning := 0
	for _, r := range c.replicas {
		if r.Status == "running" {
			currentRunning++
		}
	}
	desired := c.desiredReplicas
	log.Printf("Controller: reconcile currentRunning=%d, desired=%d", currentRunning, desired)

	if desired > currentRunning {
		need := desired - currentRunning
		for i := 0; i < need; i++ {
			agent := c.pickAgentForNewReplica()
			if agent == "" {
				log.Println("Controller: no agents available to start replicas")
				return
			}
			replicaID := fmt.Sprintf("r-%d-%d", time.Now().UnixNano(), rand.Intn(1000))
			info, err := c.startReplicaOnAgent(agent, replicaID)
			if err != nil {
				log.Printf("Controller: failed to start replica on %s: %v", agent, err)
				continue
			}
			c.replicas[replicaID] = info
		}
	} else if desired < currentRunning {
		needStop := currentRunning - desired
		for needStop > 0 {
			for id, r := range c.replicas {
				if r.Status == "running" {
					if err := c.stopReplicaOnAgent(r.AgentURL, id); err != nil {
						log.Printf("Controller: failed to stop replica %s: %v", id, err)
					}
					r.Status = "stopped"
					c.replicas[id] = r
					needStop--
					break
				}
			}
			if needStop > 0 && !existsRunning(c.replicas) {
				break
			}
		}
	}
}

func existsRunning(replicas map[string]ReplicaInfo) bool {
	for _, r := range replicas {
		if r.Status == "running" {
			return true
		}
	}
	return false
}

func (c *Controller) startReplicaOnAgent(agentURL, replicaID string) (ReplicaInfo, error) {
	config := json.RawMessage(`{"service":"payload","version":"1.0"}`)

	reqBody := StartRequest{
		ReplicaID: replicaID,
		Config:    config,
	}
	body, _ := json.Marshal(&reqBody)
	resp, err := http.Post(agentURL+"/start", "application/json", bytes.NewReader(body))
	if err != nil {
		return ReplicaInfo{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ReplicaInfo{}, fmt.Errorf("agent returned %s", resp.Status)
	}
	var info ReplicaInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return ReplicaInfo{}, err
	}
	info.AgentURL = agentURL
	return info, nil
}

func (c *Controller) stopReplicaOnAgent(agentURL, replicaID string) error {
	req := StopRequest{ReplicaID: replicaID}
	body, _ := json.Marshal(&req)
	resp, err := http.Post(agentURL+"/stop", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("agent returned %s", resp.Status)
	}
	return nil
}

func (c *Controller) monitorLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		c.pollAgents()
	}
}

func (c *Controller) pollAgents() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.agents) == 0 {
		return
	}

	for agent := range c.agents {
		status, err := getAgentStatus(agent)
		if err != nil {
			log.Printf("Controller: agent %s unreachable: %v", agent, err)
			for id, r := range c.replicas {
				if r.AgentURL == agent && r.Status == "running" {
					r.Status = "agent_down"
					c.replicas[id] = r
				}
			}
			continue
		}
		for _, r := range status.Replicas {
			r.AgentURL = agent
			c.replicas[r.ReplicaID] = r
		}
	}
}

func (c *Controller) autoscalerLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		c.runAutoscalingStep()
	}
}

func (c *Controller) runAutoscalingStep() {
	c.mu.Lock()

	if time.Since(c.lastScaleTime) < 20*time.Second {
		c.mu.Unlock()
		return
	}

	var sumLoad float64
	var count int
	for _, r := range c.replicas {
		if r.Status == "running" {
			sumLoad += r.Load
			count++
		}
	}
	if count == 0 {
		c.mu.Unlock()
		return
	}
	avg := sumLoad / float64(count)
	desired := c.desiredReplicas
	c.mu.Unlock()

	log.Printf("Controller: autoscale check, avgLoad=%.2f, desired=%d", avg, desired)

	if avg > 0.7 && desired < c.maxReplicas {
		c.mu.Lock()
		c.desiredReplicas++
		c.lastScaleTime = time.Now()
		newDesired := c.desiredReplicas
		c.mu.Unlock()
		log.Printf("Controller: autoscale UP, new desired=%d", newDesired)
		go c.reconcile()
	} else if avg < 0.3 && desired > c.minReplicas {
		c.mu.Lock()
		c.desiredReplicas--
		c.lastScaleTime = time.Now()
		newDesired := c.desiredReplicas
		c.mu.Unlock()
		log.Printf("Controller: autoscale DOWN, new desired=%d", newDesired)
		go c.reconcile()
	}
}

func getAgentStatus(agentURL string) (AgentStatus, error) {
	resp, err := http.Get(agentURL + "/status")
	if err != nil {
		return AgentStatus{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return AgentStatus{}, fmt.Errorf("status %s", resp.Status)
	}
	var st AgentStatus
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return AgentStatus{}, err
	}
	return st, nil
}

func main() {
	rand.Seed(time.Now().UnixNano())

	initialDesired := 2
	if v := os.Getenv("DESIRED_REPLICAS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			initialDesired = n
		}
	}

	knownAgentsEnv := os.Getenv("KNOWN_AGENTS")
	var initialAgents []string
	if knownAgentsEnv != "" {
		initialAgents = strings.Split(knownAgentsEnv, ",")
	}

	ctrl := NewController(initialAgents, initialDesired)

	go ctrl.reconcile()
	go ctrl.monitorLoop()
	go ctrl.autoscalerLoop()

	mux := http.NewServeMux()

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, ctrl.Status())
	})

	mux.HandleFunc("/register", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			URL string `json:"url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ctrl.registerAgent(body.URL)
		writeJSON(w, http.StatusOK, map[string]string{"status": "registered"})
	})

	mux.HandleFunc("/scale", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Replicas int `json:"replicas"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ctrl.setDesiredReplicas(body.Replicas)
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "desired": body.Replicas})
	})

	mux.HandleFunc("/scale/up", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		ctrl.scaleUp()
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("/scale/down", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		ctrl.scaleDown()
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	port := os.Getenv("CONTROLLER_PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("Controller listening on :%s", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("controller failed: %v", err)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Println("writeJSON error:", err)
	}
}
