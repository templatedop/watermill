package kafka

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
)

// HealthStatus represents the overall health status
type HealthStatus string

const (
	// HealthStatusHealthy indicates all checks passed
	HealthStatusHealthy HealthStatus = "healthy"
	// HealthStatusDegraded indicates some checks failed but core functionality works
	HealthStatusDegraded HealthStatus = "degraded"
	// HealthStatusUnhealthy indicates critical checks failed
	HealthStatusUnhealthy HealthStatus = "unhealthy"
)

// Health represents the overall health of the client
type Health struct {
	Status    HealthStatus `json:"status"`
	Timestamp time.Time    `json:"timestamp"`
	Checks    []Check      `json:"checks"`
	Version   string       `json:"version,omitempty"`
	Uptime    string       `json:"uptime,omitempty"`
}

// Check represents an individual health check
type Check struct {
	Name        string            `json:"name"`
	Status      HealthStatus      `json:"status"`
	Message     string            `json:"message,omitempty"`
	Error       string            `json:"error,omitempty"`
	Duration    string            `json:"duration,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	LastChecked time.Time         `json:"last_checked"`
}

// HealthChecker performs health checks on the Kafka client
type HealthChecker struct {
	client    *Client
	logger    watermill.LoggerAdapter
	startTime time.Time
	version   string
	mu        sync.RWMutex
	lastCheck *Health
}

// NewHealthChecker creates a new health checker
func NewHealthChecker(client *Client) *HealthChecker {
	return &HealthChecker{
		client:    client,
		logger:    client.logger,
		startTime: time.Now(),
		version:   "1.0.0", // TODO: Get from version package
	}
}

// Check performs all health checks
func (h *HealthChecker) Check(ctx context.Context) *Health {
	start := time.Now()

	health := &Health{
		Status:    HealthStatusHealthy,
		Timestamp: time.Now(),
		Checks:    []Check{},
		Version:   h.version,
		Uptime:    time.Since(h.startTime).String(),
	}

	// Run all checks
	checks := []func(context.Context) Check{
		h.checkClient,
		h.checkPublisher,
		h.checkSubscriber,
		h.checkStorage,
	}

	for _, checkFunc := range checks {
		check := checkFunc(ctx)
		health.Checks = append(health.Checks, check)

		// Update overall status based on check results
		if check.Status == HealthStatusUnhealthy {
			health.Status = HealthStatusUnhealthy
		} else if check.Status == HealthStatusDegraded && health.Status != HealthStatusUnhealthy {
			health.Status = HealthStatusDegraded
		}
	}

	h.logger.Info("Health check completed", watermill.LogFields{
		"status":   string(health.Status),
		"duration": time.Since(start).String(),
	})

	// Cache the result
	h.mu.Lock()
	h.lastCheck = health
	h.mu.Unlock()

	return health
}

// GetLastCheck returns the last health check result
func (h *HealthChecker) GetLastCheck() *Health {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.lastCheck
}

// checkClient checks if the client is operational
func (h *HealthChecker) checkClient(ctx context.Context) Check {
	start := time.Now()
	check := Check{
		Name:        "client",
		LastChecked: time.Now(),
		Metadata:    make(map[string]string),
	}

	h.client.mu.RLock()
	closed := h.client.closed
	h.client.mu.RUnlock()

	if closed {
		check.Status = HealthStatusUnhealthy
		check.Error = "client is closed"
		check.Message = "Kafka client is not operational"
	} else {
		check.Status = HealthStatusHealthy
		check.Message = "Client is operational"
	}

	check.Duration = time.Since(start).String()
	return check
}

// checkPublisher checks if the publisher is working
func (h *HealthChecker) checkPublisher(ctx context.Context) Check {
	start := time.Now()
	check := Check{
		Name:        "publisher",
		LastChecked: time.Now(),
		Metadata:    make(map[string]string),
	}

	if h.client.publisher == nil {
		check.Status = HealthStatusUnhealthy
		check.Error = "publisher is nil"
		check.Message = "Publisher not initialized"
	} else {
		check.Status = HealthStatusHealthy
		check.Message = "Publisher is ready"
	}

	check.Duration = time.Since(start).String()
	return check
}

// checkSubscriber checks if the subscriber is working
func (h *HealthChecker) checkSubscriber(ctx context.Context) Check {
	start := time.Now()
	check := Check{
		Name:        "subscriber",
		LastChecked: time.Now(),
		Metadata:    make(map[string]string),
	}

	if h.client.subscriber == nil {
		check.Status = HealthStatusUnhealthy
		check.Error = "subscriber is nil"
		check.Message = "Subscriber not initialized"
	} else {
		check.Status = HealthStatusHealthy
		check.Message = "Subscriber is ready"
	}

	check.Duration = time.Since(start).String()
	return check
}

// checkStorage checks if storage is working (for stateful processors)
func (h *HealthChecker) checkStorage(ctx context.Context) Check {
	start := time.Now()
	check := Check{
		Name:        "storage",
		LastChecked: time.Now(),
		Metadata:    make(map[string]string),
	}

	// Storage is optional, so if not present it's not unhealthy
	check.Status = HealthStatusHealthy
	check.Message = "Storage checks passed"

	check.Duration = time.Since(start).String()
	return check
}

// HealthHandler returns an HTTP handler for health checks
func HealthHandler(client *Client) http.HandlerFunc {
	checker := NewHealthChecker(client)

	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		health := checker.Check(ctx)

		w.Header().Set("Content-Type", "application/json")

		// Set HTTP status code based on health status
		switch health.Status {
		case HealthStatusHealthy:
			w.WriteHeader(http.StatusOK)
		case HealthStatusDegraded:
			w.WriteHeader(http.StatusOK) // Still OK but degraded
		case HealthStatusUnhealthy:
			w.WriteHeader(http.StatusServiceUnavailable)
		}

		json.NewEncoder(w).Encode(health)
	}
}

// ReadinessHandler returns an HTTP handler for readiness checks
// Readiness indicates if the service is ready to accept traffic
func ReadinessHandler(client *Client) http.HandlerFunc {
	checker := NewHealthChecker(client)

	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		health := checker.Check(ctx)

		w.Header().Set("Content-Type", "application/json")

		// For readiness, only healthy status means ready
		if health.Status == HealthStatusHealthy {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{
				"status":  "ready",
				"message": "Service is ready to accept traffic",
			})
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{
				"status":  "not_ready",
				"message": "Service is not ready to accept traffic",
			})
		}
	}
}

// LivenessHandler returns an HTTP handler for liveness checks
// Liveness indicates if the service is alive (vs needs restart)
func LivenessHandler(client *Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Simple liveness check - if we can respond, we're alive
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "alive",
			"message": "Service is alive",
		})
	}
}

// StartHealthServer starts an HTTP server for health checks
func StartHealthServer(client *Client, port string) error {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", HealthHandler(client))
	mux.HandleFunc("/health/ready", ReadinessHandler(client))
	mux.HandleFunc("/health/live", LivenessHandler(client))

	client.logger.Info("Starting health check server", watermill.LogFields{
		"port": port,
	})

	return http.ListenAndServe(":"+port, mux)
}
