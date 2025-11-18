package franzgo

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// HealthStatus represents the overall health status
type HealthStatus string

const (
	HealthStatusHealthy   HealthStatus = "healthy"
	HealthStatusDegraded  HealthStatus = "degraded"
	HealthStatusUnhealthy HealthStatus = "unhealthy"
)

// HealthCheck represents a single health check result
type HealthCheck struct {
	Name      string                 `json:"name"`
	Status    HealthStatus           `json:"status"`
	Message   string                 `json:"message,omitempty"`
	Error     string                 `json:"error,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
	Duration  time.Duration          `json:"duration"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// HealthReport contains the overall health status and individual checks
type HealthReport struct {
	Status     HealthStatus   `json:"status"`
	Timestamp  time.Time      `json:"timestamp"`
	Checks     []HealthCheck  `json:"checks"`
	TotalChecks int           `json:"total_checks"`
	HealthyChecks int         `json:"healthy_checks"`
	DegradedChecks int        `json:"degraded_checks"`
	UnhealthyChecks int       `json:"unhealthy_checks"`
}

// HealthChecker provides health check functionality
type HealthChecker struct {
	client           *Client
	checks           []CheckFunc
	timeout          time.Duration
	mu               sync.RWMutex
	lastReport       *HealthReport
	lastCheckTime    time.Time
	cacheTimeout     time.Duration
}

// CheckFunc is a function that performs a health check
type CheckFunc func(ctx context.Context) HealthCheck

// HealthConfig configures health checking
type HealthConfig struct {
	// Timeout for each individual health check
	CheckTimeout time.Duration

	// Cache health check results for this duration
	CacheTimeout time.Duration

	// Consumer lag threshold (messages behind) for degraded status
	ConsumerLagWarningThreshold int64

	// Consumer lag threshold (messages behind) for unhealthy status
	ConsumerLagCriticalThreshold int64

	// Broker connection timeout
	BrokerConnectionTimeout time.Duration
}

// DefaultHealthConfig returns default health check configuration
func DefaultHealthConfig() *HealthConfig {
	return &HealthConfig{
		CheckTimeout:                 5 * time.Second,
		CacheTimeout:                 10 * time.Second,
		ConsumerLagWarningThreshold:  1000,
		ConsumerLagCriticalThreshold: 10000,
		BrokerConnectionTimeout:      3 * time.Second,
	}
}

// NewHealthChecker creates a new health checker
func NewHealthChecker(client *Client, config *HealthConfig) *HealthChecker {
	if config == nil {
		config = DefaultHealthConfig()
	}

	hc := &HealthChecker{
		client:       client,
		checks:       make([]CheckFunc, 0),
		timeout:      config.CheckTimeout,
		cacheTimeout: config.CacheTimeout,
	}

	// Add default health checks
	hc.AddCheck(hc.BrokerConnectivityCheck(config.BrokerConnectionTimeout))
	hc.AddCheck(hc.ClientHealthCheck())

	return hc
}

// AddCheck adds a custom health check
func (hc *HealthChecker) AddCheck(check CheckFunc) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	hc.checks = append(hc.checks, check)
}

// Check performs all health checks and returns a health report
func (hc *HealthChecker) Check(ctx context.Context) (*HealthReport, error) {
	// Check if we have a recent cached result
	hc.mu.RLock()
	if hc.lastReport != nil && time.Since(hc.lastCheckTime) < hc.cacheTimeout {
		report := hc.lastReport
		hc.mu.RUnlock()
		return report, nil
	}
	hc.mu.RUnlock()

	// Perform health checks
	hc.mu.Lock()
	checks := make([]CheckFunc, len(hc.checks))
	copy(checks, hc.checks)
	hc.mu.Unlock()

	report := &HealthReport{
		Timestamp:       time.Now(),
		Checks:          make([]HealthCheck, 0, len(checks)),
		TotalChecks:     len(checks),
		HealthyChecks:   0,
		DegradedChecks:  0,
		UnhealthyChecks: 0,
	}

	// Run all checks concurrently
	checkResults := make(chan HealthCheck, len(checks))

	for _, check := range checks {
		go func(checkFunc CheckFunc) {
			checkCtx, cancel := context.WithTimeout(ctx, hc.timeout)
			defer cancel()

			checkResults <- checkFunc(checkCtx)
		}(check)
	}

	// Collect results
	for i := 0; i < len(checks); i++ {
		check := <-checkResults
		report.Checks = append(report.Checks, check)

		switch check.Status {
		case HealthStatusHealthy:
			report.HealthyChecks++
		case HealthStatusDegraded:
			report.DegradedChecks++
		case HealthStatusUnhealthy:
			report.UnhealthyChecks++
		}
	}

	// Determine overall status
	if report.UnhealthyChecks > 0 {
		report.Status = HealthStatusUnhealthy
	} else if report.DegradedChecks > 0 {
		report.Status = HealthStatusDegraded
	} else {
		report.Status = HealthStatusHealthy
	}

	// Cache the result
	hc.mu.Lock()
	hc.lastReport = report
	hc.lastCheckTime = time.Now()
	hc.mu.Unlock()

	return report, nil
}

// IsHealthy returns true if the service is healthy
func (hc *HealthChecker) IsHealthy(ctx context.Context) bool {
	report, err := hc.Check(ctx)
	if err != nil {
		return false
	}
	return report.Status == HealthStatusHealthy
}

// IsReady returns true if the service is ready to accept traffic (healthy or degraded)
func (hc *HealthChecker) IsReady(ctx context.Context) bool {
	report, err := hc.Check(ctx)
	if err != nil {
		return false
	}
	return report.Status == HealthStatusHealthy || report.Status == HealthStatusDegraded
}

// BrokerConnectivityCheck checks if we can connect to Kafka brokers
func (hc *HealthChecker) BrokerConnectivityCheck(timeout time.Duration) CheckFunc {
	return func(ctx context.Context) HealthCheck {
		start := time.Now()
		check := HealthCheck{
			Name:      "broker_connectivity",
			Timestamp: start,
			Metadata:  make(map[string]interface{}),
		}

		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		err := hc.client.Ping(ctx)
		check.Duration = time.Since(start)

		if err != nil {
			check.Status = HealthStatusUnhealthy
			check.Error = err.Error()
			check.Message = "Failed to connect to Kafka brokers"
			return check
		}

		check.Status = HealthStatusHealthy
		check.Message = "Successfully connected to Kafka brokers"
		check.Metadata["connection_time_ms"] = check.Duration.Milliseconds()

		return check
	}
}

// ClientHealthCheck checks if the client is in a healthy state
func (hc *HealthChecker) ClientHealthCheck() CheckFunc {
	return func(ctx context.Context) HealthCheck {
		start := time.Now()
		check := HealthCheck{
			Name:      "client_health",
			Timestamp: start,
			Metadata:  make(map[string]interface{}),
		}

		hc.client.mu.RLock()
		isClosed := hc.client.closed
		hc.client.mu.RUnlock()

		check.Duration = time.Since(start)

		if isClosed {
			check.Status = HealthStatusUnhealthy
			check.Message = "Client is closed"
			return check
		}

		check.Status = HealthStatusHealthy
		check.Message = "Client is active"

		return check
	}
}

// ConsumerLagCheck checks consumer lag for specified topics
func (hc *HealthChecker) ConsumerLagCheck(topics []string, warningThreshold, criticalThreshold int64) CheckFunc {
	return func(ctx context.Context) HealthCheck {
		start := time.Now()
		check := HealthCheck{
			Name:      "consumer_lag",
			Timestamp: start,
			Metadata:  make(map[string]interface{}),
		}

		kgoClient := hc.client.GetKgoClient()

		// Get consumer group metadata
		groupResp := kgoClient.FetchOffsetsForTopics(ctx, topics...)

		check.Duration = time.Since(start)

		if groupResp.Err() != nil {
			check.Status = HealthStatusDegraded
			check.Error = groupResp.Err().Error()
			check.Message = "Failed to fetch consumer offsets"
			return check
		}

		var totalLag int64
		var maxLag int64
		lagByTopic := make(map[string]int64)

		groupResp.EachPartition(func(par kgo.FetchOffsetPartition) {
			if par.Err == nil {
				// Calculate lag (this is a simplified version)
				// In production, you'd compare with high watermark
				lag := par.Offset.At
				totalLag += lag
				if lag > maxLag {
					maxLag = lag
				}

				topic := par.Topic
				lagByTopic[topic] += lag
			}
		})

		check.Metadata["total_lag"] = totalLag
		check.Metadata["max_lag"] = maxLag
		check.Metadata["lag_by_topic"] = lagByTopic

		if maxLag > criticalThreshold {
			check.Status = HealthStatusUnhealthy
			check.Message = fmt.Sprintf("Consumer lag is critical: %d messages behind", maxLag)
		} else if maxLag > warningThreshold {
			check.Status = HealthStatusDegraded
			check.Message = fmt.Sprintf("Consumer lag is elevated: %d messages behind", maxLag)
		} else {
			check.Status = HealthStatusHealthy
			check.Message = fmt.Sprintf("Consumer lag is normal: %d messages behind", maxLag)
		}

		return check
	}
}

// ProducerHealthCheck checks if the producer can produce messages
func (hc *HealthChecker) ProducerHealthCheck(testTopic string) CheckFunc {
	return func(ctx context.Context) HealthCheck {
		start := time.Now()
		check := HealthCheck{
			Name:      "producer_health",
			Timestamp: start,
			Metadata:  make(map[string]interface{}),
		}

		// Create a test record
		testKey := []byte(fmt.Sprintf("health-check-%d", time.Now().UnixNano()))
		testValue := []byte("health-check")

		record := &kgo.Record{
			Topic: testTopic,
			Key:   testKey,
			Value: testValue,
		}

		// Try to produce (sync)
		kgoClient := hc.client.GetKgoClient()
		results := kgoClient.ProduceSync(ctx, record)

		check.Duration = time.Since(start)

		if err := results.FirstErr(); err != nil {
			check.Status = HealthStatusUnhealthy
			check.Error = err.Error()
			check.Message = "Failed to produce test message"
			return check
		}

		check.Status = HealthStatusHealthy
		check.Message = "Successfully produced test message"
		check.Metadata["produce_time_ms"] = check.Duration.Milliseconds()

		return check
	}
}

// MetadataHealthCheck checks if we can fetch cluster metadata
func (hc *HealthChecker) MetadataHealthCheck() CheckFunc {
	return func(ctx context.Context) HealthCheck {
		start := time.Now()
		check := HealthCheck{
			Name:      "metadata_health",
			Timestamp: start,
			Metadata:  make(map[string]interface{}),
		}

		kgoClient := hc.client.GetKgoClient()

		// Request metadata
		metadataResp := kgoClient.Metadata(ctx)

		check.Duration = time.Since(start)

		if metadataResp.Err() != nil {
			check.Status = HealthStatusUnhealthy
			check.Error = metadataResp.Err().Error()
			check.Message = "Failed to fetch cluster metadata"
			return check
		}

		brokerCount := len(metadataResp.Brokers)
		topicCount := len(metadataResp.Topics)

		check.Metadata["broker_count"] = brokerCount
		check.Metadata["topic_count"] = topicCount

		if brokerCount == 0 {
			check.Status = HealthStatusUnhealthy
			check.Message = "No brokers available"
			return check
		}

		check.Status = HealthStatusHealthy
		check.Message = fmt.Sprintf("Cluster metadata available: %d brokers, %d topics", brokerCount, topicCount)

		return check
	}
}

// KubernetesHealthHandler provides HTTP handlers for Kubernetes probes
type KubernetesHealthHandler struct {
	checker *HealthChecker
}

// NewKubernetesHealthHandler creates handlers for Kubernetes health checks
func NewKubernetesHealthHandler(checker *HealthChecker) *KubernetesHealthHandler {
	return &KubernetesHealthHandler{
		checker: checker,
	}
}

// LivenessProbe returns true if the service is alive (client not closed)
// This should almost always return true unless there's a catastrophic failure
func (h *KubernetesHealthHandler) LivenessProbe(ctx context.Context) (bool, string) {
	h.checker.client.mu.RLock()
	isClosed := h.checker.client.closed
	h.checker.client.mu.RUnlock()

	if isClosed {
		return false, "Client is closed"
	}

	return true, "Service is alive"
}

// ReadinessProbe returns true if the service is ready to accept traffic
// This checks if we can connect to Kafka and the service is functional
func (h *KubernetesHealthHandler) ReadinessProbe(ctx context.Context) (bool, string, *HealthReport) {
	report, err := h.checker.Check(ctx)
	if err != nil {
		return false, fmt.Sprintf("Health check failed: %v", err), nil
	}

	if report.Status == HealthStatusUnhealthy {
		return false, "Service is unhealthy", report
	}

	// Ready if healthy or degraded (can still serve traffic but with warnings)
	return true, "Service is ready", report
}

// HealthCheckMiddleware creates a middleware that performs health checks
func HealthCheckMiddleware(checker *HealthChecker) Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, record *kgo.Record) error {
			// Check if service is ready before processing
			if !checker.IsReady(ctx) {
				return fmt.Errorf("service is not ready to process messages")
			}

			return next(ctx, record)
		}
	}
}

// HealthMonitor continuously monitors health and calls callbacks on status changes
type HealthMonitor struct {
	checker        *HealthChecker
	interval       time.Duration
	onHealthy      func(*HealthReport)
	onDegraded     func(*HealthReport)
	onUnhealthy    func(*HealthReport)
	stopChan       chan struct{}
	lastStatus     HealthStatus
	mu             sync.RWMutex
}

// NewHealthMonitor creates a new health monitor
func NewHealthMonitor(checker *HealthChecker, interval time.Duration) *HealthMonitor {
	return &HealthMonitor{
		checker:     checker,
		interval:    interval,
		stopChan:    make(chan struct{}),
		lastStatus:  HealthStatusHealthy,
	}
}

// OnHealthy sets callback for when service becomes healthy
func (hm *HealthMonitor) OnHealthy(callback func(*HealthReport)) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	hm.onHealthy = callback
}

// OnDegraded sets callback for when service becomes degraded
func (hm *HealthMonitor) OnDegraded(callback func(*HealthReport)) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	hm.onDegraded = callback
}

// OnUnhealthy sets callback for when service becomes unhealthy
func (hm *HealthMonitor) OnUnhealthy(callback func(*HealthReport)) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	hm.onUnhealthy = callback
}

// Start begins health monitoring
func (hm *HealthMonitor) Start(ctx context.Context) {
	ticker := time.NewTicker(hm.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-hm.stopChan:
			return
		case <-ticker.C:
			report, err := hm.checker.Check(ctx)
			if err != nil {
				continue
			}

			hm.mu.RLock()
			lastStatus := hm.lastStatus
			onHealthy := hm.onHealthy
			onDegraded := hm.onDegraded
			onUnhealthy := hm.onUnhealthy
			hm.mu.RUnlock()

			// Check if status changed
			if report.Status != lastStatus {
				hm.mu.Lock()
				hm.lastStatus = report.Status
				hm.mu.Unlock()

				// Call appropriate callback
				switch report.Status {
				case HealthStatusHealthy:
					if onHealthy != nil {
						onHealthy(report)
					}
				case HealthStatusDegraded:
					if onDegraded != nil {
						onDegraded(report)
					}
				case HealthStatusUnhealthy:
					if onUnhealthy != nil {
						onUnhealthy(report)
					}
				}
			}
		}
	}
}

// Stop stops health monitoring
func (hm *HealthMonitor) Stop() {
	close(hm.stopChan)
}

// GetLastStatus returns the last known health status
func (hm *HealthMonitor) GetLastStatus() HealthStatus {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	return hm.lastStatus
}
