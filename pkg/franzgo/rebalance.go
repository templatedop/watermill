package franzgo

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// RebalanceEvent represents a rebalance event
type RebalanceEvent struct {
	Type              RebalanceEventType
	Timestamp         time.Time
	AssignedPartitions map[string][]int32 // topic -> partitions
	RevokedPartitions  map[string][]int32 // topic -> partitions
	GroupID           string
	MemberID          string
	GenerationID      int32
	Error             error
}

// RebalanceEventType represents the type of rebalance event
type RebalanceEventType int

const (
	RebalanceAssigned RebalanceEventType = iota
	RebalanceRevoked
	RebalanceLost
)

func (ret RebalanceEventType) String() string {
	switch ret {
	case RebalanceAssigned:
		return "assigned"
	case RebalanceRevoked:
		return "revoked"
	case RebalanceLost:
		return "lost"
	default:
		return "unknown"
	}
}

// RebalanceListener listens for rebalance events
type RebalanceListener interface {
	// OnPartitionsAssigned is called when partitions are assigned
	OnPartitionsAssigned(ctx context.Context, event *RebalanceEvent) error

	// OnPartitionsRevoked is called when partitions are about to be revoked
	OnPartitionsRevoked(ctx context.Context, event *RebalanceEvent) error

	// OnPartitionsLost is called when partitions are lost (unclean shutdown)
	OnPartitionsLost(ctx context.Context, event *RebalanceEvent) error
}

// RebalanceCallback is a function that handles rebalance events
type RebalanceCallback func(ctx context.Context, event *RebalanceEvent) error

// FunctionalRebalanceListener implements RebalanceListener using callbacks
type FunctionalRebalanceListener struct {
	onAssigned RebalanceCallback
	onRevoked  RebalanceCallback
	onLost     RebalanceCallback
}

// NewFunctionalRebalanceListener creates a listener from callbacks
func NewFunctionalRebalanceListener() *FunctionalRebalanceListener {
	return &FunctionalRebalanceListener{}
}

// OnAssigned sets the assignment callback
func (frl *FunctionalRebalanceListener) OnAssigned(callback RebalanceCallback) *FunctionalRebalanceListener {
	frl.onAssigned = callback
	return frl
}

// OnRevoked sets the revocation callback
func (frl *FunctionalRebalanceListener) OnRevoked(callback RebalanceCallback) *FunctionalRebalanceListener {
	frl.onRevoked = callback
	return frl
}

// OnLost sets the lost callback
func (frl *FunctionalRebalanceListener) OnLost(callback RebalanceCallback) *FunctionalRebalanceListener {
	frl.onLost = callback
	return frl
}

func (frl *FunctionalRebalanceListener) OnPartitionsAssigned(ctx context.Context, event *RebalanceEvent) error {
	if frl.onAssigned != nil {
		return frl.onAssigned(ctx, event)
	}
	return nil
}

func (frl *FunctionalRebalanceListener) OnPartitionsRevoked(ctx context.Context, event *RebalanceEvent) error {
	if frl.onRevoked != nil {
		return frl.onRevoked(ctx, event)
	}
	return nil
}

func (frl *FunctionalRebalanceListener) OnPartitionsLost(ctx context.Context, event *RebalanceEvent) error {
	if frl.onLost != nil {
		return frl.onLost(ctx, event)
	}
	return nil
}

// LoggingRebalanceListener logs rebalance events
type LoggingRebalanceListener struct {
	logger func(format string, args ...interface{})
}

// NewLoggingRebalanceListener creates a logging listener
func NewLoggingRebalanceListener(logger func(format string, args ...interface{})) *LoggingRebalanceListener {
	return &LoggingRebalanceListener{logger: logger}
}

func (lrl *LoggingRebalanceListener) OnPartitionsAssigned(ctx context.Context, event *RebalanceEvent) error {
	lrl.logger("Partitions assigned: group=%s, member=%s, generation=%d, partitions=%v",
		event.GroupID, event.MemberID, event.GenerationID, event.AssignedPartitions)
	return nil
}

func (lrl *LoggingRebalanceListener) OnPartitionsRevoked(ctx context.Context, event *RebalanceEvent) error {
	lrl.logger("Partitions revoked: group=%s, member=%s, partitions=%v",
		event.GroupID, event.MemberID, event.RevokedPartitions)
	return nil
}

func (lrl *LoggingRebalanceListener) OnPartitionsLost(ctx context.Context, event *RebalanceEvent) error {
	lrl.logger("Partitions lost: group=%s, member=%s, partitions=%v",
		event.GroupID, event.MemberID, event.RevokedPartitions)
	return nil
}

// MetricsRebalanceListener tracks rebalance metrics
type MetricsRebalanceListener struct {
	assignedCount   int64
	revokedCount    int64
	lostCount       int64
	lastRebalance   time.Time
	rebalanceDurations []time.Duration
	mu              sync.RWMutex
}

// NewMetricsRebalanceListener creates a metrics listener
func NewMetricsRebalanceListener() *MetricsRebalanceListener {
	return &MetricsRebalanceListener{
		rebalanceDurations: make([]time.Duration, 0),
	}
}

func (mrl *MetricsRebalanceListener) OnPartitionsAssigned(ctx context.Context, event *RebalanceEvent) error {
	mrl.mu.Lock()
	defer mrl.mu.Unlock()

	mrl.assignedCount++
	mrl.lastRebalance = event.Timestamp

	if len(mrl.rebalanceDurations) > 0 {
		duration := event.Timestamp.Sub(mrl.lastRebalance)
		mrl.rebalanceDurations = append(mrl.rebalanceDurations, duration)
	}

	return nil
}

func (mrl *MetricsRebalanceListener) OnPartitionsRevoked(ctx context.Context, event *RebalanceEvent) error {
	mrl.mu.Lock()
	defer mrl.mu.Unlock()
	mrl.revokedCount++
	return nil
}

func (mrl *MetricsRebalanceListener) OnPartitionsLost(ctx context.Context, event *RebalanceEvent) error {
	mrl.mu.Lock()
	defer mrl.mu.Unlock()
	mrl.lostCount++
	return nil
}

// GetMetrics returns rebalance metrics
func (mrl *MetricsRebalanceListener) GetMetrics() (assigned, revoked, lost int64, lastRebalance time.Time) {
	mrl.mu.RLock()
	defer mrl.mu.RUnlock()
	return mrl.assignedCount, mrl.revokedCount, mrl.lostCount, mrl.lastRebalance
}

// StateAwareRebalanceListener integrates with stateful processing
type StateAwareRebalanceListener struct {
	stateStores map[int32]StateStore
	onAssigned  func(partition int32, store StateStore) error
	onRevoked   func(partition int32, store StateStore) error
	mu          sync.RWMutex
}

// NewStateAwareRebalanceListener creates a state-aware listener
func NewStateAwareRebalanceListener() *StateAwareRebalanceListener {
	return &StateAwareRebalanceListener{
		stateStores: make(map[int32]StateStore),
	}
}

// RegisterStateStore registers a state store for a partition
func (sarl *StateAwareRebalanceListener) RegisterStateStore(partition int32, store StateStore) {
	sarl.mu.Lock()
	defer sarl.mu.Unlock()
	sarl.stateStores[partition] = store
}

// SetOnAssigned sets callback for partition assignment
func (sarl *StateAwareRebalanceListener) SetOnAssigned(callback func(partition int32, store StateStore) error) {
	sarl.onAssigned = callback
}

// SetOnRevoked sets callback for partition revocation
func (sarl *StateAwareRebalanceListener) SetOnRevoked(callback func(partition int32, store StateStore) error) {
	sarl.onRevoked = callback
}

func (sarl *StateAwareRebalanceListener) OnPartitionsAssigned(ctx context.Context, event *RebalanceEvent) error {
	if sarl.onAssigned == nil {
		return nil
	}

	sarl.mu.RLock()
	defer sarl.mu.RUnlock()

	for _, partitions := range event.AssignedPartitions {
		for _, partition := range partitions {
			if store, exists := sarl.stateStores[partition]; exists {
				if err := sarl.onAssigned(partition, store); err != nil {
					return fmt.Errorf("partition %d assignment callback failed: %w", partition, err)
				}
			}
		}
	}

	return nil
}

func (sarl *StateAwareRebalanceListener) OnPartitionsRevoked(ctx context.Context, event *RebalanceEvent) error {
	if sarl.onRevoked == nil {
		return nil
	}

	sarl.mu.RLock()
	defer sarl.mu.RUnlock()

	for _, partitions := range event.RevokedPartitions {
		for _, partition := range partitions {
			if store, exists := sarl.stateStores[partition]; exists {
				if err := sarl.onRevoked(partition, store); err != nil {
					return fmt.Errorf("partition %d revocation callback failed: %w", partition, err)
				}
			}
		}
	}

	return nil
}

func (sarl *StateAwareRebalanceListener) OnPartitionsLost(ctx context.Context, event *RebalanceEvent) error {
	// Same as revoked for state cleanup
	return sarl.OnPartitionsRevoked(ctx, event)
}

// CompositeRebalanceListener chains multiple listeners
type CompositeRebalanceListener struct {
	listeners []RebalanceListener
}

// NewCompositeRebalanceListener creates a composite listener
func NewCompositeRebalanceListener(listeners ...RebalanceListener) *CompositeRebalanceListener {
	return &CompositeRebalanceListener{
		listeners: listeners,
	}
}

// AddListener adds a listener to the chain
func (crl *CompositeRebalanceListener) AddListener(listener RebalanceListener) {
	crl.listeners = append(crl.listeners, listener)
}

func (crl *CompositeRebalanceListener) OnPartitionsAssigned(ctx context.Context, event *RebalanceEvent) error {
	for _, listener := range crl.listeners {
		if err := listener.OnPartitionsAssigned(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (crl *CompositeRebalanceListener) OnPartitionsRevoked(ctx context.Context, event *RebalanceEvent) error {
	for _, listener := range crl.listeners {
		if err := listener.OnPartitionsRevoked(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (crl *CompositeRebalanceListener) OnPartitionsLost(ctx context.Context, event *RebalanceEvent) error {
	for _, listener := range crl.listeners {
		if err := listener.OnPartitionsLost(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

// ConsumerWithRebalanceListener wraps a consumer with rebalance listener
type ConsumerWithRebalanceListener struct {
	*Consumer
	listener RebalanceListener
}

// NewConsumerWithRebalanceListener creates a consumer with rebalance listener
func NewConsumerWithRebalanceListener(consumer *Consumer, listener RebalanceListener) *ConsumerWithRebalanceListener {
	return &ConsumerWithRebalanceListener{
		Consumer: consumer,
		listener: listener,
	}
}

// ConsumeWithRebalance consumes messages and triggers rebalance callbacks
func (cwrl *ConsumerWithRebalanceListener) ConsumeWithRebalance(ctx context.Context, topics []string, handler HandlerFunc) error {
	// Set up franz-go rebalance hooks
	kgoClient := cwrl.client.GetKgoClient()

	// Track assigned partitions
	var currentAssignment map[string][]int32
	var mu sync.Mutex

	// OnPartitionsAssigned hook
	kgoClient.AssignPartitions(func(ctx context.Context, c *kgo.Client, assigned map[string][]int32) {
		mu.Lock()
		defer mu.Unlock()

		event := &RebalanceEvent{
			Type:               RebalanceAssigned,
			Timestamp:          time.Now(),
			AssignedPartitions: assigned,
			GroupID:            cwrl.client.config.ConsumerGroup,
		}

		if err := cwrl.listener.OnPartitionsAssigned(ctx, event); err != nil {
			// Log error but continue
			fmt.Printf("OnPartitionsAssigned error: %v\n", err)
		}

		currentAssignment = assigned
	})

	// OnPartitionsRevoked hook
	kgoClient.OnPartitionsRevoked(func(ctx context.Context, c *kgo.Client, revoked map[string][]int32) {
		mu.Lock()
		defer mu.Unlock()

		event := &RebalanceEvent{
			Type:              RebalanceRevoked,
			Timestamp:         time.Now(),
			RevokedPartitions: revoked,
			GroupID:           cwrl.client.config.ConsumerGroup,
		}

		if err := cwrl.listener.OnPartitionsRevoked(ctx, event); err != nil {
			// Log error but continue
			fmt.Printf("OnPartitionsRevoked error: %v\n", err)
		}
	})

	// OnPartitionsLost hook
	kgoClient.OnPartitionsLost(func(ctx context.Context, c *kgo.Client, lost map[string][]int32) {
		mu.Lock()
		defer mu.Unlock()

		event := &RebalanceEvent{
			Type:              RebalanceLost,
			Timestamp:         time.Now(),
			RevokedPartitions: lost,
			GroupID:           cwrl.client.config.ConsumerGroup,
		}

		if err := cwrl.listener.OnPartitionsLost(ctx, event); err != nil {
			// Log error but continue
			fmt.Printf("OnPartitionsLost error: %v\n", err)
		}
	})

	// Start normal consumption
	return cwrl.Consumer.Consume(ctx, topics, handler)
}

// OffsetCommittingRebalanceListener commits offsets on rebalance
type OffsetCommittingRebalanceListener struct {
	client *Client
}

// NewOffsetCommittingRebalanceListener creates an offset-committing listener
func NewOffsetCommittingRebalanceListener(client *Client) *OffsetCommittingRebalanceListener {
	return &OffsetCommittingRebalanceListener{
		client: client,
	}
}

func (ocrl *OffsetCommittingRebalanceListener) OnPartitionsAssigned(ctx context.Context, event *RebalanceEvent) error {
	// Nothing to do on assignment
	return nil
}

func (ocrl *OffsetCommittingRebalanceListener) OnPartitionsRevoked(ctx context.Context, event *RebalanceEvent) error {
	// Commit offsets before revocation
	kgoClient := ocrl.client.GetKgoClient()

	offsets := kgoClient.UncommittedOffsets()
	if len(offsets) == 0 {
		return nil
	}

	commitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := kgoClient.CommitUncommittedOffsets(commitCtx); err != nil {
		return fmt.Errorf("failed to commit offsets on revoke: %w", err)
	}

	return nil
}

func (ocrl *OffsetCommittingRebalanceListener) OnPartitionsLost(ctx context.Context, event *RebalanceEvent) error {
	// Can't commit on lost partitions
	return nil
}

// NotificationRebalanceListener sends notifications on rebalance
type NotificationRebalanceListener struct {
	notifyChan chan<- *RebalanceEvent
}

// NewNotificationRebalanceListener creates a notification listener
func NewNotificationRebalanceListener(notifyChan chan<- *RebalanceEvent) *NotificationRebalanceListener {
	return &NotificationRebalanceListener{
		notifyChan: notifyChan,
	}
}

func (nrl *NotificationRebalanceListener) OnPartitionsAssigned(ctx context.Context, event *RebalanceEvent) error {
	select {
	case nrl.notifyChan <- event:
	default:
		// Non-blocking send
	}
	return nil
}

func (nrl *NotificationRebalanceListener) OnPartitionsRevoked(ctx context.Context, event *RebalanceEvent) error {
	select {
	case nrl.notifyChan <- event:
	default:
	}
	return nil
}

func (nrl *NotificationRebalanceListener) OnPartitionsLost(ctx context.Context, event *RebalanceEvent) error {
	select {
	case nrl.notifyChan <- event:
	default:
	}
	return nil
}

// RetryRebalanceListener retries rebalance callbacks with backoff
type RetryRebalanceListener struct {
	delegate   RebalanceListener
	maxRetries int
	retryDelay time.Duration
}

// NewRetryRebalanceListener creates a retry listener
func NewRetryRebalanceListener(delegate RebalanceListener, maxRetries int, retryDelay time.Duration) *RetryRebalanceListener {
	return &RetryRebalanceListener{
		delegate:   delegate,
		maxRetries: maxRetries,
		retryDelay: retryDelay,
	}
}

func (rrl *RetryRebalanceListener) retry(fn func() error) error {
	var lastErr error
	for i := 0; i <= rrl.maxRetries; i++ {
		if err := fn(); err != nil {
			lastErr = err
			if i < rrl.maxRetries {
				time.Sleep(rrl.retryDelay * time.Duration(i+1))
			}
		} else {
			return nil
		}
	}
	return lastErr
}

func (rrl *RetryRebalanceListener) OnPartitionsAssigned(ctx context.Context, event *RebalanceEvent) error {
	return rrl.retry(func() error {
		return rrl.delegate.OnPartitionsAssigned(ctx, event)
	})
}

func (rrl *RetryRebalanceListener) OnPartitionsRevoked(ctx context.Context, event *RebalanceEvent) error {
	return rrl.retry(func() error {
		return rrl.delegate.OnPartitionsRevoked(ctx, event)
	})
}

func (rrl *RetryRebalanceListener) OnPartitionsLost(ctx context.Context, event *RebalanceEvent) error {
	return rrl.retry(func() error {
		return rrl.delegate.OnPartitionsLost(ctx, event)
	})
}
