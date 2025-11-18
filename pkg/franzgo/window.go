package franzgo

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// WindowType defines the type of window
type WindowType int

const (
	WindowTypeTumbling WindowType = iota
	WindowTypeSliding
	WindowTypeSession
)

// Window represents a time window
type Window struct {
	Start time.Time
	End   time.Time
	Key   string
}

// WindowFunc processes records in a window
type WindowFunc func(ctx *WindowContext) error

// WindowContext provides context for window processing
type WindowContext struct {
	ctx     context.Context
	Window  *Window
	Records []*kgo.Record
	emitter func(topic string, key, value []byte) error
}

// Emit emits a message from the window
func (wc *WindowContext) Emit(topic string, key []byte, payload interface{}) error {
	value, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to encode payload: %w", err)
	}

	return wc.emitter(topic, key, value)
}

// EmitRaw emits a raw message from the window
func (wc *WindowContext) EmitRaw(topic string, key, value []byte) error {
	return wc.emitter(topic, key, value)
}

// TumblingWindow implements tumbling time windows
type TumblingWindow struct {
	size        time.Duration
	processor   WindowFunc
	windows     map[string]*windowState
	mu          sync.RWMutex
	producer    *Producer
	outputTopic string
}

type windowState struct {
	window  *Window
	records []*kgo.Record
	timer   *time.Timer
}

// NewTumblingWindow creates a new tumbling window
func NewTumblingWindow(client *Client, size time.Duration, processor WindowFunc) *TumblingWindow {
	return &TumblingWindow{
		size:      size,
		processor: processor,
		windows:   make(map[string]*windowState),
		producer:  NewProducer(client),
	}
}

// SetOutputTopic sets the output topic for window results
func (tw *TumblingWindow) SetOutputTopic(topic string) {
	tw.outputTopic = topic
}

// Process processes a record in the tumbling window
func (tw *TumblingWindow) Process(ctx context.Context, key string, record *kgo.Record) error {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	now := time.Now()
	windowStart := now.Truncate(tw.size)
	windowEnd := windowStart.Add(tw.size)
	windowKey := fmt.Sprintf("%s:%d", key, windowStart.Unix())

	ws, exists := tw.windows[windowKey]
	if !exists {
		window := &Window{
			Start: windowStart,
			End:   windowEnd,
			Key:   key,
		}

		ws = &windowState{
			window:  window,
			records: []*kgo.Record{},
		}

		// Set timer to trigger window processing
		ws.timer = time.AfterFunc(time.Until(windowEnd), func() {
			tw.triggerWindow(windowKey)
		})

		tw.windows[windowKey] = ws
	}

	ws.records = append(ws.records, record)

	return nil
}

func (tw *TumblingWindow) triggerWindow(windowKey string) {
	tw.mu.Lock()
	ws, exists := tw.windows[windowKey]
	if !exists {
		tw.mu.Unlock()
		return
	}
	delete(tw.windows, windowKey)
	tw.mu.Unlock()

	// Process window
	wc := &WindowContext{
		ctx:     context.Background(),
		Window:  ws.window,
		Records: ws.records,
		emitter: func(topic string, key, value []byte) error {
			return tw.producer.Produce(context.Background(), topic, key, value)
		},
	}

	if err := tw.processor(wc); err != nil {
		// Handle error (log, DLQ, etc.)
		_ = err
	}
}

// SlidingWindow implements sliding time windows
type SlidingWindow struct {
	size        time.Duration
	slide       time.Duration
	processor   WindowFunc
	records     []*timestampedRecord
	mu          sync.RWMutex
	producer    *Producer
	outputTopic string
	ticker      *time.Ticker
	stopChan    chan struct{}
}

type timestampedRecord struct {
	timestamp time.Time
	key       string
	record    *kgo.Record
}

// NewSlidingWindow creates a new sliding window
func NewSlidingWindow(client *Client, size, slide time.Duration, processor WindowFunc) *SlidingWindow {
	sw := &SlidingWindow{
		size:      size,
		slide:     slide,
		processor: processor,
		records:   make([]*timestampedRecord, 0),
		producer:  NewProducer(client),
		stopChan:  make(chan struct{}),
	}

	sw.ticker = time.NewTicker(slide)
	go sw.runTicker()

	return sw
}

// SetOutputTopic sets the output topic for window results
func (sw *SlidingWindow) SetOutputTopic(topic string) {
	sw.outputTopic = topic
}

// Process processes a record in the sliding window
func (sw *SlidingWindow) Process(ctx context.Context, key string, record *kgo.Record) error {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	sw.records = append(sw.records, &timestampedRecord{
		timestamp: time.Now(),
		key:       key,
		record:    record,
	})

	return nil
}

func (sw *SlidingWindow) runTicker() {
	for {
		select {
		case <-sw.ticker.C:
			sw.processWindows()
		case <-sw.stopChan:
			return
		}
	}
}

func (sw *SlidingWindow) processWindows() {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := time.Now()
	windowStart := now.Add(-sw.size)

	// Group records by key
	keyRecords := make(map[string][]*kgo.Record)
	validRecords := make([]*timestampedRecord, 0)

	for _, tr := range sw.records {
		if tr.timestamp.After(windowStart) {
			keyRecords[tr.key] = append(keyRecords[tr.key], tr.record)
			validRecords = append(validRecords, tr)
		}
	}

	// Update records list (remove old ones)
	sw.records = validRecords

	// Process windows for each key
	for key, recs := range keyRecords {
		window := &Window{
			Start: windowStart,
			End:   now,
			Key:   key,
		}

		wc := &WindowContext{
			ctx:     context.Background(),
			Window:  window,
			Records: recs,
			emitter: func(topic string, k, value []byte) error {
				return sw.producer.Produce(context.Background(), topic, k, value)
			},
		}

		if err := sw.processor(wc); err != nil {
			// Handle error
			_ = err
		}
	}
}

// Stop stops the sliding window
func (sw *SlidingWindow) Stop() {
	sw.ticker.Stop()
	close(sw.stopChan)
}

// SessionWindow implements session windows with inactivity gaps
type SessionWindow struct {
	gap         time.Duration
	processor   WindowFunc
	sessions    map[string]*sessionState
	mu          sync.RWMutex
	producer    *Producer
	outputTopic string
}

type sessionState struct {
	window       *Window
	records      []*kgo.Record
	lastActivity time.Time
	timer        *time.Timer
}

// NewSessionWindow creates a new session window
func NewSessionWindow(client *Client, gap time.Duration, processor WindowFunc) *SessionWindow {
	return &SessionWindow{
		gap:       gap,
		processor: processor,
		sessions:  make(map[string]*sessionState),
		producer:  NewProducer(client),
	}
}

// SetOutputTopic sets the output topic for window results
func (sw *SessionWindow) SetOutputTopic(topic string) {
	sw.outputTopic = topic
}

// Process processes a record in the session window
func (sw *SessionWindow) Process(ctx context.Context, key string, record *kgo.Record) error {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := time.Now()

	session, exists := sw.sessions[key]
	if !exists {
		// Start new session
		session = &sessionState{
			window: &Window{
				Start: now,
				Key:   key,
			},
			records:      []*kgo.Record{},
			lastActivity: now,
		}

		session.timer = time.AfterFunc(sw.gap, func() {
			sw.closeSession(key)
		})

		sw.sessions[key] = session
	} else {
		// Reset timer
		session.timer.Stop()
		session.timer = time.AfterFunc(sw.gap, func() {
			sw.closeSession(key)
		})
		session.lastActivity = now
	}

	session.records = append(session.records, record)

	return nil
}

func (sw *SessionWindow) closeSession(key string) {
	sw.mu.Lock()
	session, exists := sw.sessions[key]
	if !exists {
		sw.mu.Unlock()
		return
	}
	delete(sw.sessions, key)
	sw.mu.Unlock()

	// Close the window
	session.window.End = session.lastActivity

	// Process session
	wc := &WindowContext{
		ctx:     context.Background(),
		Window:  session.window,
		Records: session.records,
		emitter: func(topic string, k, value []byte) error {
			return sw.producer.Produce(context.Background(), topic, k, value)
		},
	}

	if err := sw.processor(wc); err != nil {
		// Handle error
		_ = err
	}
}

// WindowAggregator provides common aggregation functions for windows
type WindowAggregator struct{}

// Count returns the count of records in a window
func (wa *WindowAggregator) Count(wc *WindowContext) (int, error) {
	return len(wc.Records), nil
}

// Sum sums a numeric field from all records
func (wa *WindowAggregator) Sum(wc *WindowContext, field string) (float64, error) {
	sum := 0.0

	for _, record := range wc.Records {
		var data map[string]interface{}
		if err := json.Unmarshal(record.Value, &data); err != nil {
			continue
		}

		if val, ok := data[field].(float64); ok {
			sum += val
		}
	}

	return sum, nil
}

// Average calculates the average of a numeric field
func (wa *WindowAggregator) Average(wc *WindowContext, field string) (float64, error) {
	sum, err := wa.Sum(wc, field)
	if err != nil {
		return 0, err
	}

	count, _ := wa.Count(wc)
	if count == 0 {
		return 0, nil
	}

	return sum / float64(count), nil
}

// Min finds the minimum value of a numeric field
func (wa *WindowAggregator) Min(wc *WindowContext, field string) (float64, error) {
	min := float64(0)
	found := false

	for _, record := range wc.Records {
		var data map[string]interface{}
		if err := json.Unmarshal(record.Value, &data); err != nil {
			continue
		}

		if val, ok := data[field].(float64); ok {
			if !found || val < min {
				min = val
				found = true
			}
		}
	}

	if !found {
		return 0, fmt.Errorf("no valid values found")
	}

	return min, nil
}

// Max finds the maximum value of a numeric field
func (wa *WindowAggregator) Max(wc *WindowContext, field string) (float64, error) {
	max := float64(0)
	found := false

	for _, record := range wc.Records {
		var data map[string]interface{}
		if err := json.Unmarshal(record.Value, &data); err != nil {
			continue
		}

		if val, ok := data[field].(float64); ok {
			if !found || val > max {
				max = val
				found = true
			}
		}
	}

	if !found {
		return 0, fmt.Errorf("no valid values found")
	}

	return max, nil
}

// Collect collects all values of a field into a slice
func (wa *WindowAggregator) Collect(wc *WindowContext, field string) ([]interface{}, error) {
	values := make([]interface{}, 0, len(wc.Records))

	for _, record := range wc.Records {
		var data map[string]interface{}
		if err := json.Unmarshal(record.Value, &data); err != nil {
			continue
		}

		if val, ok := data[field]; ok {
			values = append(values, val)
		}
	}

	return values, nil
}

// GroupBy groups records by a field value
func (wa *WindowAggregator) GroupBy(wc *WindowContext, field string) (map[interface{}][]*kgo.Record, error) {
	groups := make(map[interface{}][]*kgo.Record)

	for _, record := range wc.Records {
		var data map[string]interface{}
		if err := json.Unmarshal(record.Value, &data); err != nil {
			continue
		}

		if val, ok := data[field]; ok {
			groups[val] = append(groups[val], record)
		}
	}

	return groups, nil
}

// WindowedAggregation is a helper for common windowed aggregation patterns
type WindowedAggregation struct {
	client      *Client
	consumer    *Consumer
	window      interface{} // TumblingWindow, SlidingWindow, or SessionWindow
	outputTopic string
}

// NewWindowedAggregation creates a new windowed aggregation
func NewWindowedAggregation(client *Client, windowType WindowType, size time.Duration, processor WindowFunc) (*WindowedAggregation, error) {
	var window interface{}

	switch windowType {
	case WindowTypeTumbling:
		window = NewTumblingWindow(client, size, processor)
	case WindowTypeSliding:
		// For sliding windows, use slide = size/2 by default
		window = NewSlidingWindow(client, size, size/2, processor)
	case WindowTypeSession:
		// For session windows, gap = size
		window = NewSessionWindow(client, size, processor)
	default:
		return nil, fmt.Errorf("unsupported window type: %d", windowType)
	}

	return &WindowedAggregation{
		client:   client,
		consumer: NewConsumer(client),
		window:   window,
	}, nil
}

// SetOutputTopic sets the output topic
func (wa *WindowedAggregation) SetOutputTopic(topic string) {
	wa.outputTopic = topic

	switch w := wa.window.(type) {
	case *TumblingWindow:
		w.SetOutputTopic(topic)
	case *SlidingWindow:
		w.SetOutputTopic(topic)
	case *SessionWindow:
		w.SetOutputTopic(topic)
	}
}

// Run starts the windowed aggregation
func (wa *WindowedAggregation) Run(ctx context.Context, inputTopic string, keyExtractor func(*kgo.Record) string) error {
	return wa.consumer.Consume(ctx, []string{inputTopic}, func(ctx context.Context, record *kgo.Record) error {
		key := keyExtractor(record)

		switch w := wa.window.(type) {
		case *TumblingWindow:
			return w.Process(ctx, key, record)
		case *SlidingWindow:
			return w.Process(ctx, key, record)
		case *SessionWindow:
			return w.Process(ctx, key, record)
		default:
			return fmt.Errorf("unsupported window type")
		}
	})
}

// Stop stops the windowed aggregation
func (wa *WindowedAggregation) Stop() {
	if sw, ok := wa.window.(*SlidingWindow); ok {
		sw.Stop()
	}
}
