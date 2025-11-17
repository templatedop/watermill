package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
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

// WindowFunc processes messages in a window
type WindowFunc func(ctx *WindowContext) error

// WindowContext provides context for window processing
type WindowContext struct {
	ctx      context.Context
	Window   *Window
	Messages []*message.Message
	State    Storage
	emitter  func(topic string, msg *message.Message) error
}

// Emit emits a message from the window
func (wc *WindowContext) Emit(topic string, payload interface{}) error {
	msg, err := encodeMessage(payload)
	if err != nil {
		return fmt.Errorf("failed to encode message: %w", err)
	}

	return wc.emitter(topic, msg)
}

// GetState retrieves state for a key
func (wc *WindowContext) GetState(key string) (interface{}, error) {
	data, err := wc.State.Get(key)
	if err != nil {
		return nil, err
	}

	codec := &JSONCodec{}
	var value interface{}
	if err := codec.Decode(data, &value); err != nil {
		return nil, err
	}

	return value, nil
}

// SetState sets state for a key
func (wc *WindowContext) SetState(key string, value interface{}) error {
	codec := &JSONCodec{}
	data, err := codec.Encode(value)
	if err != nil {
		return err
	}

	return wc.State.Set(key, data)
}

// TumblingWindow implements tumbling time windows
type TumblingWindow struct {
	size         time.Duration
	processor    WindowFunc
	windows      map[string]*windowState
	mu           sync.RWMutex
	storage      Storage
	outputTopic  string
	emitter      func(topic string, msg *message.Message) error
}

type windowState struct {
	window   *Window
	messages []*message.Message
	timer    *time.Timer
}

// NewTumblingWindow creates a new tumbling window
func NewTumblingWindow(size time.Duration, processor WindowFunc) *TumblingWindow {
	return &TumblingWindow{
		size:      size,
		processor: processor,
		windows:   make(map[string]*windowState),
		storage:   NewInMemoryStorage(),
	}
}

// SetOutputTopic sets the output topic for window results
func (tw *TumblingWindow) SetOutputTopic(topic string) {
	tw.outputTopic = topic
}

// SetEmitter sets the message emitter function
func (tw *TumblingWindow) SetEmitter(emitter func(topic string, msg *message.Message) error) {
	tw.emitter = emitter
}

// Process processes a message in the tumbling window
func (tw *TumblingWindow) Process(ctx context.Context, key string, msg *message.Message) error {
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
			window:   window,
			messages: []*message.Message{},
		}

		// Set timer to trigger window processing
		ws.timer = time.AfterFunc(time.Until(windowEnd), func() {
			tw.triggerWindow(windowKey)
		})

		tw.windows[windowKey] = ws
	}

	ws.messages = append(ws.messages, msg)

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
		ctx:      context.Background(),
		Window:   ws.window,
		Messages: ws.messages,
		State:    tw.storage,
		emitter:  tw.emitter,
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
	messages    []*timestampedMessage
	mu          sync.RWMutex
	storage     Storage
	outputTopic string
	emitter     func(topic string, msg *message.Message) error
	ticker      *time.Ticker
	stopChan    chan struct{}
}

type timestampedMessage struct {
	timestamp time.Time
	key       string
	message   *message.Message
}

// NewSlidingWindow creates a new sliding window
func NewSlidingWindow(size, slide time.Duration, processor WindowFunc) *SlidingWindow {
	sw := &SlidingWindow{
		size:      size,
		slide:     slide,
		processor: processor,
		messages:  make([]*timestampedMessage, 0),
		storage:   NewInMemoryStorage(),
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

// SetEmitter sets the message emitter function
func (sw *SlidingWindow) SetEmitter(emitter func(topic string, msg *message.Message) error) {
	sw.emitter = emitter
}

// Process processes a message in the sliding window
func (sw *SlidingWindow) Process(ctx context.Context, key string, msg *message.Message) error {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	sw.messages = append(sw.messages, &timestampedMessage{
		timestamp: time.Now(),
		key:       key,
		message:   msg,
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

	// Group messages by key
	keyMessages := make(map[string][]*message.Message)
	validMessages := make([]*timestampedMessage, 0)

	for _, tm := range sw.messages {
		if tm.timestamp.After(windowStart) {
			keyMessages[tm.key] = append(keyMessages[tm.key], tm.message)
			validMessages = append(validMessages, tm)
		}
	}

	// Update messages list (remove old ones)
	sw.messages = validMessages

	// Process windows for each key
	for key, msgs := range keyMessages {
		window := &Window{
			Start: windowStart,
			End:   now,
			Key:   key,
		}

		wc := &WindowContext{
			ctx:      context.Background(),
			Window:   window,
			Messages: msgs,
			State:    sw.storage,
			emitter:  sw.emitter,
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
	gap          time.Duration
	processor    WindowFunc
	sessions     map[string]*sessionState
	mu           sync.RWMutex
	storage      Storage
	outputTopic  string
	emitter      func(topic string, msg *message.Message) error
}

type sessionState struct {
	window       *Window
	messages     []*message.Message
	lastActivity time.Time
	timer        *time.Timer
}

// NewSessionWindow creates a new session window
func NewSessionWindow(gap time.Duration, processor WindowFunc) *SessionWindow {
	return &SessionWindow{
		gap:       gap,
		processor: processor,
		sessions:  make(map[string]*sessionState),
		storage:   NewInMemoryStorage(),
	}
}

// SetOutputTopic sets the output topic for window results
func (sw *SessionWindow) SetOutputTopic(topic string) {
	sw.outputTopic = topic
}

// SetEmitter sets the message emitter function
func (sw *SessionWindow) SetEmitter(emitter func(topic string, msg *message.Message) error) {
	sw.emitter = emitter
}

// Process processes a message in the session window
func (sw *SessionWindow) Process(ctx context.Context, key string, msg *message.Message) error {
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
			messages:     []*message.Message{},
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

	session.messages = append(session.messages, msg)

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
		ctx:      context.Background(),
		Window:   session.window,
		Messages: session.messages,
		State:    sw.storage,
		emitter:  sw.emitter,
	}

	if err := sw.processor(wc); err != nil {
		// Handle error
		_ = err
	}
}

// WindowAggregator provides common aggregation functions for windows
type WindowAggregator struct{}

// Count returns the count of messages in a window
func (wa *WindowAggregator) Count(wc *WindowContext) (int, error) {
	return len(wc.Messages), nil
}

// Sum sums a numeric field from all messages
func (wa *WindowAggregator) Sum(wc *WindowContext, field string) (float64, error) {
	codec := &JSONCodec{}
	sum := 0.0

	for _, msg := range wc.Messages {
		var data map[string]interface{}
		if err := codec.Decode(msg.Payload, &data); err != nil {
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
	codec := &JSONCodec{}
	min := float64(0)
	found := false

	for _, msg := range wc.Messages {
		var data map[string]interface{}
		if err := codec.Decode(msg.Payload, &data); err != nil {
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
	codec := &JSONCodec{}
	max := float64(0)
	found := false

	for _, msg := range wc.Messages {
		var data map[string]interface{}
		if err := codec.Decode(msg.Payload, &data); err != nil {
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
