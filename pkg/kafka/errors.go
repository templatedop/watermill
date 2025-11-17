package kafka

import "fmt"

// ErrInvalidConfig is returned when configuration is invalid
type ErrInvalidConfig struct {
	Field  string
	Reason string
}

func (e ErrInvalidConfig) Error() string {
	return fmt.Sprintf("invalid config field %s: %s", e.Field, e.Reason)
}

// ErrClientClosed is returned when operation is attempted on closed client
type ErrClientClosed struct{}

func (e ErrClientClosed) Error() string {
	return "client is closed"
}

// ErrPublishFailed is returned when message publishing fails
type ErrPublishFailed struct {
	Topic string
	Err   error
}

func (e ErrPublishFailed) Error() string {
	return fmt.Sprintf("failed to publish to topic %s: %v", e.Topic, e.Err)
}

// ErrSubscribeFailed is returned when subscription fails
type ErrSubscribeFailed struct {
	Topic string
	Err   error
}

func (e ErrSubscribeFailed) Error() string {
	return fmt.Sprintf("failed to subscribe to topic %s: %v", e.Topic, e.Err)
}

// ErrMaxRetriesExceeded is returned when maximum retries are exceeded
type ErrMaxRetriesExceeded struct {
	Attempts int
	LastErr  error
}

func (e ErrMaxRetriesExceeded) Error() string {
	return fmt.Sprintf("max retries exceeded after %d attempts: %v", e.Attempts, e.LastErr)
}
