package kafka

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
)

// MessageHelper provides helper methods for message metadata
type MessageHelper struct{}

// NewMessageHelper creates a new message helper
func NewMessageHelper() *MessageHelper {
	return &MessageHelper{}
}

// ============================================================================
// Correlation ID Support
// ============================================================================

// SetCorrelationID sets the correlation ID for request tracing
func (h *MessageHelper) SetCorrelationID(msg *message.Message, correlationID string) {
	msg.Metadata.Set("correlation_id", correlationID)
}

// GetCorrelationID retrieves the correlation ID
func (h *MessageHelper) GetCorrelationID(msg *message.Message) string {
	return msg.Metadata.Get("correlation_id")
}

// GenerateCorrelationID generates a new correlation ID
func (h *MessageHelper) GenerateCorrelationID() string {
	return uuid.New().String()
}

// EnsureCorrelationID ensures a correlation ID exists, creating one if needed
func (h *MessageHelper) EnsureCorrelationID(msg *message.Message) string {
	correlationID := h.GetCorrelationID(msg)
	if correlationID == "" {
		correlationID = h.GenerateCorrelationID()
		h.SetCorrelationID(msg, correlationID)
	}
	return correlationID
}

// ============================================================================
// Trace Context Support (OpenTelemetry compatible)
// ============================================================================

// SetTraceContext sets distributed tracing context
func (h *MessageHelper) SetTraceContext(msg *message.Message, traceID, spanID string) {
	msg.Metadata.Set("trace_id", traceID)
	msg.Metadata.Set("span_id", spanID)
}

// GetTraceContext retrieves the trace context
func (h *MessageHelper) GetTraceContext(msg *message.Message) (traceID, spanID string) {
	return msg.Metadata.Get("trace_id"), msg.Metadata.Get("span_id")
}

// SetParentSpanID sets the parent span ID
func (h *MessageHelper) SetParentSpanID(msg *message.Message, parentSpanID string) {
	msg.Metadata.Set("parent_span_id", parentSpanID)
}

// GetParentSpanID retrieves the parent span ID
func (h *MessageHelper) GetParentSpanID(msg *message.Message) string {
	return msg.Metadata.Get("parent_span_id")
}

// ============================================================================
// Timestamp Support
// ============================================================================

// SetCreatedAt sets the message creation timestamp
func (h *MessageHelper) SetCreatedAt(msg *message.Message, t time.Time) {
	msg.Metadata.Set("created_at", t.Format(time.RFC3339Nano))
}

// GetCreatedAt retrieves the creation timestamp
func (h *MessageHelper) GetCreatedAt(msg *message.Message) (time.Time, error) {
	ts := msg.Metadata.Get("created_at")
	if ts == "" {
		return time.Time{}, fmt.Errorf("created_at not set")
	}
	return time.Parse(time.RFC3339Nano, ts)
}

// SetProcessedAt sets the message processing timestamp
func (h *MessageHelper) SetProcessedAt(msg *message.Message, t time.Time) {
	msg.Metadata.Set("processed_at", t.Format(time.RFC3339Nano))
}

// GetProcessedAt retrieves the processing timestamp
func (h *MessageHelper) GetProcessedAt(msg *message.Message) (time.Time, error) {
	ts := msg.Metadata.Get("processed_at")
	if ts == "" {
		return time.Time{}, fmt.Errorf("processed_at not set")
	}
	return time.Parse(time.RFC3339Nano, ts)
}

// GetAge returns the message age (time since creation)
func (h *MessageHelper) GetAge(msg *message.Message) (time.Duration, error) {
	createdAt, err := h.GetCreatedAt(msg)
	if err != nil {
		return 0, err
	}
	return time.Since(createdAt), nil
}

// ============================================================================
// Content Type and Encoding
// ============================================================================

// SetContentType sets the content type (e.g., "application/json")
func (h *MessageHelper) SetContentType(msg *message.Message, contentType string) {
	msg.Metadata.Set("content_type", contentType)
}

// GetContentType retrieves the content type
func (h *MessageHelper) GetContentType(msg *message.Message) string {
	return msg.Metadata.Get("content_type")
}

// SetEncoding sets the content encoding (e.g., "gzip", "deflate")
func (h *MessageHelper) SetEncoding(msg *message.Message, encoding string) {
	msg.Metadata.Set("content_encoding", encoding)
}

// GetEncoding retrieves the content encoding
func (h *MessageHelper) GetEncoding(msg *message.Message) string {
	return msg.Metadata.Get("content_encoding")
}

// ============================================================================
// Custom Headers
// ============================================================================

// SetHeaders sets custom headers as JSON
func (h *MessageHelper) SetHeaders(msg *message.Message, headers map[string]string) error {
	data, err := json.Marshal(headers)
	if err != nil {
		return fmt.Errorf("failed to marshal headers: %w", err)
	}
	msg.Metadata.Set("custom_headers", string(data))
	return nil
}

// GetHeaders retrieves custom headers
func (h *MessageHelper) GetHeaders(msg *message.Message) (map[string]string, error) {
	headersStr := msg.Metadata.Get("custom_headers")
	if headersStr == "" {
		return nil, nil
	}

	var headers map[string]string
	if err := json.Unmarshal([]byte(headersStr), &headers); err != nil {
		return nil, fmt.Errorf("failed to unmarshal headers: %w", err)
	}
	return headers, nil
}

// SetHeader sets a single custom header
func (h *MessageHelper) SetHeader(msg *message.Message, key, value string) error {
	headers, err := h.GetHeaders(msg)
	if err != nil {
		headers = make(map[string]string)
	}
	if headers == nil {
		headers = make(map[string]string)
	}
	headers[key] = value
	return h.SetHeaders(msg, headers)
}

// GetHeader retrieves a single custom header
func (h *MessageHelper) GetHeader(msg *message.Message, key string) (string, error) {
	headers, err := h.GetHeaders(msg)
	if err != nil {
		return "", err
	}
	if headers == nil {
		return "", nil
	}
	return headers[key], nil
}

// ============================================================================
// Source and Destination
// ============================================================================

// SetSource sets the message source (e.g., "api-gateway", "mobile-app")
func (h *MessageHelper) SetSource(msg *message.Message, source string) {
	msg.Metadata.Set("source", source)
}

// GetSource retrieves the message source
func (h *MessageHelper) GetSource(msg *message.Message) string {
	return msg.Metadata.Get("source")
}

// SetSourceVersion sets the source service version
func (h *MessageHelper) SetSourceVersion(msg *message.Message, version string) {
	msg.Metadata.Set("source_version", version)
}

// GetSourceVersion retrieves the source service version
func (h *MessageHelper) GetSourceVersion(msg *message.Message) string {
	return msg.Metadata.Get("source_version")
}

// ============================================================================
// User and Session Information
// ============================================================================

// SetUserID sets the user ID
func (h *MessageHelper) SetUserID(msg *message.Message, userID string) {
	msg.Metadata.Set("user_id", userID)
}

// GetUserID retrieves the user ID
func (h *MessageHelper) GetUserID(msg *message.Message) string {
	return msg.Metadata.Get("user_id")
}

// SetSessionID sets the session ID
func (h *MessageHelper) SetSessionID(msg *message.Message, sessionID string) {
	msg.Metadata.Set("session_id", sessionID)
}

// GetSessionID retrieves the session ID
func (h *MessageHelper) GetSessionID(msg *message.Message) string {
	return msg.Metadata.Get("session_id")
}

// SetTenantID sets the tenant ID (for multi-tenant systems)
func (h *MessageHelper) SetTenantID(msg *message.Message, tenantID string) {
	msg.Metadata.Set("tenant_id", tenantID)
}

// GetTenantID retrieves the tenant ID
func (h *MessageHelper) GetTenantID(msg *message.Message) string {
	return msg.Metadata.Get("tenant_id")
}

// ============================================================================
// Message Priority and Scheduling
// ============================================================================

// SetPriority sets the message priority (0-9, higher = more priority)
func (h *MessageHelper) SetPriority(msg *message.Message, priority int) {
	msg.Metadata.Set("priority", fmt.Sprintf("%d", priority))
}

// GetPriority retrieves the message priority
func (h *MessageHelper) GetPriority(msg *message.Message) (int, error) {
	priorityStr := msg.Metadata.Get("priority")
	if priorityStr == "" {
		return 0, nil
	}
	var priority int
	if _, err := fmt.Sscanf(priorityStr, "%d", &priority); err != nil {
		return 0, fmt.Errorf("invalid priority: %w", err)
	}
	return priority, nil
}

// SetScheduledAt sets when the message should be processed
func (h *MessageHelper) SetScheduledAt(msg *message.Message, t time.Time) {
	msg.Metadata.Set("scheduled_at", t.Format(time.RFC3339Nano))
}

// GetScheduledAt retrieves the scheduled processing time
func (h *MessageHelper) GetScheduledAt(msg *message.Message) (time.Time, error) {
	ts := msg.Metadata.Get("scheduled_at")
	if ts == "" {
		return time.Time{}, fmt.Errorf("scheduled_at not set")
	}
	return time.Parse(time.RFC3339Nano, ts)
}

// IsScheduled checks if message has a scheduled time
func (h *MessageHelper) IsScheduled(msg *message.Message) bool {
	return msg.Metadata.Get("scheduled_at") != ""
}

// ShouldProcessNow checks if a scheduled message should be processed now
func (h *MessageHelper) ShouldProcessNow(msg *message.Message) (bool, error) {
	if !h.IsScheduled(msg) {
		return true, nil
	}

	scheduledAt, err := h.GetScheduledAt(msg)
	if err != nil {
		return false, err
	}

	return time.Now().After(scheduledAt) || time.Now().Equal(scheduledAt), nil
}

// ============================================================================
// Retry Information
// ============================================================================

// SetRetryCount sets the retry count
func (h *MessageHelper) SetRetryCount(msg *message.Message, count int) {
	msg.Metadata.Set("retry_count", fmt.Sprintf("%d", count))
}

// GetRetryCount retrieves the retry count
func (h *MessageHelper) GetRetryCount(msg *message.Message) (int, error) {
	countStr := msg.Metadata.Get("retry_count")
	if countStr == "" {
		return 0, nil
	}
	var count int
	if _, err := fmt.Sscanf(countStr, "%d", &count); err != nil {
		return 0, fmt.Errorf("invalid retry count: %w", err)
	}
	return count, nil
}

// IncrementRetryCount increments the retry count
func (h *MessageHelper) IncrementRetryCount(msg *message.Message) (int, error) {
	count, err := h.GetRetryCount(msg)
	if err != nil {
		count = 0
	}
	count++
	h.SetRetryCount(msg, count)
	return count, nil
}

// SetLastError sets the last error message
func (h *MessageHelper) SetLastError(msg *message.Message, err error) {
	if err != nil {
		msg.Metadata.Set("last_error", err.Error())
		msg.Metadata.Set("last_error_at", time.Now().Format(time.RFC3339Nano))
	}
}

// GetLastError retrieves the last error message
func (h *MessageHelper) GetLastError(msg *message.Message) string {
	return msg.Metadata.Get("last_error")
}

// ============================================================================
// Environment and Debugging
// ============================================================================

// SetEnvironment sets the environment (dev, staging, prod)
func (h *MessageHelper) SetEnvironment(msg *message.Message, env string) {
	msg.Metadata.Set("environment", env)
}

// GetEnvironment retrieves the environment
func (h *MessageHelper) GetEnvironment(msg *message.Message) string {
	return msg.Metadata.Get("environment")
}

// SetDebugMode enables debug mode for this message
func (h *MessageHelper) SetDebugMode(msg *message.Message, enabled bool) {
	if enabled {
		msg.Metadata.Set("debug_mode", "true")
	} else {
		msg.Metadata.Delete("debug_mode")
	}
}

// IsDebugMode checks if debug mode is enabled
func (h *MessageHelper) IsDebugMode(msg *message.Message) bool {
	return msg.Metadata.Get("debug_mode") == "true"
}

// ============================================================================
// Message Decorators
// ============================================================================

// DecorateWithDefaults adds default metadata to a message
func (h *MessageHelper) DecorateWithDefaults(msg *message.Message, source, version string) {
	// Ensure correlation ID
	h.EnsureCorrelationID(msg)

	// Set timestamps
	if _, err := h.GetCreatedAt(msg); err != nil {
		h.SetCreatedAt(msg, time.Now())
	}

	// Set source
	if h.GetSource(msg) == "" && source != "" {
		h.SetSource(msg, source)
	}

	// Set version
	if h.GetSourceVersion(msg) == "" && version != "" {
		h.SetSourceVersion(msg, version)
	}

	// Set content type if not set
	if h.GetContentType(msg) == "" {
		h.SetContentType(msg, "application/json")
	}
}

// CopyMetadata copies metadata from one message to another
func (h *MessageHelper) CopyMetadata(from, to *message.Message, keys ...string) {
	if len(keys) == 0 {
		// Copy all metadata
		for k, v := range from.Metadata {
			to.Metadata.Set(k, v)
		}
	} else {
		// Copy specific keys
		for _, key := range keys {
			if value := from.Metadata.Get(key); value != "" {
				to.Metadata.Set(key, value)
			}
		}
	}
}

// CloneMessage creates a copy of a message with the same metadata
func (h *MessageHelper) CloneMessage(msg *message.Message) *message.Message {
	newMsg := message.NewMessage(uuid.New().String(), msg.Payload)
	h.CopyMetadata(msg, newMsg)
	return newMsg
}
