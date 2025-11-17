package kafka

import (
	"encoding/json"
	"fmt"
)

// Codec is an interface for encoding/decoding messages
// Similar to Goka's codec system for flexible serialization
type Codec interface {
	// Encode converts a value to bytes
	Encode(value interface{}) ([]byte, error)
	// Decode converts bytes to a value
	Decode(data []byte, value interface{}) error
}

// JSONCodec implements JSON encoding/decoding
type JSONCodec struct{}

// NewJSONCodec creates a new JSON codec
func NewJSONCodec() *JSONCodec {
	return &JSONCodec{}
}

// Encode encodes a value to JSON
func (c *JSONCodec) Encode(value interface{}) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("json encode failed: %w", err)
	}
	return data, nil
}

// Decode decodes JSON to a value
func (c *JSONCodec) Decode(data []byte, value interface{}) error {
	if err := json.Unmarshal(data, value); err != nil {
		return fmt.Errorf("json decode failed: %w", err)
	}
	return nil
}

// StringCodec implements string encoding/decoding
type StringCodec struct{}

// NewStringCodec creates a new string codec
func NewStringCodec() *StringCodec {
	return &StringCodec{}
}

// Encode encodes a string to bytes
func (c *StringCodec) Encode(value interface{}) ([]byte, error) {
	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("value must be string, got %T", value)
	}
	return []byte(str), nil
}

// Decode decodes bytes to a string
func (c *StringCodec) Decode(data []byte, value interface{}) error {
	ptr, ok := value.(*string)
	if !ok {
		return fmt.Errorf("value must be *string, got %T", value)
	}
	*ptr = string(data)
	return nil
}

// BytesCodec implements raw bytes encoding/decoding
type BytesCodec struct{}

// NewBytesCodec creates a new bytes codec
func NewBytesCodec() *BytesCodec {
	return &BytesCodec{}
}

// Encode returns bytes as-is
func (c *BytesCodec) Encode(value interface{}) ([]byte, error) {
	bytes, ok := value.([]byte)
	if !ok {
		return nil, fmt.Errorf("value must be []byte, got %T", value)
	}
	return bytes, nil
}

// Decode returns bytes as-is
func (c *BytesCodec) Decode(data []byte, value interface{}) error {
	ptr, ok := value.(*[]byte)
	if !ok {
		return fmt.Errorf("value must be *[]byte, got %T", value)
	}
	*ptr = data
	return nil
}

// Int64Codec implements int64 encoding/decoding
type Int64Codec struct{}

// NewInt64Codec creates a new int64 codec
func NewInt64Codec() *Int64Codec {
	return &Int64Codec{}
}

// Encode encodes int64 to bytes (8 bytes big-endian)
func (c *Int64Codec) Encode(value interface{}) ([]byte, error) {
	v, ok := value.(int64)
	if !ok {
		return nil, fmt.Errorf("value must be int64, got %T", value)
	}

	bytes := make([]byte, 8)
	bytes[0] = byte(v >> 56)
	bytes[1] = byte(v >> 48)
	bytes[2] = byte(v >> 40)
	bytes[3] = byte(v >> 32)
	bytes[4] = byte(v >> 24)
	bytes[5] = byte(v >> 16)
	bytes[6] = byte(v >> 8)
	bytes[7] = byte(v)

	return bytes, nil
}

// Decode decodes bytes to int64
func (c *Int64Codec) Decode(data []byte, value interface{}) error {
	if len(data) != 8 {
		return fmt.Errorf("data must be 8 bytes, got %d", len(data))
	}

	ptr, ok := value.(*int64)
	if !ok {
		return fmt.Errorf("value must be *int64, got %T", value)
	}

	*ptr = int64(data[0])<<56 | int64(data[1])<<48 |
		int64(data[2])<<40 | int64(data[3])<<32 |
		int64(data[4])<<24 | int64(data[5])<<16 |
		int64(data[6])<<8 | int64(data[7])

	return nil
}

// CodecRegistry manages codecs for different types
type CodecRegistry struct {
	codecs map[string]Codec
}

// NewCodecRegistry creates a new codec registry
func NewCodecRegistry() *CodecRegistry {
	return &CodecRegistry{
		codecs: make(map[string]Codec),
	}
}

// Register registers a codec for a type name
func (r *CodecRegistry) Register(typeName string, codec Codec) {
	r.codecs[typeName] = codec
}

// Get retrieves a codec for a type name
func (r *CodecRegistry) Get(typeName string) (Codec, error) {
	codec, exists := r.codecs[typeName]
	if !exists {
		return nil, fmt.Errorf("codec not found for type: %s", typeName)
	}
	return codec, nil
}

// DefaultCodecRegistry returns a registry with common codecs
func DefaultCodecRegistry() *CodecRegistry {
	registry := NewCodecRegistry()
	registry.Register("json", NewJSONCodec())
	registry.Register("string", NewStringCodec())
	registry.Register("bytes", NewBytesCodec())
	registry.Register("int64", NewInt64Codec())
	return registry
}
