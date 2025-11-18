package schema

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// ProtobufCodec implements Protobuf encoding/decoding with schema registry
type ProtobufCodec struct {
	*BaseCodec
	descriptors map[int]protoreflect.MessageDescriptor
}

// NewProtobufCodec creates a new Protobuf codec
func NewProtobufCodec(registry SchemaRegistry) *ProtobufCodec {
	return &ProtobufCodec{
		BaseCodec:   NewBaseCodec(registry),
		descriptors: make(map[int]protoreflect.MessageDescriptor),
	}
}

// Encode encodes data to Protobuf format with schema
func (c *ProtobufCodec) Encode(schemaID int, data proto.Message) ([]byte, error) {
	// Encode to protobuf binary
	binary, err := proto.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal protobuf: %w", err)
	}

	// Add magic byte and schema ID
	return c.EncodeWithMagicByte(schemaID, binary)
}

// Decode decodes Protobuf data with schema
func (c *ProtobufCodec) Decode(data []byte) (proto.Message, int, error) {
	// Extract payload and schema ID
	payload, schemaID, err := c.DecodeWithMagicByte(data)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to decode magic byte: %w", err)
	}

	// Get descriptor for schema
	descriptor, err := c.getDescriptor(schemaID)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get descriptor: %w", err)
	}

	// Create dynamic message
	message := dynamicpb.NewMessage(descriptor)

	// Unmarshal protobuf binary
	if err := proto.Unmarshal(payload, message); err != nil {
		return nil, 0, fmt.Errorf("failed to unmarshal protobuf: %w", err)
	}

	return message, schemaID, nil
}

// DecodeInto decodes Protobuf data into a provided message
func (c *ProtobufCodec) DecodeInto(data []byte, message proto.Message) (int, error) {
	// Extract payload and schema ID
	payload, schemaID, err := c.DecodeWithMagicByte(data)
	if err != nil {
		return 0, fmt.Errorf("failed to decode magic byte: %w", err)
	}

	// Unmarshal into provided message
	if err := proto.Unmarshal(payload, message); err != nil {
		return 0, fmt.Errorf("failed to unmarshal protobuf: %w", err)
	}

	return schemaID, nil
}

// getDescriptor retrieves a descriptor for a schema
func (c *ProtobufCodec) getDescriptor(schemaID int) (protoreflect.MessageDescriptor, error) {
	// Check if descriptor already exists
	if descriptor, exists := c.descriptors[schemaID]; exists {
		return descriptor, nil
	}

	// Note: In a real implementation, you would need to:
	// 1. Fetch the schema from the registry
	// 2. Parse the .proto file or use protobuf descriptors
	// 3. Create the message descriptor
	// For now, this is a placeholder that would be implemented based on
	// how protobuf schemas are stored in your schema registry

	return nil, fmt.Errorf("descriptor not found for schema ID %d - implement schema parsing", schemaID)
}

// ProtobufProducer wraps a producer with Protobuf serialization
type ProtobufProducer struct {
	codec    *ProtobufCodec
	subject  string
	schemaID int
}

// NewProtobufProducer creates a new Protobuf producer
func NewProtobufProducer(codec *ProtobufCodec, subject string, schemaID int) *ProtobufProducer {
	return &ProtobufProducer{
		codec:    codec,
		subject:  subject,
		schemaID: schemaID,
	}
}

// Serialize serializes data using Protobuf
func (p *ProtobufProducer) Serialize(message proto.Message) ([]byte, error) {
	return p.codec.Encode(p.schemaID, message)
}

// ProtobufConsumer wraps a consumer with Protobuf deserialization
type ProtobufConsumer struct {
	codec *ProtobufCodec
}

// NewProtobufConsumer creates a new Protobuf consumer
func NewProtobufConsumer(codec *ProtobufCodec) *ProtobufConsumer {
	return &ProtobufConsumer{
		codec: codec,
	}
}

// Deserialize deserializes Protobuf data
func (c *ProtobufConsumer) Deserialize(data []byte) (proto.Message, int, error) {
	return c.codec.Decode(data)
}

// DeserializeInto deserializes Protobuf data into a provided message
func (c *ProtobufConsumer) DeserializeInto(data []byte, message proto.Message) (int, error) {
	return c.codec.DecodeInto(data, message)
}

// ProtobufSchemaHelper provides utility functions for Protobuf schemas
type ProtobufSchemaHelper struct{}

// NewProtobufSchemaHelper creates a new Protobuf schema helper
func NewProtobufSchemaHelper() *ProtobufSchemaHelper {
	return &ProtobufSchemaHelper{}
}

// GetMessageName returns the full name of a protobuf message
func (h *ProtobufSchemaHelper) GetMessageName(message proto.Message) string {
	return string(message.ProtoReflect().Descriptor().FullName())
}

// GetFieldNames returns all field names in a message
func (h *ProtobufSchemaHelper) GetFieldNames(message proto.Message) []string {
	descriptor := message.ProtoReflect().Descriptor()
	fields := descriptor.Fields()

	names := make([]string, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		names[i] = string(fields.Get(i).Name())
	}

	return names
}

// ValidateMessage validates a protobuf message
func (h *ProtobufSchemaHelper) ValidateMessage(message proto.Message) error {
	// Basic validation - check if all required fields are set
	// In protobuf3, all fields are optional, but you might want custom validation
	if !proto.IsValid(message) {
		return fmt.Errorf("invalid protobuf message")
	}
	return nil
}

// CloneMessage creates a deep copy of a protobuf message
func (h *ProtobufSchemaHelper) CloneMessage(message proto.Message) proto.Message {
	return proto.Clone(message)
}

// MergeMessages merges src into dst
func (h *ProtobufSchemaHelper) MergeMessages(dst, src proto.Message) {
	proto.Merge(dst, src)
}

// MessageToBytes converts a message to bytes (without schema registry)
func (h *ProtobufSchemaHelper) MessageToBytes(message proto.Message) ([]byte, error) {
	return proto.Marshal(message)
}

// BytesToMessage converts bytes to a message (without schema registry)
func (h *ProtobufSchemaHelper) BytesToMessage(data []byte, message proto.Message) error {
	return proto.Unmarshal(data, message)
}

// GetFieldValue gets a field value from a message
func (h *ProtobufSchemaHelper) GetFieldValue(message proto.Message, fieldName string) (interface{}, error) {
	reflection := message.ProtoReflect()
	descriptor := reflection.Descriptor()
	fields := descriptor.Fields()

	field := fields.ByName(protoreflect.Name(fieldName))
	if field == nil {
		return nil, fmt.Errorf("field %s not found", fieldName)
	}

	value := reflection.Get(field)
	return value.Interface(), nil
}

// SetFieldValue sets a field value in a message
func (h *ProtobufSchemaHelper) SetFieldValue(message proto.Message, fieldName string, value interface{}) error {
	reflection := message.ProtoReflect()
	descriptor := reflection.Descriptor()
	fields := descriptor.Fields()

	field := fields.ByName(protoreflect.Name(fieldName))
	if field == nil {
		return fmt.Errorf("field %s not found", fieldName)
	}

	// Convert value to protoreflect.Value
	protoValue := protoreflect.ValueOf(value)
	reflection.Set(field, protoValue)

	return nil
}

// TypedProtobufCodec provides type-safe encoding/decoding
type TypedProtobufCodec[T proto.Message] struct {
	codec    *ProtobufCodec
	schemaID int
}

// NewTypedProtobufCodec creates a new typed Protobuf codec
func NewTypedProtobufCodec[T proto.Message](codec *ProtobufCodec, schemaID int) *TypedProtobufCodec[T] {
	return &TypedProtobufCodec[T]{
		codec:    codec,
		schemaID: schemaID,
	}
}

// Encode encodes a typed message
func (c *TypedProtobufCodec[T]) Encode(message T) ([]byte, error) {
	return c.codec.Encode(c.schemaID, message)
}

// Decode decodes into a typed message
func (c *TypedProtobufCodec[T]) Decode(data []byte, message T) (int, error) {
	return c.codec.DecodeInto(data, message)
}
