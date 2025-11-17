package schema

import (
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// ProtobufCodec implements Protobuf encoding/decoding with schema registry
type ProtobufCodec struct {
	registry   *RegistryClient
	subject    string
	schema     *Schema
	descriptor protoreflect.MessageDescriptor
}

// NewProtobufCodec creates a new Protobuf codec
func NewProtobufCodec(registry *RegistryClient, subject string) (*ProtobufCodec, error) {
	// Get latest schema
	schema, err := registry.GetLatestSchema(subject)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest schema: %w", err)
	}

	// Parse Protobuf schema
	descriptor, err := parseProtobufSchema(schema.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Protobuf schema: %w", err)
	}

	return &ProtobufCodec{
		registry:   registry,
		subject:    subject,
		schema:     schema,
		descriptor: descriptor,
	}, nil
}

// NewProtobufCodecWithSchema creates a new Protobuf codec with a specific schema
func NewProtobufCodecWithSchema(registry *RegistryClient, subject string, schemaString string) (*ProtobufCodec, error) {
	// Register schema
	schema, err := registry.RegisterSchema(subject, schemaString, SchemaTypeProtobuf)
	if err != nil {
		return nil, fmt.Errorf("failed to register schema: %w", err)
	}

	// Parse Protobuf schema
	descriptor, err := parseProtobufSchema(schemaString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Protobuf schema: %w", err)
	}

	return &ProtobufCodec{
		registry:   registry,
		subject:    subject,
		schema:     schema,
		descriptor: descriptor,
	}, nil
}

// Encode encodes a Protobuf message and prepends schema ID
func (pc *ProtobufCodec) Encode(msg proto.Message) ([]byte, error) {
	// Encode using Protobuf
	binary, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to encode Protobuf: %w", err)
	}

	// Prepend schema ID
	encoder := NewEncoder(pc.registry)
	encoded, err := encoder.Encode(pc.schema.ID, binary)
	if err != nil {
		return nil, fmt.Errorf("failed to encode with schema ID: %w", err)
	}

	return encoded, nil
}

// Decode decodes a Protobuf message with schema registry
func (pc *ProtobufCodec) Decode(encoded []byte, msg proto.Message) error {
	// Decode schema ID and data
	decoder := NewDecoder(pc.registry)
	schemaID, data, err := decoder.Decode(encoded)
	if err != nil {
		return fmt.Errorf("failed to decode schema ID: %w", err)
	}

	// Get schema
	schema, err := pc.registry.GetSchemaByID(schemaID)
	if err != nil {
		return fmt.Errorf("failed to get schema: %w", err)
	}

	// Parse descriptor for this schema
	descriptor, err := parseProtobufSchema(schema.Schema)
	if err != nil {
		return fmt.Errorf("failed to parse Protobuf schema: %w", err)
	}

	// Create dynamic message
	dynamicMsg := dynamicpb.NewMessage(descriptor)

	// Unmarshal Protobuf
	if err := proto.Unmarshal(data, dynamicMsg); err != nil {
		return fmt.Errorf("failed to decode Protobuf: %w", err)
	}

	// Copy to provided message
	if err := proto.Unmarshal(data, msg); err != nil {
		return fmt.Errorf("failed to unmarshal to provided message: %w", err)
	}

	return nil
}

// EncodeMap encodes a map as Protobuf
func (pc *ProtobufCodec) EncodeMap(data map[string]interface{}) ([]byte, error) {
	// Create dynamic message
	msg := dynamicpb.NewMessage(pc.descriptor)

	// Set fields from map
	for key, value := range data {
		field := pc.descriptor.Fields().ByJSONName(key)
		if field == nil {
			field = pc.descriptor.Fields().ByName(protoreflect.Name(key))
		}
		if field == nil {
			continue // Skip unknown fields
		}

		msg.Set(field, protoreflect.ValueOf(value))
	}

	return pc.Encode(msg)
}

// DecodeToMap decodes Protobuf to a map
func (pc *ProtobufCodec) DecodeToMap(encoded []byte) (map[string]interface{}, error) {
	// Decode schema ID and data
	decoder := NewDecoder(pc.registry)
	schemaID, data, err := decoder.Decode(encoded)
	if err != nil {
		return nil, fmt.Errorf("failed to decode schema ID: %w", err)
	}

	// Get schema
	schema, err := pc.registry.GetSchemaByID(schemaID)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	// Parse descriptor
	descriptor, err := parseProtobufSchema(schema.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Protobuf schema: %w", err)
	}

	// Create and unmarshal dynamic message
	msg := dynamicpb.NewMessage(descriptor)
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("failed to decode Protobuf: %w", err)
	}

	// Convert to map
	result := make(map[string]interface{})
	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		result[string(fd.Name())] = v.Interface()
		return true
	})

	return result, nil
}

// GetSchema returns the current schema
func (pc *ProtobufCodec) GetSchema() *Schema {
	return pc.schema
}

// UpdateSchema updates to a new schema version
func (pc *ProtobufCodec) UpdateSchema(version int) error {
	schema, err := pc.registry.GetSchemaByVersion(pc.subject, version)
	if err != nil {
		return fmt.Errorf("failed to get schema version %d: %w", version, err)
	}

	descriptor, err := parseProtobufSchema(schema.Schema)
	if err != nil {
		return fmt.Errorf("failed to parse Protobuf schema: %w", err)
	}

	pc.schema = schema
	pc.descriptor = descriptor

	return nil
}

func parseProtobufSchema(schemaString string) (protoreflect.MessageDescriptor, error) {
	// For simplicity, we expect the schema to be a FileDescriptorSet JSON
	// In production, you'd want to parse .proto files or use proper descriptor parsing
	var fdSet descriptorpb.FileDescriptorSet
	if err := json.Unmarshal([]byte(schemaString), &fdSet); err != nil {
		return nil, fmt.Errorf("failed to unmarshal FileDescriptorSet: %w", err)
	}

	if len(fdSet.File) == 0 {
		return nil, fmt.Errorf("empty FileDescriptorSet")
	}

	fd, err := protodesc.NewFile(fdSet.File[0], nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create file descriptor: %w", err)
	}

	// Get the first message type
	messages := fd.Messages()
	if messages.Len() == 0 {
		return nil, fmt.Errorf("no message types in schema")
	}

	return messages.Get(0), nil
}

// Helper function for JSON marshaling
func jsonMarshal(v interface{}) ([]byte, error) {
	return json.MarshalIndent(v, "", "  ")
}

// Example Protobuf schemas (as FileDescriptorSet JSON)
const (
	OrderEventProtoSchema = `{
		"file": [{
			"name": "order.proto",
			"package": "ecommerce",
			"messageType": [{
				"name": "OrderEvent",
				"field": [
					{"name": "order_id", "number": 1, "label": "LABEL_REQUIRED", "type": "TYPE_STRING"},
					{"name": "customer_id", "number": 2, "label": "LABEL_REQUIRED", "type": "TYPE_STRING"},
					{"name": "total", "number": 3, "label": "LABEL_REQUIRED", "type": "TYPE_DOUBLE"},
					{"name": "status", "number": 4, "label": "LABEL_REQUIRED", "type": "TYPE_STRING"},
					{"name": "created_at", "number": 5, "label": "LABEL_REQUIRED", "type": "TYPE_INT64"}
				]
			}]
		}]
	}`

	PaymentEventProtoSchema = `{
		"file": [{
			"name": "payment.proto",
			"package": "ecommerce",
			"messageType": [{
				"name": "PaymentEvent",
				"field": [
					{"name": "payment_id", "number": 1, "label": "LABEL_REQUIRED", "type": "TYPE_STRING"},
					{"name": "order_id", "number": 2, "label": "LABEL_REQUIRED", "type": "TYPE_STRING"},
					{"name": "amount", "number": 3, "label": "LABEL_REQUIRED", "type": "TYPE_DOUBLE"},
					{"name": "currency", "number": 4, "label": "LABEL_REQUIRED", "type": "TYPE_STRING"},
					{"name": "status", "number": 5, "label": "LABEL_REQUIRED", "type": "TYPE_STRING"}
				]
			}]
		}]
	}`
)
