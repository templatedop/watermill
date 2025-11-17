module github.com/templatedop/watermill

go 1.21

require (
	github.com/IBM/sarama v1.43.0
	github.com/ThreeDotsLabs/watermill v1.3.5
	github.com/ThreeDotsLabs/watermill-kafka/v3 v3.0.0
	github.com/google/uuid v1.6.0
	github.com/pkg/errors v0.9.1

	// Observability
	github.com/prometheus/client_golang v1.18.0
	go.opentelemetry.io/otel v1.22.0
	go.opentelemetry.io/otel/trace v1.22.0

	// Storage backends
	github.com/syndtr/goleveldb v1.0.0
	github.com/redis/go-redis/v9 v9.4.0
	github.com/dgraph-io/badger/v4 v4.2.0

	// Schema registry
	github.com/linkedin/goavro/v2 v2.12.0
	google.golang.org/protobuf v1.32.0

	// Security
	github.com/xdg-go/scram v1.1.2
)
