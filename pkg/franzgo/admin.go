package franzgo

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// AdminClient provides cluster administration operations
type AdminClient struct {
	client *Client
	admin  *kadm.Client
}

// NewAdminClient creates a new admin client
func NewAdminClient(client *Client) *AdminClient {
	return &AdminClient{
		client: client,
		admin:  kadm.NewClient(client.GetKgoClient()),
	}
}

// TopicConfig represents topic configuration
type TopicConfig struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int16
	Configs           map[string]string
}

// TopicDetail contains detailed topic information
type TopicDetail struct {
	Name              string
	Partitions        []PartitionDetail
	ReplicationFactor int16
	Configs           map[string]string
	Internal          bool
}

// PartitionDetail contains partition information
type PartitionDetail struct {
	Partition int32
	Leader    int32
	Replicas  []int32
	ISR       []int32
	Offline   []int32
}

// CreateTopic creates a new topic
func (ac *AdminClient) CreateTopic(ctx context.Context, config TopicConfig) error {
	// Convert config to kadm format
	topic := kadm.TopicDetail{
		Topic:             config.Name,
		NumPartitions:     config.NumPartitions,
		ReplicationFactor: config.ReplicationFactor,
	}

	if config.Configs != nil {
		topic.Configs = make(map[string]*string)
		for k, v := range config.Configs {
			val := v
			topic.Configs[k] = &val
		}
	}

	// Create topic
	resp, err := ac.admin.CreateTopics(ctx, -1, topic)
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	// Check for errors
	for _, result := range resp {
		if result.Err != nil {
			return fmt.Errorf("failed to create topic %s: %w", result.Topic, result.Err)
		}
	}

	return nil
}

// CreateTopics creates multiple topics
func (ac *AdminClient) CreateTopics(ctx context.Context, configs []TopicConfig) error {
	topics := make([]kadm.TopicDetail, len(configs))
	for i, config := range configs {
		topics[i] = kadm.TopicDetail{
			Topic:             config.Name,
			NumPartitions:     config.NumPartitions,
			ReplicationFactor: config.ReplicationFactor,
		}

		if config.Configs != nil {
			topics[i].Configs = make(map[string]*string)
			for k, v := range config.Configs {
				val := v
				topics[i].Configs[k] = &val
			}
		}
	}

	resp, err := ac.admin.CreateTopics(ctx, -1, topics...)
	if err != nil {
		return fmt.Errorf("failed to create topics: %w", err)
	}

	// Collect errors
	var errs []error
	for _, result := range resp {
		if result.Err != nil {
			errs = append(errs, fmt.Errorf("topic %s: %w", result.Topic, result.Err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors creating topics: %v", errs)
	}

	return nil
}

// DeleteTopic deletes a topic
func (ac *AdminClient) DeleteTopic(ctx context.Context, topic string) error {
	resp, err := ac.admin.DeleteTopics(ctx, topic)
	if err != nil {
		return fmt.Errorf("failed to delete topic: %w", err)
	}

	for _, result := range resp {
		if result.Err != nil {
			return fmt.Errorf("failed to delete topic %s: %w", result.Topic, result.Err)
		}
	}

	return nil
}

// DeleteTopics deletes multiple topics
func (ac *AdminClient) DeleteTopics(ctx context.Context, topics []string) error {
	resp, err := ac.admin.DeleteTopics(ctx, topics...)
	if err != nil {
		return fmt.Errorf("failed to delete topics: %w", err)
	}

	var errs []error
	for _, result := range resp {
		if result.Err != nil {
			errs = append(errs, fmt.Errorf("topic %s: %w", result.Topic, result.Err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors deleting topics: %v", errs)
	}

	return nil
}

// ListTopics lists all topics
func (ac *AdminClient) ListTopics(ctx context.Context) ([]string, error) {
	metadata, err := ac.admin.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %w", err)
	}

	topics := make([]string, 0, len(metadata.Topics))
	for topic := range metadata.Topics {
		topics = append(topics, topic)
	}

	return topics, nil
}

// DescribeTopic describes a topic
func (ac *AdminClient) DescribeTopic(ctx context.Context, topic string) (*TopicDetail, error) {
	metadata, err := ac.admin.Metadata(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic metadata: %w", err)
	}

	topicMeta, exists := metadata.Topics[topic]
	if !exists {
		return nil, fmt.Errorf("topic %s not found", topic)
	}

	detail := &TopicDetail{
		Name:       topic,
		Partitions: make([]PartitionDetail, len(topicMeta.Partitions)),
		Internal:   topicMeta.IsInternal,
	}

	for i, partition := range topicMeta.Partitions {
		detail.Partitions[i] = PartitionDetail{
			Partition: partition.Partition,
			Leader:    partition.Leader,
			Replicas:  partition.Replicas,
			ISR:       partition.ISR,
			Offline:   partition.OfflineReplicas,
		}

		if len(partition.Replicas) > 0 {
			detail.ReplicationFactor = int16(len(partition.Replicas))
		}
	}

	// Get topic configs
	configs, err := ac.admin.DescribeTopicConfigs(ctx, topic)
	if err == nil {
		detail.Configs = make(map[string]string)
		for _, config := range configs {
			for _, entry := range config.Configs {
				if entry.Value != nil {
					detail.Configs[entry.Key] = *entry.Value
				}
			}
		}
	}

	return detail, nil
}

// ConsumerGroupDetail contains consumer group information
type ConsumerGroupDetail struct {
	GroupID       string
	State         string
	Protocol      string
	ProtocolType  string
	Members       []ConsumerGroupMember
	Coordinator   int32
	Lag           map[string]map[int32]int64 // topic -> partition -> lag
}

// ConsumerGroupMember represents a member of a consumer group
type ConsumerGroupMember struct {
	MemberID   string
	ClientID   string
	ClientHost string
	Assignment []TopicPartition
}

// TopicPartition represents a topic partition assignment
type TopicPartition struct {
	Topic     string
	Partition int32
}

// ListConsumerGroups lists all consumer groups
func (ac *AdminClient) ListConsumerGroups(ctx context.Context) ([]string, error) {
	groups, err := ac.admin.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups: %w", err)
	}

	groupIDs := make([]string, 0, len(groups))
	for groupID := range groups {
		groupIDs = append(groupIDs, groupID)
	}

	return groupIDs, nil
}

// DescribeConsumerGroup describes a consumer group
func (ac *AdminClient) DescribeConsumerGroup(ctx context.Context, groupID string) (*ConsumerGroupDetail, error) {
	described, err := ac.admin.DescribeGroups(ctx, groupID)
	if err != nil {
		return nil, fmt.Errorf("failed to describe consumer group: %w", err)
	}

	group, exists := described[groupID]
	if !exists {
		return nil, fmt.Errorf("consumer group %s not found", groupID)
	}

	detail := &ConsumerGroupDetail{
		GroupID:      groupID,
		State:        group.State,
		Protocol:     group.Protocol,
		ProtocolType: group.ProtocolType,
		Coordinator:  group.Coordinator.NodeID,
		Members:      make([]ConsumerGroupMember, len(group.Members)),
	}

	for i, member := range group.Members {
		detail.Members[i] = ConsumerGroupMember{
			MemberID:   member.MemberID,
			ClientID:   member.ClientID,
			ClientHost: member.ClientHost,
		}
	}

	// Get consumer group lag
	lag, err := ac.admin.Lag(ctx, groupID)
	if err == nil {
		detail.Lag = make(map[string]map[int32]int64)
		lag.Each(func(l kadm.DescribedGroupLag) {
			if _, exists := detail.Lag[l.Topic]; !exists {
				detail.Lag[l.Topic] = make(map[int32]int64)
			}
			detail.Lag[l.Topic][l.Partition] = l.Lag
		})
	}

	return detail, nil
}

// DeleteConsumerGroup deletes a consumer group
func (ac *AdminClient) DeleteConsumerGroup(ctx context.Context, groupID string) error {
	resp, err := ac.admin.DeleteGroups(ctx, groupID)
	if err != nil {
		return fmt.Errorf("failed to delete consumer group: %w", err)
	}

	for _, result := range resp {
		if result.Err != nil {
			return fmt.Errorf("failed to delete consumer group %s: %w", result.Group, result.Err)
		}
	}

	return nil
}

// GetConsumerGroupLag gets the lag for a consumer group
func (ac *AdminClient) GetConsumerGroupLag(ctx context.Context, groupID string) (map[string]map[int32]int64, error) {
	lag, err := ac.admin.Lag(ctx, groupID)
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer group lag: %w", err)
	}

	result := make(map[string]map[int32]int64)
	lag.Each(func(l kadm.DescribedGroupLag) {
		if _, exists := result[l.Topic]; !exists {
			result[l.Topic] = make(map[int32]int64)
		}
		result[l.Topic][l.Partition] = l.Lag
	})

	return result, nil
}

// ResetConsumerGroupOffsets resets consumer group offsets
func (ac *AdminClient) ResetConsumerGroupOffsets(ctx context.Context, groupID string, topic string, offsets map[int32]int64) error {
	// Build offset request
	offsetMap := make(map[string]map[int32]kadm.Offset)
	offsetMap[topic] = make(map[int32]kadm.Offset)

	for partition, offset := range offsets {
		offsetMap[topic][partition] = kadm.Offset{
			At: offset,
		}
	}

	// Commit offsets
	resp, err := ac.admin.CommitOffsets(ctx, groupID, offsetMap)
	if err != nil {
		return fmt.Errorf("failed to reset offsets: %w", err)
	}

	// Check for errors
	if resp.Error() != nil {
		return fmt.Errorf("offset reset errors: %w", resp.Error())
	}

	return nil
}

// BrokerInfo contains broker information
type BrokerInfo struct {
	NodeID int32
	Host   string
	Port   int32
	Rack   string
}

// ClusterInfo contains cluster information
type ClusterInfo struct {
	ClusterID     string
	Controller    int32
	Brokers       []BrokerInfo
	TopicCount    int
	PartitionCount int
}

// GetClusterInfo retrieves cluster information
func (ac *AdminClient) GetClusterInfo(ctx context.Context) (*ClusterInfo, error) {
	metadata, err := ac.admin.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster metadata: %w", err)
	}

	info := &ClusterInfo{
		Controller: metadata.Controller.NodeID,
		Brokers:    make([]BrokerInfo, len(metadata.Brokers)),
		TopicCount: len(metadata.Topics),
	}

	for i, broker := range metadata.Brokers {
		info.Brokers[i] = BrokerInfo{
			NodeID: broker.NodeID,
			Host:   broker.Host,
			Port:   broker.Port,
			Rack:   broker.Rack,
		}
	}

	// Count partitions
	for _, topic := range metadata.Topics {
		info.PartitionCount += len(topic.Partitions)
	}

	return info, nil
}

// PartitionReassignment represents a partition reassignment
type PartitionReassignment struct {
	Topic     string
	Partition int32
	Replicas  []int32
}

// ReassignPartitions reassigns partitions to different brokers
func (ac *AdminClient) ReassignPartitions(ctx context.Context, reassignments []PartitionReassignment) error {
	// Build reassignment request
	req := kmsg.NewAlterPartitionAssignmentsRequest()

	for _, reassignment := range reassignments {
		topicReq := kmsg.NewAlterPartitionAssignmentsRequestTopic()
		topicReq.Topic = reassignment.Topic

		partReq := kmsg.NewAlterPartitionAssignmentsRequestTopicPartition()
		partReq.Partition = reassignment.Partition
		partReq.Replicas = reassignment.Replicas

		topicReq.Partitions = append(topicReq.Partitions, partReq)
		req.Topics = append(req.Topics, topicReq)
	}

	// Execute request
	resp, err := req.RequestWith(ctx, ac.client.GetKgoClient())
	if err != nil {
		return fmt.Errorf("failed to reassign partitions: %w", err)
	}

	// Check for errors
	if resp.ErrorCode != 0 {
		return fmt.Errorf("partition reassignment error: code %d, message: %s",
			resp.ErrorCode, resp.ErrorMessage)
	}

	return nil
}

// AlterTopicConfig alters topic configuration
func (ac *AdminClient) AlterTopicConfig(ctx context.Context, topic string, configs map[string]string) error {
	// Build config entries
	resources := []kadm.AlterConfig{
		{
			Op:    kadm.SetConfig,
			Name:  topic,
			Type:  kadm.ConfigResourceTypeTopic,
			Configs: func() []kadm.AlterConfigOp {
				ops := make([]kadm.AlterConfigOp, 0, len(configs))
				for k, v := range configs {
					val := v
					ops = append(ops, kadm.AlterConfigOp{
						Name:  k,
						Value: &val,
					})
				}
				return ops
			}(),
		},
	}

	// Alter config
	resp, err := ac.admin.AlterConfigs(ctx, resources)
	if err != nil {
		return fmt.Errorf("failed to alter topic config: %w", err)
	}

	// Check for errors
	for _, result := range resp {
		if result.Err != nil {
			return fmt.Errorf("failed to alter config for %s: %w", result.Name, result.Err)
		}
	}

	return nil
}

// OffsetInfo contains offset information for a partition
type OffsetInfo struct {
	Topic          string
	Partition      int32
	OldestOffset   int64
	NewestOffset   int64
	MessageCount   int64
}

// GetPartitionOffsets gets offset information for partitions
func (ac *AdminClient) GetPartitionOffsets(ctx context.Context, topics []string) ([]OffsetInfo, error) {
	offsets, err := ac.admin.ListEndOffsets(ctx, topics...)
	if err != nil {
		return nil, fmt.Errorf("failed to list offsets: %w", err)
	}

	startOffsets, err := ac.admin.ListStartOffsets(ctx, topics...)
	if err != nil {
		return nil, fmt.Errorf("failed to list start offsets: %w", err)
	}

	var result []OffsetInfo

	offsets.Each(func(o kadm.ListedOffset) {
		info := OffsetInfo{
			Topic:        o.Topic,
			Partition:    o.Partition,
			NewestOffset: o.Offset,
		}

		// Find corresponding start offset
		startOffsets.Each(func(s kadm.ListedOffset) {
			if s.Topic == o.Topic && s.Partition == o.Partition {
				info.OldestOffset = s.Offset
				info.MessageCount = o.Offset - s.Offset
			}
		})

		result = append(result, info)
	})

	return result, nil
}

// AddPartitions adds partitions to an existing topic
func (ac *AdminClient) AddPartitions(ctx context.Context, topic string, totalPartitions int32) error {
	req := kmsg.NewCreatePartitionsRequest()

	topicReq := kmsg.NewCreatePartitionsRequestTopic()
	topicReq.Topic = topic
	topicReq.Count = totalPartitions

	req.Topics = append(req.Topics, topicReq)
	req.TimeoutMillis = 30000

	resp, err := req.RequestWith(ctx, ac.client.GetKgoClient())
	if err != nil {
		return fmt.Errorf("failed to add partitions: %w", err)
	}

	// Check for errors
	for _, result := range resp.Topics {
		if result.ErrorCode != 0 {
			return fmt.Errorf("failed to add partitions to %s: error code %d, message: %s",
				result.Topic, result.ErrorCode, result.ErrorMessage)
		}
	}

	return nil
}

// ElectLeaders triggers leader election for partitions
func (ac *AdminClient) ElectLeaders(ctx context.Context, electionType int8, topics map[string][]int32) error {
	req := kmsg.NewElectLeadersRequest()
	req.ElectionType = electionType
	req.TimeoutMillis = 30000

	if topics != nil {
		for topic, partitions := range topics {
			topicReq := kmsg.NewElectLeadersRequestTopic()
			topicReq.Topic = topic
			topicReq.Partitions = partitions
			req.Topics = append(req.Topics, topicReq)
		}
	}

	resp, err := req.RequestWith(ctx, ac.client.GetKgoClient())
	if err != nil {
		return fmt.Errorf("failed to elect leaders: %w", err)
	}

	// Check for errors
	if resp.ErrorCode != 0 {
		return fmt.Errorf("leader election error: code %d", resp.ErrorCode)
	}

	return nil
}

// CreateCompactedTopic creates a compacted topic (useful for changelog)
func (ac *AdminClient) CreateCompactedTopic(ctx context.Context, name string, partitions int32, replicationFactor int16) error {
	return ac.CreateTopic(ctx, TopicConfig{
		Name:              name,
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
		Configs: map[string]string{
			"cleanup.policy":      "compact",
			"min.cleanable.dirty.ratio": "0.01",
			"delete.retention.ms": "100",
		},
	})
}

// WaitForTopic waits for a topic to be created and available
func (ac *AdminClient) WaitForTopic(ctx context.Context, topic string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		_, err := ac.DescribeTopic(ctx, topic)
		if err == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
			// Continue waiting
		}
	}

	return fmt.Errorf("timeout waiting for topic %s", topic)
}

// Close closes the admin client
func (ac *AdminClient) Close() {
	// kadm.Client doesn't need explicit closing
}
