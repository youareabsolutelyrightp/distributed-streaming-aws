package replay

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type topicPartition struct {
	topic     string
	partition int32
}

func consumeAssignedUntilQuiet(
	parent context.Context,
	brokers []string,
	topics map[string]int,
	timeout time.Duration,
	quietPeriod time.Duration,
	ready func() bool,
	visit func(*kgo.Record) error,
) error {
	assignments := make(map[string]map[int32]kgo.Offset, len(topics))
	expected := make(map[topicPartition]struct{})
	for topic, partitionCount := range topics {
		partitions := make(map[int32]kgo.Offset, partitionCount)
		for partition := 0; partition < partitionCount; partition++ {
			id := int32(partition)
			partitions[id] = kgo.NewOffset().AtStart()
			expected[topicPartition{topic: topic, partition: id}] = struct{}{}
		}
		assignments[topic] = partitions
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID("reconstructed-foil-verifier-v3"),
		kgo.ConsumePartitions(assignments),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	if err != nil {
		return err
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	positions := make(map[topicPartition]int64, len(expected))
	highWatermarks := make(map[topicPartition]int64, len(expected))
	var caughtUpSince time.Time
	for {
		pollCtx, pollCancel := context.WithTimeout(ctx, 250*time.Millisecond)
		fetches := client.PollFetches(pollCtx)
		pollErr := pollCtx.Err()
		pollCancel()
		if ctx.Err() != nil {
			return fmt.Errorf(
				"timed out waiting for completion and zero lag (ready=%t positions=%v high_watermarks=%v): %w",
				ready(), positions, highWatermarks, ctx.Err())
		}
		for _, fetchErr := range fetches.Errors() {
			if pollErr != nil && errors.Is(fetchErr.Err, pollErr) {
				continue
			}
			return fmt.Errorf(
				"consume %s partition %d: %w",
				fetchErr.Topic, fetchErr.Partition, fetchErr.Err)
		}
		var visitErr error
		fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
			key := topicPartition{topic: partition.Topic, partition: partition.Partition}
			highWatermarks[key] = partition.HighWatermark
			for _, record := range partition.Records {
				positions[key] = record.Offset + 1
				if visitErr == nil {
					visitErr = visit(record)
				}
			}
		})
		if visitErr != nil {
			return visitErr
		}
		caughtUp := true
		for partition := range expected {
			if positions[partition] < highWatermarks[partition] {
				caughtUp = false
				break
			}
		}
		if ready() && caughtUp {
			if caughtUpSince.IsZero() {
				caughtUpSince = time.Now()
			} else if time.Since(caughtUpSince) >= quietPeriod {
				return nil
			}
		} else {
			caughtUpSince = time.Time{}
		}
	}
}
