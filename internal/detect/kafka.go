package detect

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mertmetin/listing-alerts/internal/event"
)

// Topics.
const (
	TopicChanges = "listing.changes"
	TopicEvents  = "listing.events"
	Group        = "detector"
)

// KafkaPublisher writes listing events keyed by ListingKey.
type KafkaPublisher struct {
	Client *kgo.Client
	Topic  string
}

func (p KafkaPublisher) Publish(ctx context.Context, events []event.ListingEvent) error {
	if len(events) == 0 {
		return nil
	}
	recs := make([]*kgo.Record, 0, len(events))
	for _, e := range events {
		b, err := e.Encode()
		if err != nil {
			return err
		}
		recs = append(recs, &kgo.Record{
			Topic: p.Topic, Key: []byte(e.ListingKey), Value: b,
			Headers: []kgo.RecordHeader{
				{Key: "id", Value: []byte(e.ID)},
				{Key: "type", Value: []byte(e.Type)},
				{Key: "public", Value: []byte(strconv.FormatBool(e.Public))},
				{Key: "observed_at_ms", Value: []byte(strconv.FormatInt(e.ObservedAt.UnixMilli(), 10))},
			},
		})
	}
	var errs []error
	for _, r := range p.Client.ProduceSync(ctx, recs...) {
		if r.Err != nil {
			errs = append(errs, r.Err)
		}
	}
	return errors.Join(errs...)
}

// ConsumerOpts configures the detector's group consumer: manual commits after
// each processed batch, and rebalances held until a batch is finished, so a
// partition is not processed by two members at once in the normal case (the
// state version check covers the abnormal one, e.g. an expired session).
func ConsumerOpts(group, topic string) []kgo.Opt {
	return []kgo.Opt{
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
	}
}

// Run consumes listing.changes until ctx is cancelled. Offsets are committed
// only after a batch's events are published and its state saved:
// at-least-once, with replays absorbed by the state version checks.
func Run(ctx context.Context, cl *kgo.Client, d *Detector, maxBatch int, log *slog.Logger) error {
	// With BlockRebalanceOnPoll, leaving the group waits for AllowRebalance.
	// Release it on every exit, or shutdown hangs in LeaveGroup.
	defer cl.AllowRebalance()
	for {
		fs := cl.PollRecords(ctx, maxBatch)
		if ctx.Err() != nil {
			return nil
		}
		fs.EachError(func(t string, p int32, err error) {
			log.Error("fetch error", "topic", t, "partition", p, "err", err)
		})
		recs := fs.Records()
		batch := make([]event.Change, 0, len(recs))
		for _, r := range recs {
			c, err := event.Decode(r.Value)
			if err != nil || c.ListingKey == "" {
				d.Metrics.Invalid.Inc()
				log.Error("skipping undecodable change", "partition", r.Partition, "offset", r.Offset, "err", err)
				continue
			}
			batch = append(batch, c)
		}
		for {
			err := d.Process(ctx, batch)
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return nil
			}
			// Postgres or Redpanda trouble: keep the batch, retry. Offsets
			// stay uncommitted, so a restart would redo it too.
			log.Error("batch failed, retrying", "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(2 * time.Second):
			}
		}
		if len(recs) > 0 {
			if err := cl.CommitRecords(ctx, recs...); err != nil && ctx.Err() == nil {
				log.Error("commit offsets", "err", err) // the next commit covers these
			}
		}
		cl.AllowRebalance()
	}
}
