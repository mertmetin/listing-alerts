package ingest

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mertmetin/listing-alerts/internal/event"
)

// TopicChanges is where pollers publish.
const TopicChanges = "listing.changes"

// KafkaPublisher publishes changes keyed by ListingKey. The franz-go client
// is idempotent by default with acks from all in-sync replicas, so a retried
// batch is not duplicated inside the broker; cross-restart duplicates are
// handled by event IDs.
type KafkaPublisher struct {
	Client *kgo.Client
	Topic  string
}

// ProducerOpts are the client options pollers use.
func ProducerOpts() []kgo.Opt {
	return []kgo.Opt{
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5 * time.Millisecond),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
		kgo.RecordRetries(10),
	}
}

// Publish sends every change and waits for all acks.
func (p KafkaPublisher) Publish(ctx context.Context, changes []event.Change) error {
	if len(changes) == 0 {
		return nil
	}
	recs := make([]*kgo.Record, 0, len(changes))
	for _, c := range changes {
		b, err := c.Encode()
		if err != nil {
			return err
		}
		recs = append(recs, &kgo.Record{
			Topic: p.Topic,
			Key:   []byte(c.ListingKey),
			Value: b,
			Headers: []kgo.RecordHeader{
				{Key: "id", Value: []byte(c.ID)},
				{Key: "kind", Value: []byte(c.Kind)},
				{Key: "source", Value: []byte(c.Source)},
				{Key: "observed_at_ms", Value: []byte(strconv.FormatInt(c.ObservedAt.UnixMilli(), 10))},
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
