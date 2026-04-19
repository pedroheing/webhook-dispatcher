package schedule

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"webhook-dispatcher/internal/pkg/domain"
	"webhook-dispatcher/internal/schedule/mocks"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func buildScheduler(repo Repository, writer KafkaWriter) *Scheduler {
	return NewScheduler(writer, repo, Config{
		DbBatchSize:        100,
		PendingEventsTopic: "events.pending",
	})
}

func TestProcessRetries_NoEventsDue(t *testing.T) {
	repo := mocks.NewMockRepository(t)
	writer := mocks.NewMockKafkaWriter(t)

	repo.EXPECT().FetchDueRetries(mock.Anything, int64(100), mock.Anything).Return(nil, nil)

	s := buildScheduler(repo, writer)
	s.processRetries(context.Background())
}

func TestProcessRetries_PublishesAndMarksPending(t *testing.T) {
	repo := mocks.NewMockRepository(t)
	writer := mocks.NewMockKafkaWriter(t)

	events := []domain.Event{
		{ID: "ev_1", WebhookID: "wh_1", EventType: "user.created", Data: json.RawMessage(`{"a":1}`), Status: domain.EventStatusFailed},
		{ID: "ev_2", WebhookID: "wh_2", EventType: "order.paid", Data: json.RawMessage(`{"b":2}`), Status: domain.EventStatusFailed},
	}

	repo.EXPECT().FetchDueRetries(mock.Anything, mock.Anything, mock.Anything).Return(events, nil)

	var captured []kafka.Message
	writer.EXPECT().WriteMessages(mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, msgs ...kafka.Message) { captured = msgs }).
		Return(nil)

	repo.EXPECT().MarkPending(mock.Anything, []string{"ev_1", "ev_2"}).Return(nil)

	s := buildScheduler(repo, writer)
	s.processRetries(context.Background())

	require.Len(t, captured, 2)
	for i, msg := range captured {
		assert.Equal(t, "events.pending", msg.Topic)
		assert.Equal(t, events[i].ID, string(msg.Key))

		var ev domain.Event
		require.NoError(t, json.Unmarshal(msg.Value, &ev))
		assert.Equal(t, domain.EventStatusPending, ev.Status, "status must be re-published as pending")
	}
}

func TestProcessRetries_FetchError_NoWrites(t *testing.T) {
	repo := mocks.NewMockRepository(t)
	writer := mocks.NewMockKafkaWriter(t)

	repo.EXPECT().FetchDueRetries(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("mongo down"))

	s := buildScheduler(repo, writer)
	s.processRetries(context.Background())
}

func TestProcessRetries_WriterError_DoesNotMarkPending(t *testing.T) {
	repo := mocks.NewMockRepository(t)
	writer := mocks.NewMockKafkaWriter(t)

	events := []domain.Event{{ID: "ev_1", WebhookID: "wh_1"}}
	repo.EXPECT().FetchDueRetries(mock.Anything, mock.Anything, mock.Anything).Return(events, nil)
	writer.EXPECT().WriteMessages(mock.Anything, mock.Anything).Return(errors.New("kafka down"))

	s := buildScheduler(repo, writer)
	s.processRetries(context.Background())
}
