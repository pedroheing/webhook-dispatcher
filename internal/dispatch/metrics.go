package dispatch

import (
	"webhook-dispatcher/internal/pkg/domain"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var eventsProcessedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "dispatcher_events_processed_total",
	Help: "Total number of events processed by the dispatcher, by final outcome.",
}, []string{"outcome"})

func recordOutcomeMetric(status domain.EventStatus) {
	eventsProcessedTotal.WithLabelValues(string(status)).Inc()
}
