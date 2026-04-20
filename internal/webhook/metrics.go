package webhook

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var eventsCreatedTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "webhook_events_created_total",
	Help: "Total number of events successfully created and published.",
})
