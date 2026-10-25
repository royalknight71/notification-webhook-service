package observability

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics owns an isolated registry so constructing it cannot collide with
// process-global or test registries.
type Metrics struct {
	registry               *prometheus.Registry
	eventsAccepted         prometheus.Counter
	eventsRejected         prometheus.Counter
	deliveryAttempts       prometheus.Counter
	deliveryRetries        prometheus.Counter
	deliveryRetryExhausted prometheus.Counter
	deliveryOutcomes       *prometheus.CounterVec
	deliveryAttemptTime    prometheus.Histogram
	outboxPending          prometheus.Gauge
	consumerRunning        prometheus.Gauge
	dispatcherRunning      prometheus.Gauge
}

func NewMetrics() (*Metrics, error) {
	registry := prometheus.NewRegistry()
	metrics := &Metrics{
		registry: registry,
		eventsAccepted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "notification_events_accepted_total",
			Help: "Events durably accepted by the API.",
		}),
		eventsRejected: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "notification_events_rejected_total",
			Help: "Event requests rejected by validation or persistence.",
		}),
		deliveryAttempts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "notification_delivery_attempts_total",
			Help: "Webhook attempts started after their attempt count was persisted.",
		}),
		deliveryRetries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "notification_delivery_retries_total",
			Help: "Webhook retry attempts started after the first attempt.",
		}),
		deliveryRetryExhausted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "notification_delivery_retry_exhausted_total",
			Help: "Deliveries durably marked failed after exhausting their retry limit.",
		}),
		deliveryOutcomes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notification_delivery_outcomes_total",
			Help: "Durably recorded delivery outcomes.",
		}, []string{"outcome"}),
		deliveryAttemptTime: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "notification_delivery_attempt_duration_seconds",
			Help:    "Time spent performing an individual webhook HTTP attempt.",
			Buckets: prometheus.DefBuckets,
		}),
		outboxPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "notification_outbox_pending",
			Help: "Persisted outbox rows not yet confirmed published; periodically sampled.",
		}),
		consumerRunning: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "notification_consumer_running",
			Help: "Whether the RabbitMQ consumer goroutine is running (1) or stopped (0).",
		}),
		dispatcherRunning: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "notification_outbox_dispatcher_running",
			Help: "Whether the outbox dispatcher goroutine is running (1) or stopped (0).",
		}),
	}

	collectors := []prometheus.Collector{
		metrics.eventsAccepted,
		metrics.eventsRejected,
		metrics.deliveryAttempts,
		metrics.deliveryRetries,
		metrics.deliveryRetryExhausted,
		metrics.deliveryOutcomes,
		metrics.deliveryAttemptTime,
		metrics.outboxPending,
		metrics.consumerRunning,
		metrics.dispatcherRunning,
	}
	for _, collector := range collectors {
		if err := registry.Register(collector); err != nil {
			return nil, fmt.Errorf("register application metric: %w", err)
		}
	}
	return metrics, nil
}

func (m *Metrics) Handler() http.Handler {
	if m == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) IncEventAccepted() {
	if m != nil {
		m.eventsAccepted.Inc()
	}
}

func (m *Metrics) IncEventRejected() {
	if m != nil {
		m.eventsRejected.Inc()
	}
}

func (m *Metrics) IncDeliveryAttempt() {
	if m != nil {
		m.deliveryAttempts.Inc()
	}
}

func (m *Metrics) IncDeliveryRetry() {
	if m != nil {
		m.deliveryRetries.Inc()
	}
}

func (m *Metrics) IncDeliveryRetryExhausted() {
	if m != nil {
		m.deliveryRetryExhausted.Inc()
	}
}

func (m *Metrics) IncDeliveryOutcomeSucceeded() {
	if m != nil {
		m.deliveryOutcomes.WithLabelValues("succeeded").Inc()
	}
}

func (m *Metrics) IncDeliveryOutcomeFailed() {
	if m != nil {
		m.deliveryOutcomes.WithLabelValues("failed").Inc()
	}
}

func (m *Metrics) ObserveDeliveryAttempt(seconds float64) {
	if m != nil {
		m.deliveryAttemptTime.Observe(seconds)
	}
}

func (m *Metrics) SetOutboxPending(count int64) {
	if m != nil {
		m.outboxPending.Set(float64(count))
	}
}

func (m *Metrics) SetConsumerRunning(running bool) {
	if m != nil {
		setRunning(m.consumerRunning, running)
	}
}

func (m *Metrics) SetDispatcherRunning(running bool) {
	if m != nil {
		setRunning(m.dispatcherRunning, running)
	}
}

func setRunning(gauge prometheus.Gauge, running bool) {
	if running {
		gauge.Set(1)
		return
	}
	gauge.Set(0)
}
