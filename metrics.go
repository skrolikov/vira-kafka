//go:build !test
// +build !test

package kafka

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// DefaultMetrics содержит зарегистрированные метрики Kafka (глобально)
	DefaultMetrics *KafkaMetrics
)

// KafkaMetrics содержит все метрики для мониторинга Kafka Producer и Consumer.
type KafkaMetrics struct {
	// Producer metrics
	MessagesSent      *prometheus.CounterVec
	MessagesFailed    *prometheus.CounterVec
	MessageSizeBytes  *prometheus.HistogramVec
	BatchSizeMessages *prometheus.HistogramVec
	BatchLatencyMs    *prometheus.HistogramVec

	// Consumer metrics
	MessagesProcessed *prometheus.CounterVec
	MessagesCommitted *prometheus.CounterVec
	ProcessingTime    *prometheus.HistogramVec
	Offset            *prometheus.GaugeVec
	Lag               *prometheus.GaugeVec
	MessagesInQueue   *prometheus.GaugeVec
	RebalancesTotal   *prometheus.CounterVec
	RebalanceTimeMs   *prometheus.HistogramVec

	// Connection metrics
	ConnectionsActive *prometheus.GaugeVec
	ConnectionErrors  *prometheus.CounterVec
}

// NewKafkaMetrics инициализирует и регистрирует метрики Kafka.
func NewKafkaMetrics(reg prometheus.Registerer) *KafkaMetrics {
	const (
		namespace = "kafka"
		msBuckets = 10      // Количество бакетов для гистограмм в миллисекундах
		sizeRange = 1 << 26 // 64MB для максимального размера сообщения
	)

	m := &KafkaMetrics{
		// Producer metrics
		MessagesSent: promauto.With(reg).NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "messages_sent_total",
				Help:      "Total number of Kafka messages successfully sent",
			},
			[]string{"topic"},
		),
		MessagesFailed: promauto.With(reg).NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "messages_failed_total",
				Help:      "Total number of Kafka message send failures",
			},
			[]string{"topic", "error_type"},
		),
		MessageSizeBytes: promauto.With(reg).NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "message_size_bytes",
				Help:      "Size of Kafka messages in bytes",
				Buckets:   prometheus.ExponentialBuckets(100, 10, 7), // 100B to 1MB
			},
			[]string{"topic"},
		),
		BatchSizeMessages: promauto.With(reg).NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "batch_size_messages",
				Help:      "Number of messages per batch",
				Buckets:   prometheus.LinearBuckets(1, 5, 20), // 1 to 100
			},
			[]string{"topic"},
		),
		BatchLatencyMs: promauto.With(reg).NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "batch_latency_ms",
				Help:      "Time to fill and send a batch in milliseconds",
				Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 512ms
			},
			[]string{"topic"},
		),

		// Consumer metrics
		MessagesProcessed: promauto.With(reg).NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "messages_processed_total",
				Help:      "Total number of Kafka messages processed",
			},
			[]string{"topic", "status"},
		),
		MessagesCommitted: promauto.With(reg).NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "messages_committed_total",
				Help:      "Total number of Kafka messages committed",
			},
			[]string{"topic"},
		),
		ProcessingTime: promauto.With(reg).NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "processing_time_ms",
				Help:      "Time to process a message in milliseconds",
				Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1ms to 512ms
			},
			[]string{"topic"},
		),
		Offset: promauto.With(reg).NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "consumer_offset",
				Help:      "Current consumer offset",
			},
			[]string{"topic", "partition"},
		),
		Lag: promauto.With(reg).NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "consumer_lag",
				Help:      "Current consumer lag (messages behind)",
			},
			[]string{"topic", "partition"},
		),
		MessagesInQueue: promauto.With(reg).NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "messages_in_queue",
				Help:      "Number of messages waiting in internal queue",
			},
			[]string{"topic"},
		),
		RebalancesTotal: promauto.With(reg).NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "rebalances_total",
				Help:      "Total number of consumer group rebalances",
			},
			[]string{"topic", "group"},
		),
		RebalanceTimeMs: promauto.With(reg).NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "rebalance_time_ms",
				Help:      "Time taken for consumer group rebalance in milliseconds",
				Buckets:   prometheus.LinearBuckets(0, 100, 10), // 0 to 1s
			},
			[]string{"topic", "group"},
		),

		// Connection metrics
		ConnectionsActive: promauto.With(reg).NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "connections_active",
				Help:      "Number of active connections to Kafka brokers",
			},
			[]string{"broker"},
		),
		ConnectionErrors: promauto.With(reg).NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "connection_errors_total",
				Help:      "Total number of connection errors",
			},
			[]string{"broker", "error_type"},
		),
	}

	return m
}

// RegisterAll инициализирует глобальные метрики Kafka.
func RegisterAll() {
	DefaultMetrics = NewKafkaMetrics(prometheus.DefaultRegisterer)
}

// ObserveProcessingTime регистрирует время обработки сообщения.
func (m *KafkaMetrics) ObserveProcessingTime(topic string, start time.Time) {
	if m == nil {
		return
	}
	duration := time.Since(start).Milliseconds()
	m.ProcessingTime.WithLabelValues(topic).Observe(float64(duration))
}

// RecordMessageSize регистрирует размер сообщения.
func (m *KafkaMetrics) RecordMessageSize(topic string, size int) {
	if m == nil {
		return
	}
	m.MessageSizeBytes.WithLabelValues(topic).Observe(float64(size))
}

// IncrementRebalance регистрирует событие ребаланса.
func (m *KafkaMetrics) IncrementRebalance(topic, group string, duration time.Duration) {
	if m == nil {
		return
	}
	m.RebalancesTotal.WithLabelValues(topic, group).Inc()
	m.RebalanceTimeMs.WithLabelValues(topic, group).Observe(float64(duration.Milliseconds()))
}
