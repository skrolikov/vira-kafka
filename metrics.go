//go:build !test
// +build !test

package kafka

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// DefaultMetrics содержит зарегистрированные метрики Kafka (глобально)
	DefaultMetrics *KafkaMetrics
)

// KafkaMetrics содержит все метрики для мониторинга Kafka Producer.
type KafkaMetrics struct {
	MessagesSent   *prometheus.CounterVec
	MessagesFailed *prometheus.CounterVec
}

// NewKafkaMetrics инициализирует и регистрирует метрики Kafka.
func NewKafkaMetrics(reg prometheus.Registerer) *KafkaMetrics {
	m := &KafkaMetrics{
		MessagesSent: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "kafka_messages_sent_total",
				Help: "Total number of Kafka messages successfully sent",
			},
			[]string{"topic"},
		),
		MessagesFailed: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "kafka_messages_failed_total",
				Help: "Total number of Kafka message send failures",
			},
			[]string{"topic"},
		),
	}

	reg.MustRegister(m.MessagesSent, m.MessagesFailed)
	return m
}

// RegisterAll инициализирует глобальные метрики Kafka
func RegisterAll() {
	DefaultMetrics = NewKafkaMetrics(prometheus.DefaultRegisterer)
}
