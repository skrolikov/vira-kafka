package kafka

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	kafkaMessagesSent = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_messages_sent_total",
			Help: "Общее число сообщений, отправленных в Kafka.",
		},
		[]string{"topic"},
	)

	kafkaMessagesFailed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_messages_failed_total",
			Help: "Общее число ошибок при отправке сообщений в Kafka.",
		},
		[]string{"topic"},
	)

	kafkaMessagesReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_messages_received_total",
			Help: "Общее число сообщений, полученных из Kafka.",
		},
		[]string{"topic"},
	)

	kafkaMessagesReadFailed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_messages_read_failed_total",
			Help: "Общее число ошибок при чтении сообщений из Kafka.",
		},
		[]string{"topic"},
	)
)

// RegisterKafkaMetrics регистрирует метрики Kafka в Prometheus.
func RegisterKafkaMetrics(reg prometheus.Registerer) {
	reg.MustRegister(kafkaMessagesSent, kafkaMessagesFailed, kafkaMessagesReceived, kafkaMessagesReadFailed)
}
