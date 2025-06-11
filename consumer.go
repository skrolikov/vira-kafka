package kafka

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	log "github.com/skrolikov/vira-logger"
)

// ConsumerConfig содержит параметры конфигурации Kafka Consumer.
type ConsumerConfig struct {
	Brokers  []string
	Topic    string
	GroupID  string
	MinBytes int
	MaxBytes int
	MaxWait  time.Duration

	Metrics *KafkaMetrics
}

// Consumer — Kafka consumer с логгированием и метриками.
type Consumer struct {
	reader  *kafka.Reader
	logger  *log.Logger
	metrics *KafkaMetrics
	topic   string
}

// NewConsumer создаёт новый Kafka consumer.
func NewConsumer(cfg ConsumerConfig, logger *log.Logger) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		Topic:       cfg.Topic,
		GroupID:     cfg.GroupID,
		MinBytes:    cfg.MinBytes,
		MaxBytes:    cfg.MaxBytes,
		MaxWait:     cfg.MaxWait,
		StartOffset: kafka.LastOffset,
	})

	logger.Info("✅ Kafka consumer создан для topic: %s, group: %s", cfg.Topic, cfg.GroupID)

	return &Consumer{
		reader:  reader,
		logger:  logger,
		metrics: cfg.Metrics,
		topic:   cfg.Topic,
	}
}

// ReadMessage читает одно сообщение из Kafka, обновляет метрики и логирует.
func (c *Consumer) ReadMessage(ctx context.Context) (kafka.Message, error) {
	msg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		if c.metrics != nil {
			c.metrics.MessagesFailed.WithLabelValues(c.topic).Inc()
		}
		c.logger.WithContext(ctx).Error("❌ Ошибка чтения из Kafka: %v", err)
		return kafka.Message{}, err
	}

	if c.metrics != nil {
		c.metrics.MessagesSent.WithLabelValues(c.topic).Inc()
	}
	c.logger.WithContext(ctx).Debug(
		"📥 Получено сообщение: topic=%s partition=%d offset=%d",
		msg.Topic, msg.Partition, msg.Offset,
	)

	return msg, nil
}

// Close закрывает Kafka consumer.
func (c *Consumer) Close() error {
	err := c.reader.Close()
	if err != nil {
		c.logger.Error("❌ Ошибка при закрытии Kafka consumer: %v", err)
	} else {
		c.logger.Info("🔌 Kafka consumer закрыт")
	}
	return err
}
