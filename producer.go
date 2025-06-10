package kafka

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	log "github.com/skrolikov/vira-logger"
)

// ProducerConfig содержит параметры конфигурации продюсера Kafka.
type ProducerConfig struct {
	Brokers      []string
	Topic        string
	BatchTimeout time.Duration
	Async        bool
}

// Producer — обёртка над kafka.Writer.
type Producer struct {
	writer *kafka.Writer
	logger *log.Logger
}

// NewProducer создаёт нового Kafka producer.
func NewProducer(cfg ProducerConfig, logger *log.Logger) *Producer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        cfg.Async,
		BatchTimeout: cfg.BatchTimeout,
	}
	logger.Info("✅ Kafka producer создан для topic: %s", cfg.Topic)

	return &Producer{writer: w, logger: logger}
}

// Send отправляет сообщение в Kafka.
func (p *Producer) Send(ctx context.Context, key string, value []byte) error {
	msg := kafka.Message{
		Key:   []byte(key),
		Value: value,
		Time:  time.Now(),
	}
	err := p.writer.WriteMessages(ctx, msg)
	if err != nil {
		p.logger.Error("❌ Ошибка отправки сообщения в Kafka: %v", err)
		return err
	}
	p.logger.Debug("📤 Отправлено сообщение в Kafka: key=%s", key)
	return nil
}

// Close закрывает writer Kafka.
func (p *Producer) Close() error {
	err := p.writer.Close()
	if err != nil {
		p.logger.Error("❌ Ошибка при закрытии Kafka producer: %v", err)
	} else {
		p.logger.Info("🔌 Kafka producer закрыт")
	}
	return err
}
