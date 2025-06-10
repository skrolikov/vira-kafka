package kafka

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
	log "github.com/skrolikov/vira-logger"
)

// MessageMiddleware позволяет модифицировать сообщение перед отправкой.
type MessageMiddleware func(ctx context.Context, key string, value []byte) (string, []byte, error)

// ProducerConfig содержит расширенные параметры конфигурации Kafka Producer.
type ProducerConfig struct {
	Brokers      []string
	Topic        string
	BatchTimeout time.Duration
	Async        bool

	RequiredAcks kafka.RequiredAcks
	Compression  kafka.Compression
	MaxAttempts  int

	Middlewares []MessageMiddleware
}

// Producer — Kafka producer с middleware и логгированием.
type Producer struct {
	writer      *kafka.Writer
	logger      *log.Logger
	middlewares []MessageMiddleware
}

// NewProducer создаёт нового Kafka producer по конфигурации.
func NewProducer(cfg ProducerConfig, logger *log.Logger) *Producer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: cfg.RequiredAcks,
		Async:        cfg.Async,
		BatchTimeout: cfg.BatchTimeout,
		Compression:  cfg.Compression,
		MaxAttempts:  cfg.MaxAttempts,
	}

	logger.Info("✅ Kafka producer создан для topic: %s", cfg.Topic)

	return &Producer{
		writer:      w,
		logger:      logger,
		middlewares: cfg.Middlewares,
	}
}

// Send отправляет сообщение в Kafka, проходя через все middleware.
func (p *Producer) Send(ctx context.Context, key string, value []byte) error {
	var err error
	for _, m := range p.middlewares {
		key, value, err = m(ctx, key, value)
		if err != nil {
			p.logger.WithContext(ctx).Error("❌ Middleware ошибка: %v", err)
			return err
		}
	}

	msg := kafka.Message{
		Key:   []byte(key),
		Value: value,
		Time:  time.Now(),
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err = p.writer.WriteMessages(ctx, msg)
	if err != nil {
		kafkaMessagesFailed.WithLabelValues(p.writer.Topic).Inc()
		p.logger.WithContext(ctx).Error("❌ Ошибка отправки сообщения в Kafka: %v", err)
		return err
	}

	kafkaMessagesSent.WithLabelValues(p.writer.Topic).Inc()
	p.logger.WithContext(ctx).Debug("📤 Сообщение отправлено: key=%s", key)
	return nil
}

// SendEvent сериализует и отправляет событие.
func (p *Producer) SendEvent(ctx context.Context, key string, event any) error {
	data, err := json.Marshal(event)
	if err != nil {
		p.logger.WithContext(ctx).Error("❌ Не удалось сериализовать событие: %v", err)
		return err
	}
	return p.Send(ctx, key, data)
}

// Close закрывает Kafka writer.
func (p *Producer) Close() error {
	err := p.writer.Close()
	if err != nil {
		p.logger.Error("❌ Ошибка при закрытии Kafka producer: %v", err)
	} else {
		p.logger.Info("🔌 Kafka producer закрыт")
	}
	return err
}
