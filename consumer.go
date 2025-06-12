package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	log "github.com/skrolikov/vira-logger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// ConsumerConfig содержит параметры конфигурации Kafka Consumer.
type ConsumerConfig struct {
	Brokers           []string      `json:"brokers"`
	Topic             string        `json:"topic"`
	GroupID           string        `json:"group_id"`
	MinBytes          int           `json:"min_bytes"`       // 1KB
	MaxBytes          int           `json:"max_bytes"`       // 10MB
	MaxWait           time.Duration `json:"max_wait"`        // 10s
	CommitInterval    time.Duration `json:"commit_interval"` // 0 - синхронный коммит
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`
	SessionTimeout    time.Duration `json:"session_timeout"`
	StartOffset       int64         `json:"start_offset"` // -1 = LastOffset, -2 = FirstOffset
	RetentionTime     time.Duration `json:"retention_time"`
	QueueCapacity     int           `json:"queue_capacity"`
	ReadLagInterval   time.Duration `json:"read_lag_interval"`

	// Аутентификация
	SASLUsername string `json:"-"`
	SASLPassword string `json:"-"`

	// Обработка ошибок
	MaxRetryAttempts int           `json:"max_retry_attempts"`
	RetryBackoff     time.Duration `json:"retry_backoff"`

	Metrics *KafkaMetrics `json:"-"`
	Logger  *log.Logger   `json:"-"`
	Tracer  trace.Tracer  `json:"-"`
}

// Consumer — Kafka consumer с расширенными возможностями.
type Consumer struct {
	reader         *kafka.Reader
	logger         *log.Logger
	metrics        *KafkaMetrics
	tracer         trace.Tracer
	config         ConsumerConfig
	wg             sync.WaitGroup
	closeOnce      sync.Once
	closed         chan struct{}
	messageChannel chan kafka.Message
	errorChannel   chan error
}

// NewConsumer создаёт новый Kafka consumer с расширенной конфигурацией.
func NewConsumer(cfg ConsumerConfig) *Consumer {
	if cfg.Logger == nil {
		cfg.Logger = log.DefaultLogger()
	}

	if cfg.Tracer == nil {
		cfg.Tracer = otel.Tracer("kafka-consumer")
	}

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	// Настройка SASL если указаны учетные данные
	if cfg.SASLUsername != "" && cfg.SASLPassword != "" {
		dialer.SASLMechanism = plain.Mechanism{
			Username: cfg.SASLUsername,
			Password: cfg.SASLPassword,
		}
	}

	readerConfig := kafka.ReaderConfig{
		Brokers:           cfg.Brokers,
		Topic:             cfg.Topic,
		GroupID:           cfg.GroupID,
		MinBytes:          cfg.MinBytes,
		MaxBytes:          cfg.MaxBytes,
		MaxWait:           cfg.MaxWait,
		Dialer:            dialer,
		CommitInterval:    cfg.CommitInterval,
		HeartbeatInterval: cfg.HeartbeatInterval,
		SessionTimeout:    cfg.SessionTimeout,
		RetentionTime:     cfg.RetentionTime,
		QueueCapacity:     cfg.QueueCapacity,
		ReadLagInterval:   cfg.ReadLagInterval,
	}

	switch cfg.StartOffset {
	case -2:
		readerConfig.StartOffset = kafka.FirstOffset
	case -1:
		readerConfig.StartOffset = kafka.LastOffset
	default:
		readerConfig.StartOffset = cfg.StartOffset
	}

	reader := kafka.NewReader(readerConfig)

	c := &Consumer{
		reader:         reader,
		logger:         cfg.Logger,
		metrics:        cfg.Metrics,
		tracer:         cfg.Tracer,
		config:         cfg,
		closed:         make(chan struct{}),
		messageChannel: make(chan kafka.Message, 100),
		errorChannel:   make(chan error, 10),
	}

	// Запускаем обработчик сообщений в фоне
	c.wg.Add(1)
	go c.messageHandler()

	cfg.Logger.Info("✅ Kafka consumer создан: topic=%s group=%s brokers=%v",
		cfg.Topic, cfg.GroupID, cfg.Brokers)

	return c
}

// ReadMessage читает и обрабатывает сообщение с возможностью ретраев.
func (c *Consumer) ReadMessage(ctx context.Context) (kafka.Message, error) {
	ctx, span := c.tracer.Start(ctx, "kafka.Consumer.ReadMessage")
	defer span.End()

	select {
	case <-c.closed:
		return kafka.Message{}, ErrConsumerClosed
	default:
	}

	var attempt int
	var msg kafka.Message
	var err error

	for attempt = 0; attempt <= c.config.MaxRetryAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(c.config.RetryBackoff)
		}

		msg, err = c.reader.ReadMessage(ctx)
		if err == nil {
			break
		}

		c.logger.WithContext(ctx).Error("Ошибка чтения из Kafka: attempt=%d error=%v topic=%s",
			attempt, err, c.config.Topic,
		)

		if c.metrics != nil {
			c.metrics.MessagesFailed.WithLabelValues(c.config.Topic).Inc()
		}
	}

	if err != nil {
		span.RecordError(err)
		return kafka.Message{}, fmt.Errorf("после %d попыток: %w", attempt, err)
	}

	if c.metrics != nil {
		c.metrics.MessagesProcessed.WithLabelValues(c.config.Topic).Inc()
		c.metrics.Offset.WithLabelValues(c.config.Topic).Set(float64(msg.Offset))
		c.metrics.Lag.WithLabelValues(c.config.Topic).Set(float64(c.reader.Lag()))
	}

	span.SetAttributes(
		attribute.String("kafka.topic", msg.Topic),
		attribute.Int64("kafka.offset", msg.Offset),
		attribute.Int("kafka.partition", msg.Partition),
	)

	c.logger.WithContext(ctx).Debug("Получено сообщение из Kafka: topic=%s partition=%d offset=%d key=%s value_length=%d",
		msg.Topic,
		msg.Partition,
		msg.Offset,
		string(msg.Key),
		len(msg.Value),
	)

	return msg, nil
}

// Consume запускает обработку сообщений через каналы.
func (c *Consumer) Consume(ctx context.Context) (<-chan kafka.Message, <-chan error) {
	return c.messageChannel, c.errorChannel
}

// messageHandler обрабатывает сообщения в фоне и отправляет их в канал.
func (c *Consumer) messageHandler() {
	defer c.wg.Done()

	for {
		select {
		case <-c.closed:
			return
		default:
			msg, err := c.reader.FetchMessage(context.Background())
			if err != nil {
				c.errorChannel <- fmt.Errorf("ошибка получения сообщения: %w", err)
				continue
			}

			select {
			case c.messageChannel <- msg:
				if c.metrics != nil {
					c.metrics.MessagesInQueue.WithLabelValues(c.config.Topic).Inc()
				}
			case <-c.closed:
				return
			}
		}
	}
}

// CommitMessage подтверждает обработку сообщения.
func (c *Consumer) CommitMessage(ctx context.Context, msg kafka.Message) error {
	ctx, span := c.tracer.Start(ctx, "kafka.Consumer.CommitMessage")
	defer span.End()

	err := c.reader.CommitMessages(ctx, msg)
	if err != nil {
		span.RecordError(err)
		c.logger.WithContext(ctx).Error(
			"Ошибка коммита сообщения: topic=%s partition=%d offset=%d error=%v",
			msg.Topic,
			msg.Partition,
			msg.Offset,
			err,
		)
		return fmt.Errorf("%w: %v", ErrCommitFailed, err)
	}

	if c.metrics != nil {
		c.metrics.MessagesCommitted.WithLabelValues(msg.Topic).Inc()
	}

	return nil
}

// DecodeMessage декодирует JSON сообщение в структуру.
func (c *Consumer) DecodeMessage(msg kafka.Message, v interface{}) error {
	if len(msg.Value) == 0 {
		return fmt.Errorf("%w: пустое тело сообщения", ErrInvalidMessage)
	}

	err := json.Unmarshal(msg.Value, v)
	if err != nil {
		return fmt.Errorf("%w: ошибка декодирования JSON: %v", ErrInvalidMessage, err)
	}

	return nil
}

// Close безопасно закрывает consumer.
func (c *Consumer) Close() error {
	var err error

	c.closeOnce.Do(func() {
		close(c.closed)
		c.wg.Wait()

		close(c.messageChannel)
		close(c.errorChannel)

		err = c.reader.Close()
		if err != nil {
			c.logger.Error("Ошибка при закрытии Kafka consumer: error=%v", err)
		} else {
			c.logger.Info("Kafka consumer закрыт: topic=%s group=%s",
				c.config.Topic,
				c.config.GroupID,
			)
		}
	})

	return err
}

// Stats возвращает статистику consumer.
func (c *Consumer) Stats() kafka.ReaderStats {
	return c.reader.Stats()
}

// Lag возвращает текущий lag для consumer.
func (c *Consumer) Lag() int64 {
	return c.reader.Lag()
}
