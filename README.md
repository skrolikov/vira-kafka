# Vira Kafka

Пакет `vira-kafka` предоставляет удобные обёртки для работы с Kafka, включая продюсер и консьюмер с поддержкой middleware, логирования и метрик Prometheus.

## Особенности

- Производитель (Producer) с middleware цепочкой
- Потребитель (Consumer) с автоматическим логированием
- Интеграция с Prometheus метриками
- Поддержка контекста для graceful shutdown
- Интеграция с `vira-logger`
- Поддержка синхронного/асинхронного режимов отправки

## Установка

```bash
go get github.com/skrolikov/vira-kafka
```

## Использование

### Инициализация метрик

```go
import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/skrolikov/vira-kafka"
)

func main() {
    // Регистрация метрик Kafka
    kafka.RegisterKafkaMetrics(prometheus.DefaultRegisterer)
}
```

### Producer

```go
cfg := kafka.ProducerConfig{
    Brokers:      []string{"localhost:9092"},
    Topic:        "events",
    BatchTimeout: 100 * time.Millisecond,
    Async:        false,
    RequiredAcks: kafka.RequireOne,
    Compression:  kafka.Snappy,
    MaxAttempts:  3,
    Middlewares:  []kafka.MessageMiddleware{
        kafka.DebugLogMiddleware(),
    },
}

producer := kafka.NewProducer(cfg, logger)
defer producer.Close()

// Отправка сырого сообщения
err := producer.Send(ctx, "user123", []byte("message data"))

// Отправка структуры как JSON
event := struct{ UserID string }{UserID: "123"}
err = producer.SendEvent(ctx, "user123", event)
```

### Consumer

```go
cfg := kafka.ConsumerConfig{
    Brokers:  []string{"localhost:9092"},
    Topic:    "events",
    GroupID:  "my-group",
    MinBytes: 1,
    MaxBytes: 10e6, // 10MB
    MaxWait:  1 * time.Second,
}

consumer := kafka.NewConsumer(cfg, logger)
defer consumer.Close()

for {
    msg, err := consumer.ReadMessage(ctx)
    if err != nil {
        // Обработка ошибки
        continue
    }
    
    // Обработка сообщения
    fmt.Printf("Получено сообщение: %s\n", string(msg.Value))
}
```

## Конфигурация

### ProducerConfig

| Параметр       | Описание                          | По умолчанию         |
|----------------|-----------------------------------|----------------------|
| Brokers        | Список брокеров                   | -                    |
| Topic          | Топик для отправки                | -                    |
| BatchTimeout   | Таймаут батча                     | 1s                   |
| Async          | Асинхронный режим                 | false                |
| RequiredAcks   | Уровень подтверждения (RequireOne)| kafka.RequireOne     |
| Compression    | Алгоритм сжатия                   | kafka.Snappy         |
| MaxAttempts    | Макс. попыток отправки            | 3                    |
| Middlewares    | Список middleware                 | nil                  |

### ConsumerConfig

| Параметр  | Описание                          | По умолчанию |
|-----------|-----------------------------------|--------------|
| Brokers   | Список брокеров                   | -            |
| Topic     | Топик для чтения                  | -            |
| GroupID   | ID потребительской группы         | -            |
| MinBytes  | Мин. размер сообщения             | 1            |
| MaxBytes  | Макс. размер сообщения            | 10MB         |
| MaxWait   | Макс. время ожидания сообщения    | 1s           |

## Метрики Prometheus

Пакет предоставляет следующие метрики:

- `kafka_messages_sent_total` - отправленные сообщения
- `kafka_messages_failed_total` - ошибки отправки
- `kafka_messages_received_total` - полученные сообщения
- `kafka_messages_read_failed_total` - ошибки чтения

## Middleware

Пример middleware для логирования:

```go
func LoggingMiddleware() kafka.MessageMiddleware {
    return func(ctx context.Context, key string, value []byte) (string, []byte, error) {
        logger := log.FromContext(ctx)
        logger.Debug("Отправка сообщения в Kafka", "key", key, "size", len(value))
        return key, value, nil
    }
}
```

## Лучшие практики

1. Всегда закрывайте producer/consumer через `defer`
2. Используйте middleware для сквозной функциональности
3. Настройте адекватные таймауты в контексте
4. Мониторьте метрики в Grafana/Prometheus
5. Для production используйте асинхронный режим с батчингом
6. Логируйте ключевые события (отправка/получение сообщений)