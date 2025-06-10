package kafka

import (
	"context"
	"fmt"
)

// DebugLogMiddleware — пример middleware для логгирования ключей.
func DebugLogMiddleware() func(ctx context.Context, key string, value []byte) (string, []byte, error) {
	return func(ctx context.Context, key string, value []byte) (string, []byte, error) {
		fmt.Printf("📎 Kafka debug: key=%s size=%d\n", key, len(value))
		return key, value, nil
	}
}
