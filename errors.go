package kafka

import "errors"

var (
	ErrConsumerClosed = errors.New("kafka consumer закрыт")
	ErrCommitFailed   = errors.New("не удалось зафиксировать смещение")
	ErrInvalidMessage = errors.New("недопустимый формат сообщения")
)
