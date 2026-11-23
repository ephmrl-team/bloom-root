package message

import (
	"context"

	"github.com/ephmrl-team/bloom-root/domain"
)

func (m *MessageManager) Seen(ctx context.Context, chatID int, messageIDs []int) (*domain.MarkMessagesSeenResponse, error) {
	markMessagesSeenRequest := &domain.MarkMessagesSeenRequest{
		ChatID:   chatID,
		Messages: messageIDs,
	}

	return m.messageClient.Seen(ctx, markMessagesSeenRequest)
}
