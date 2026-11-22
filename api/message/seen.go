package message

import (
	"context"

	"github.com/slipe-fun/bloom-kit/api"
	"github.com/slipe-fun/bloom-kit/domain"
)

func (m *MessageClient) Seen(ctx context.Context, markMessagesSeenRequest *domain.MarkMessagesSeenRequest) (*domain.MarkMessagesSeenResponse, error) {
	return api.Send[domain.MarkMessagesSeenRequest, domain.MarkMessagesSeenResponse](ctx, m.client, "POST", "/message/seen", markMessagesSeenRequest)
}
