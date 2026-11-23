package message

import (
	"context"

	"github.com/ephmrl-team/bloom-root/api"
	"github.com/ephmrl-team/bloom-root/domain"
)

func (m *MessageClient) Seen(ctx context.Context, markMessagesSeenRequest *domain.MarkMessagesSeenRequest) (*domain.MarkMessagesSeenResponse, error) {
	return api.Send[domain.MarkMessagesSeenRequest, domain.MarkMessagesSeenResponse](ctx, m.client, "POST", "/message/seen", markMessagesSeenRequest)
}
