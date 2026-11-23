package message

import (
	"context"

	"github.com/ephmrl-team/bloom-root/api"
	"github.com/ephmrl-team/bloom-root/domain"
)

func (m *MessageClient) Send(ctx context.Context, sendMessageRequest *domain.SendMessageRequest) (*domain.RawMessageWithReply, error) {
	return api.Send[domain.SendMessageRequest, domain.RawMessageWithReply](ctx, m.client, "POST", "/message/send", sendMessageRequest)
}
