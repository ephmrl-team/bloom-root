package message

import (
	"context"
	"fmt"

	"github.com/ephmrl-team/bloom-root/api"
	"github.com/ephmrl-team/bloom-root/domain"
)

func (m *MessageClient) GetMessagesAfter(ctx context.Context, chatID int, messageID int) (*[]domain.RawMessageWithReply, error) {
	return api.Send[struct{}, []domain.RawMessageWithReply](
		ctx,
		m.client,
		"GET",
		fmt.Sprintf("/chat/%d/messages/after/%d", chatID, messageID),
		nil,
	)
}

func (m *MessageClient) GetMessagesBefore(ctx context.Context, chatID int, messageID int) (*[]domain.RawMessageWithReply, error) {
	return api.Send[struct{}, []domain.RawMessageWithReply](
		ctx,
		m.client,
		"GET",
		fmt.Sprintf("/chat/%d/messages/before/%d", chatID, messageID),
		nil,
	)
}
