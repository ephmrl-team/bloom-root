package chat

import (
	"context"

	"github.com/ephmrl-team/bloom-root/api"
	"github.com/ephmrl-team/bloom-root/domain"
)

func (c *ChatClient) GetChats(ctx context.Context) (*[]domain.Chat, error) {
	return api.Send[struct{}, []domain.Chat](
		ctx,
		c.client,
		"GET",
		"/chats",
		nil,
	)
}
