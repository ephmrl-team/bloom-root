package chat

import (
	"context"

	"github.com/ephmrl-team/bloom-root/domain"
)

func (c *ChatManager) GetChats(ctx context.Context) (*[]domain.Chat, error) {
	return c.chatClient.GetChats(ctx)
}
