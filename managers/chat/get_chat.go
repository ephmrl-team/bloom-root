package chat

import (
	"context"

	"github.com/ephmrl-team/bloom-root/domain"
)

func (c *ChatManager) GetChat(ctx context.Context, chatID int) (*domain.Chat, error) {
	return c.chatClient.GetChat(ctx, chatID)
}
