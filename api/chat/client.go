package chat

import (
	"github.com/ephmrl-team/bloom-root/api"
)

type ChatClient struct {
	client *api.Client
}

func NewChatClient(client *api.Client) *ChatClient {
	return &ChatClient{
		client: client,
	}
}
