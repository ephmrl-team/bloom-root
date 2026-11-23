package message

import (
	"github.com/ephmrl-team/bloom-root/api"
)

type MessageClient struct {
	client *api.Client
}

func NewMessageClient(client *api.Client) *MessageClient {
	return &MessageClient{
		client: client,
	}
}
