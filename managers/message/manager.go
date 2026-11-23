package message

import (
	"github.com/ephmrl-team/bloom-root/api/message"
)

type MessageManager struct {
	messageClient *message.MessageClient
}

func NewMessageManager(messageClient *message.MessageClient) *MessageManager {
	return &MessageManager{
		messageClient: messageClient,
	}
}
