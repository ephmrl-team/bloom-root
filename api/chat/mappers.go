package chat

import (
	"github.com/ephmrl-team/bloom-root/domain"
	"github.com/ephmrl-team/bloom-root/mappers"
	"github.com/slipe-fun/skid-v4/pkg/identity"
)

func NewCreateChatRequest(recipientID string, handshake *identity.HandshakePayload) *domain.CreateChatRequest {
	return &domain.CreateChatRequest{
		Recipient: recipientID,
		Handshake: mappers.MapHandshake(handshake),
	}
}

func NewCreateGroupRequest(title string, members []domain.GroupMemberRequest) *domain.CreateChatRequest {
	return &domain.CreateChatRequest{
		Type:    "group",
		Title:   title,
		Members: members,
	}
}
