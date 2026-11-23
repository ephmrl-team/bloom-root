package message

import (
	"encoding/base64"

	"github.com/ephmrl-team/bloom-root/domain"
	"github.com/slipe-fun/skid-v4/pkg/messages"
)

func NewSendMessageRequest(clientMessageID string, chatID int, replyToID *int, encryptedMessage messages.EncryptedMessage) *domain.SendMessageRequest {
	return &domain.SendMessageRequest{
		Version:         base64.StdEncoding.EncodeToString(encryptedMessage.Version),
		Ciphertext:      base64.StdEncoding.EncodeToString(encryptedMessage.Ciphertext),
		Nonce:           base64.StdEncoding.EncodeToString(encryptedMessage.Nonce),
		Salt:            base64.StdEncoding.EncodeToString(encryptedMessage.Salt),
		ChatID:          chatID,
		ReplyTo:         replyToID,
		ClientMessageID: clientMessageID,
	}
}
