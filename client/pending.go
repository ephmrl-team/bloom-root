package client

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/ephmrl-team/bloom-root/domain"
	"github.com/google/uuid"
)

const (
	pendingSendTTL       = 10 * time.Minute
	pendingSendRetention = 24 * time.Hour
)

type inflightSend struct {
	done    chan struct{}
	message *domain.DecryptedMessageWithReply
	err     error
}

func pendingSendHash(chatID, replyTo int, content string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d:%d:%s", chatID, replyTo, content)))
	return hex.EncodeToString(sum[:])
}

func replyToValue(replyTo *int) int {
	if replyTo == nil {
		return 0
	}

	return *replyTo
}

func (c *BloomClient) pendingClientMessageID(chatID int, replyTo *int, content string) (string, error) {
	replyToID := replyToValue(replyTo)

	pending, err := c.database.GetPendingSend(
		chatID,
		replyToID,
		pendingSendHash(chatID, replyToID, content),
		time.Now().UTC().Add(-pendingSendTTL),
	)
	if err != nil {
		return "", err
	}
	if pending != nil {
		return pending.ClientMessageID, nil
	}

	return uuid.NewString(), nil
}

func (c *BloomClient) rememberPendingSend(chatID int, replyTo *int, content, clientMessageID string) error {
	replyToID := replyToValue(replyTo)

	return c.database.SavePendingSend(&domain.PendingSend{
		ChatID:          chatID,
		ReplyTo:         replyToID,
		ContentHash:     pendingSendHash(chatID, replyToID, content),
		ClientMessageID: clientMessageID,
		CreatedAt:       time.Now().UTC(),
	})
}

func (c *BloomClient) forgetPendingSend(chatID int, replyTo *int, content string) error {
	replyToID := replyToValue(replyTo)

	return c.database.DeletePendingSend(
		chatID,
		replyToID,
		pendingSendHash(chatID, replyToID, content),
	)
}
