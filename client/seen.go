package client

import (
	"encoding/json"

	"github.com/ephmrl-team/bloom-root/logger"
)

func (c *BloomClient) filterMessagesToMarkSeen(chatID int, messageIDs []int, userID string) []int {
	var filtered []int

	for _, messageID := range messageIDs {
		message, err := c.database.GetMessage(messageID)
		if err != nil {
			logger.Warn("seen", "message lookup failed", "message_id", messageID, "error", err)
			continue
		}
		if message == nil {
			continue
		}

		if message.ChatID != chatID || message.AuthorID == userID || message.Seen != nil {
			continue
		}

		filtered = append(filtered, messageID)
	}

	return filtered
}

func (c *BloomClient) MarkMessagesSeen(token *CancelToken, chatID int, messageIDs []int) ([]byte, error) {
	logger.Info("seen", "marking messages seen", "chat_id", chatID, "count", len(messageIDs))

	creds, err := c.getCredentials()
	if err != nil {
		logger.Error("seen", "mark seen failed", "chat_id", chatID, "error", err)
		return nil, err
	}

	filtered := c.filterMessagesToMarkSeen(chatID, messageIDs, creds.UserID)
	logger.Info("seen", "messages filtered", "chat_id", chatID, "requested", len(messageIDs), "filtered", len(filtered))

	if len(filtered) == 0 {
		logger.Info("seen", "nothing to mark seen", "chat_id", chatID)
		return []byte("[]"), nil
	}

	seen, err := c.messageManager.Seen(token.context(), chatID, filtered)
	if err != nil {
		logger.Error("seen", "mark seen failed", "chat_id", chatID, "count", len(filtered), "error", err)
		return nil, err
	}

	if err := c.database.UpdateMessagesSeen(seen.ChatID, seen.Messages, seen.SeenAt); err != nil {
		logger.Warn("seen", "local seen update failed", "chat_id", seen.ChatID, "count", len(seen.Messages), "error", err)
	}

	c.notifyChatsUpdated()

	seenJSON, err := json.Marshal(seen)
	if err != nil {
		logger.Error("seen", "seen marshal failed", "chat_id", seen.ChatID, "error", err)
		return nil, err
	}

	logger.Success("seen", "messages marked seen", "chat_id", seen.ChatID, "count", len(seen.Messages))

	return seenJSON, nil
}

func (c *BloomClient) RegisterMessagesSeenListener(listener MessagesSeenListener) {
	c.listenerMu.Lock()
	c.messagesSeenListener = listener
	c.listenerMu.Unlock()

	logger.Info("seen", "messages seen listener registered")
}

func (c *BloomClient) UnregisterMessagesSeenListener() {
	c.listenerMu.Lock()
	c.messagesSeenListener = nil
	c.listenerMu.Unlock()

	logger.Info("seen", "messages seen listener unregistered")
}
