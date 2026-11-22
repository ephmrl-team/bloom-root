package client

import (
	"encoding/json"
	"fmt"
)

func (c *BloomClient) filterMessagesToMarkSeen(chatID int, messageIDs []int, userID string) []int {
	var filtered []int

	for _, messageID := range messageIDs {
		message, err := c.database.GetMessage(messageID)
		if err != nil || message == nil {
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
	creds, err := c.getCredentials()
	if err != nil {
		return nil, err
	}

	filtered := c.filterMessagesToMarkSeen(chatID, messageIDs, creds.UserID)
	if len(filtered) == 0 {
		return []byte("[]"), nil
	}

	seen, err := c.messageManager.Seen(token.context(), chatID, filtered)
	if err != nil {
		return nil, err
	}

	if err := c.database.UpdateMessagesSeen(seen.ChatID, seen.Messages, seen.SeenAt); err != nil {
		fmt.Println("failed to update messages seen:", err)
	}

	c.notifyChatsUpdated()

	return json.Marshal(seen)
}

func (c *BloomClient) RegisterMessagesSeenListener(listener MessagesSeenListener) {
	c.listenerMu.Lock()
	c.messagesSeenListener = listener
	c.listenerMu.Unlock()
}

func (c *BloomClient) UnregisterMessagesSeenListener() {
	c.listenerMu.Lock()
	c.messagesSeenListener = nil
	c.listenerMu.Unlock()
}
