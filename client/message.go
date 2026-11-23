package client

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/ephmrl-team/bloom-root/domain"
	messageManager "github.com/ephmrl-team/bloom-root/managers/message"
	"github.com/ephmrl-team/bloom-root/mappers"
)

func (c *BloomClient) loadMessagesFromStorage(chatID, beforeID int) ([]domain.DecryptedMessageWithReply, error) {
	if _, err := c.getCredentials(); err != nil {
		return nil, err
	}

	messagesFromStorage, err := c.database.GetMessages(chatID, beforeID, 20)
	if err != nil {
		return nil, err
	}

	return mappers.MapDomainMessagesToDecrypted(messagesFromStorage), nil
}

func (c *BloomClient) getChatMessages(ctx context.Context, chatID, messageID int, direction messageManager.Direction) ([]domain.Message, error) {
	creds, err := c.getCredentials()
	if err != nil {
		return nil, err
	}

	chat, err := c.getChatContext(ctx, chatID)
	if err != nil {
		return nil, err
	}
	if chat == nil {
		return nil, fmt.Errorf("%w: chat %d not found", domain.ErrInvalidChat, chatID)
	}

	recipient, err := getChatRecipient(&domain.Chat{
		RawChat: chat.RawChat,
	}, creds.UserID)
	if err != nil {
		return nil, err
	}

	recipientIdentity, err := mappers.ConvertUserToIdentity(recipient)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to build recipient identity: %w", domain.ErrInvalidUserIdentity, err)
	}

	userIdentity, err := mappers.ConvertCredentialsToUser(creds)
	if err != nil {
		return nil, err
	}

	messages, err := c.messageManager.GetMessages(
		ctx,
		chatID,
		messageID,
		direction,
		chat.ChatKey,
		chat.SyncKey,
		userIdentity,
		recipientIdentity,
	)
	if err != nil {
		return nil, err
	}

	sort.SliceStable(messages, func(i, j int) bool {
		return messages[i].ID < messages[j].ID
	})

	return messages, nil
}

func (c *BloomClient) getChatMessagesBefore(ctx context.Context, chatID, beforeID int) ([]domain.Message, error) {
	return c.getChatMessages(ctx, chatID, beforeID, messageManager.DirectionBefore)
}

func (c *BloomClient) getChatMessagesAfter(ctx context.Context, chatID, afterID int) ([]domain.Message, error) {
	return c.getChatMessages(ctx, chatID, afterID, messageManager.DirectionAfter)
}

func (c *BloomClient) loadMessagesFromServer(ctx context.Context, chatID, beforeID int) ([]domain.Message, error) {
	messages, err := c.getChatMessagesBefore(ctx, chatID, beforeID)
	if err != nil {
		return nil, err
	}

	if len(messages) == 0 {
		return messages, nil
	}

	if err := c.database.SaveMessages(messages); err != nil {
		return nil, err
	}

	oldestContiguousID, err := c.database.GetChatOldestContiguousID(chatID)
	if err != nil {
		return nil, err
	}
	newestContiguousID, err := c.database.GetChatNewestContiguousID(chatID)
	if err != nil {
		return nil, err
	}

	oldestMessageID := messages[0].ID
	newestMessageID := messages[len(messages)-1].ID

	if oldestContiguousID == 0 || newestContiguousID == 0 {
		oldestContiguousID = oldestMessageID
		newestContiguousID = newestMessageID
	} else {
		if oldestMessageID < oldestContiguousID &&
			(beforeID == oldestContiguousID || newestMessageID >= oldestContiguousID) {
			oldestContiguousID = oldestMessageID
		}

		if oldestMessageID <= newestContiguousID && newestMessageID > newestContiguousID {
			newestContiguousID = newestMessageID
		}
	}

	if err := c.database.SetChatOldestContiguousID(chatID, oldestContiguousID); err != nil {
		return nil, err
	}
	if err := c.database.SetChatNewestContiguousID(chatID, newestContiguousID); err != nil {
		return nil, err
	}

	return messages, nil
}

func (c *BloomClient) fillMessagesGap(ctx context.Context, chatID, afterID, stopBeforeID int) bool {
	if afterID <= 0 || stopBeforeID <= 0 || afterID >= stopBeforeID {
		return false
	}

	cursor := afterID

	for cursor < stopBeforeID {
		page, err := c.getChatMessagesAfter(ctx, chatID, cursor)
		if err != nil || len(page) == 0 {
			return false
		}

		if err := c.database.SaveMessages(page); err != nil {
			return false
		}

		cursor = page[len(page)-1].ID

		if err := c.database.SetChatNewestContiguousID(chatID, cursor); err != nil {
			return false
		}
	}

	return true
}

func (c *BloomClient) loadInitialMessages(ctx context.Context, chatID int) ([]domain.Message, error) {
	messages, err := c.getChatMessagesAfter(ctx, chatID, 0)
	if err != nil {
		return c.database.GetMessages(chatID, 0, 20)
	}

	if len(messages) == 0 {
		return messages, nil
	}

	if err := c.database.SaveMessages(messages); err != nil {
		return nil, err
	}

	oldestContiguousID, err := c.database.GetChatOldestContiguousID(chatID)
	if err != nil {
		return nil, err
	}
	newestContiguousID, err := c.database.GetChatNewestContiguousID(chatID)
	if err != nil {
		return nil, err
	}

	oldestMessageID := messages[0].ID
	newestMessageID := messages[len(messages)-1].ID

	switch {
	case oldestContiguousID == 0 || newestContiguousID == 0:
		oldestContiguousID = oldestMessageID
		newestContiguousID = newestMessageID
	case oldestMessageID <= newestContiguousID && newestMessageID >= oldestContiguousID:
		if oldestMessageID < oldestContiguousID {
			oldestContiguousID = oldestMessageID
		}
		if newestMessageID > newestContiguousID {
			newestContiguousID = newestMessageID
		}
	case oldestMessageID > newestContiguousID:
		gapFilled := c.fillMessagesGap(ctx, chatID, newestContiguousID, oldestMessageID)

		newestContiguousID, err = c.database.GetChatNewestContiguousID(chatID)
		if err != nil {
			return nil, err
		}

		if gapFilled && newestMessageID > newestContiguousID {
			newestContiguousID = newestMessageID
		}
	}

	if err := c.database.SetChatOldestContiguousID(chatID, oldestContiguousID); err != nil {
		return nil, err
	}
	if err := c.database.SetChatNewestContiguousID(chatID, newestContiguousID); err != nil {
		return nil, err
	}

	return messages, nil
}

func (c *BloomClient) loadMessagesAuto(ctx context.Context, chatID, beforeID int) ([]domain.Message, error) {
	localMessages, err := c.database.GetMessages(chatID, beforeID, 20)
	if err != nil {
		return nil, err
	}

	if len(localMessages) > 0 {
		oldestContiguousID, err := c.database.GetChatOldestContiguousID(chatID)
		if err != nil {
			return nil, err
		}
		newestContiguousID, err := c.database.GetChatNewestContiguousID(chatID)
		if err != nil {
			return nil, err
		}

		if oldestContiguousID > 0 &&
			newestContiguousID >= oldestContiguousID &&
			beforeID <= newestContiguousID &&
			localMessages[0].ID >= oldestContiguousID &&
			len(localMessages) == 20 {
			return localMessages, nil
		}
	}

	messages, err := c.loadMessagesFromServer(ctx, chatID, beforeID)
	if err != nil {
		return nil, err
	}

	if len(messages) == 0 {
		return localMessages, nil
	}

	return messages, nil
}

func (c *BloomClient) sendMessage(ctx context.Context, chatID int, replyToID *int, content string) (*domain.DecryptedMessageWithReply, error) {
	key := pendingSendHash(chatID, replyToValue(replyToID), content)

	c.sendMu.Lock()
	if inflight, ok := c.sendInflight[key]; ok {
		c.sendMu.Unlock()

		select {
		case <-inflight.done:
			return inflight.message, inflight.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	inflight := &inflightSend{done: make(chan struct{})}
	c.sendInflight[key] = inflight
	c.sendMu.Unlock()

	defer func() {
		close(inflight.done)

		c.sendMu.Lock()
		delete(c.sendInflight, key)
		c.sendMu.Unlock()
	}()

	inflight.message, inflight.err = c.sendMessageOnce(ctx, chatID, replyToID, content)

	return inflight.message, inflight.err
}

func (c *BloomClient) sendMessageOnce(ctx context.Context, chatID int, replyToID *int, content string) (*domain.DecryptedMessageWithReply, error) {
	creds, err := c.getCredentials()
	if err != nil {
		return nil, err
	}

	chat, err := c.getChatContext(ctx, chatID)
	if err != nil {
		return nil, err
	}
	if chat == nil {
		return nil, fmt.Errorf("%w: chat %d not found", domain.ErrInvalidChat, chatID)
	}

	recipient, err := getChatRecipient(&domain.Chat{
		RawChat: chat.RawChat,
	}, creds.UserID)
	if err != nil {
		return nil, err
	}

	recipientIdentity, err := mappers.ConvertUserToIdentity(recipient)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to build recipient identity: %w", domain.ErrInvalidUserIdentity, err)
	}

	userIdentity, err := mappers.ConvertCredentialsToUser(creds)
	if err != nil {
		return nil, err
	}

	clientMessageID, err := c.pendingClientMessageID(chatID, replyToID, content)
	if err != nil {
		return nil, err
	}

	message, err := c.messageManager.Send(ctx, clientMessageID, content, chatID, replyToID, chat.ChatKey, chat.SyncKey, userIdentity, recipientIdentity)
	if err != nil {
		if rememberErr := c.rememberPendingSend(chatID, replyToID, content, clientMessageID); rememberErr != nil {
			return nil, fmt.Errorf("%w: failed to remember pending send: %w", err, rememberErr)
		}
		return nil, err
	}

	if err := c.database.SaveMessage(message); err != nil {
		fmt.Println("failed to save message:", err)
	}

	if err := c.forgetPendingSend(chatID, replyToID, content); err != nil {
		return nil, err
	}

	c.notifyChatsUpdated()

	var replyTo *domain.DecryptedMessage
	if message.ReplyToMessage != nil {
		replyTo = &domain.DecryptedMessage{
			ID:        message.ReplyToMessage.ID,
			Content:   string(message.ReplyToMessage.Content),
			AuthorID:  message.ReplyToMessage.AuthorID,
			Timestamp: message.ReplyToMessage.Timestamp,
			CreatedAt: message.ReplyToMessage.CreatedAt,
			Seen:      message.ReplyToMessage.Seen,
		}
	}

	return &domain.DecryptedMessageWithReply{
		DecryptedMessage: domain.DecryptedMessage{
			ID:        message.ID,
			Content:   content,
			AuthorID:  userIdentity.ID,
			Timestamp: message.Timestamp,
			CreatedAt: message.CreatedAt,
			Seen:      message.Seen,
		},
		ReplyTo: replyTo,
	}, nil
}

func (c *BloomClient) SendMessage(token *CancelToken, chatID int, replyToID int, content string) ([]byte, error) {
	if _, err := c.getCredentials(); err != nil {
		return nil, err
	}

	var replyToPtr *int
	if replyToID >= 0 {
		replyToPtr = &replyToID
	}

	message, err := c.sendMessage(token.context(), chatID, replyToPtr, content)
	if err != nil {
		return nil, err
	}

	return json.Marshal(message)
}

func (c *BloomClient) LoadMessages(token *CancelToken, sourceType string, chatID, beforeID int) ([]domain.DecryptedMessageWithReply, error) {
	if _, err := c.getCredentials(); err != nil {
		return nil, err
	}

	if sourceType == "storage" {
		return c.loadMessagesFromStorage(chatID, beforeID)
	}

	if beforeID <= 0 {
		messages, err := c.loadInitialMessages(token.context(), chatID)
		if err != nil {
			return nil, err
		}

		return mappers.MapDomainMessagesToDecrypted(messages), nil
	}

	if sourceType == "server" {
		messages, err := c.loadMessagesFromServer(token.context(), chatID, beforeID)
		if err != nil {
			return nil, err
		}

		return mappers.MapDomainMessagesToDecrypted(messages), nil
	}

	messages, err := c.loadMessagesAuto(token.context(), chatID, beforeID)
	if err != nil {
		return nil, err
	}

	return mappers.MapDomainMessagesToDecrypted(messages), nil
}

func (c *BloomClient) SyncMessages(token *CancelToken, chatID, afterID int) ([]domain.DecryptedMessageWithReply, error) {
	if _, err := c.getCredentials(); err != nil {
		return nil, err
	}

	if afterID <= 0 {
		messages, err := c.loadInitialMessages(token.context(), chatID)
		if err != nil {
			return nil, err
		}

		return mappers.MapDomainMessagesToDecrypted(messages), nil
	}

	var result []domain.Message
	cursor := afterID

	for {
		page, err := c.getChatMessagesAfter(token.context(), chatID, cursor)
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			break
		}

		if err := c.database.SaveMessages(page); err != nil {
			return nil, err
		}

		result = append(result, page...)
		cursor = page[len(page)-1].ID

		if len(page) < 20 {
			break
		}
	}

	return mappers.MapDomainMessagesToDecrypted(result), nil
}

func (c *BloomClient) RegisterMessagesListener(listener MessagesListener) {
	c.listenerMu.Lock()
	c.messagesListener = listener
	c.listenerMu.Unlock()
}

func (c *BloomClient) UnregisterMessagesListener() {
	c.listenerMu.Lock()
	c.messagesListener = nil
	c.listenerMu.Unlock()
}
