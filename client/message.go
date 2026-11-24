package client

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/ephmrl-team/bloom-root/domain"
	"github.com/ephmrl-team/bloom-root/logger"
	messageManager "github.com/ephmrl-team/bloom-root/managers/message"
	"github.com/ephmrl-team/bloom-root/mappers"
)

func (c *BloomClient) loadMessagesFromStorage(chatID, beforeID int) ([]domain.DecryptedMessageWithReply, error) {
	if _, err := c.getCredentials(); err != nil {
		logger.Error("message", "messages load failed", "chat_id", chatID, "before_id", beforeID, "error", err)
		return nil, err
	}

	messagesFromStorage, err := c.database.GetMessages(chatID, beforeID, 20)
	if err != nil {
		logger.Error("message", "messages storage load failed", "chat_id", chatID, "before_id", beforeID, "error", err)
		return nil, err
	}

	logger.Success("message", "messages loaded from storage", "chat_id", chatID, "before_id", beforeID, "count", len(messagesFromStorage))

	return mappers.MapDomainMessagesToDecrypted(messagesFromStorage), nil
}

func (c *BloomClient) getChatMessages(ctx context.Context, chatID, messageID int, direction messageManager.Direction) ([]domain.Message, error) {
	creds, err := c.getCredentials()
	if err != nil {
		logger.Error("message", "messages load failed", "chat_id", chatID, "message_id", messageID, "error", err)
		return nil, err
	}

	chat, err := c.getChatContext(ctx, chatID)
	if err != nil {
		logger.Error("message", "chat load failed", "chat_id", chatID, "error", err)
		return nil, err
	}
	if chat == nil {
		logger.Warn("message", "chat not found", "chat_id", chatID)
		return nil, fmt.Errorf("%w: chat %d not found", domain.ErrInvalidChat, chatID)
	}

	recipient, err := getChatRecipient(&domain.Chat{
		RawChat: chat.RawChat,
	}, creds.UserID)
	if err != nil {
		logger.Warn("message", "chat recipient lookup failed", "chat_id", chatID, "user_id", creds.UserID, "error", err)
		return nil, err
	}

	recipientIdentity, err := mappers.ConvertUserToIdentity(recipient)
	if err != nil {
		logger.Warn("message", "recipient identity failed", "chat_id", chatID, "user_id", recipient.ID, "error", err)
		return nil, fmt.Errorf("%w: failed to build recipient identity: %w", domain.ErrInvalidUserIdentity, err)
	}

	userIdentity, err := mappers.ConvertCredentialsToUser(creds)
	if err != nil {
		logger.Error("message", "user identity build failed", "chat_id", chatID, "user_id", creds.UserID, "error", err)
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
		logger.Error("message", "messages load failed", "chat_id", chatID, "message_id", messageID, "error", err)
		return nil, err
	}

	sort.SliceStable(messages, func(i, j int) bool {
		return messages[i].ID < messages[j].ID
	})

	logger.Success("message", "messages loaded", "chat_id", chatID, "message_id", messageID, "count", len(messages))

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
		logger.Error("message", "messages load failed", "chat_id", chatID, "before_id", beforeID, "error", err)
		return nil, err
	}

	if len(messages) == 0 {
		logger.Info("message", "no messages found", "chat_id", chatID, "before_id", beforeID)
		return messages, nil
	}

	if err := c.database.SaveMessages(messages); err != nil {
		logger.Error("message", "messages save failed", "chat_id", chatID, "count", len(messages), "error", err)
		return nil, err
	}

	oldestContiguousID, err := c.database.GetChatOldestContiguousID(chatID)
	if err != nil {
		logger.Error("message", "oldest message state load failed", "chat_id", chatID, "error", err)
		return nil, err
	}
	newestContiguousID, err := c.database.GetChatNewestContiguousID(chatID)
	if err != nil {
		logger.Error("message", "newest message state load failed", "chat_id", chatID, "error", err)
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
		logger.Error("message", "oldest message state save failed", "chat_id", chatID, "message_id", oldestContiguousID, "error", err)
		return nil, err
	}
	if err := c.database.SetChatNewestContiguousID(chatID, newestContiguousID); err != nil {
		logger.Error("message", "newest message state save failed", "chat_id", chatID, "message_id", newestContiguousID, "error", err)
		return nil, err
	}

	logger.Success("message", "messages loaded from server", "chat_id", chatID, "before_id", beforeID, "count", len(messages))

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
			logger.Warn("message", "message gap fill stopped", "chat_id", chatID, "after_id", cursor, "stop_before_id", stopBeforeID, "error", err)
			return false
		}

		if err := c.database.SaveMessages(page); err != nil {
			logger.Warn("message", "message gap save failed", "chat_id", chatID, "count", len(page), "error", err)
			return false
		}

		cursor = page[len(page)-1].ID

		if err := c.database.SetChatNewestContiguousID(chatID, cursor); err != nil {
			logger.Warn("message", "message gap state save failed", "chat_id", chatID, "message_id", cursor, "error", err)
			return false
		}
	}

	logger.Success("message", "message gap filled", "chat_id", chatID, "after_id", afterID, "stop_before_id", stopBeforeID)

	return true
}

func (c *BloomClient) loadInitialMessages(ctx context.Context, chatID int) ([]domain.Message, error) {
	messages, err := c.getChatMessagesAfter(ctx, chatID, 0)
	if err != nil {
		logger.Warn("message", "initial messages load failed, using local cache", "chat_id", chatID, "error", err)
		return c.database.GetMessages(chatID, 0, 20)
	}

	if len(messages) == 0 {
		logger.Info("message", "no initial messages found", "chat_id", chatID)
		return messages, nil
	}

	if err := c.database.SaveMessages(messages); err != nil {
		logger.Error("message", "initial messages save failed", "chat_id", chatID, "count", len(messages), "error", err)
		return nil, err
	}

	oldestContiguousID, err := c.database.GetChatOldestContiguousID(chatID)
	if err != nil {
		logger.Error("message", "oldest message state load failed", "chat_id", chatID, "error", err)
		return nil, err
	}
	newestContiguousID, err := c.database.GetChatNewestContiguousID(chatID)
	if err != nil {
		logger.Error("message", "newest message state load failed", "chat_id", chatID, "error", err)
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
			logger.Error("message", "newest message state load failed", "chat_id", chatID, "error", err)
			return nil, err
		}

		if gapFilled && newestMessageID > newestContiguousID {
			newestContiguousID = newestMessageID
		}
	}

	if err := c.database.SetChatOldestContiguousID(chatID, oldestContiguousID); err != nil {
		logger.Error("message", "oldest message state save failed", "chat_id", chatID, "message_id", oldestContiguousID, "error", err)
		return nil, err
	}
	if err := c.database.SetChatNewestContiguousID(chatID, newestContiguousID); err != nil {
		logger.Error("message", "newest message state save failed", "chat_id", chatID, "message_id", newestContiguousID, "error", err)
		return nil, err
	}

	logger.Success("message", "initial messages loaded", "chat_id", chatID, "count", len(messages))

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
		logger.Info("message", "send deduplicated", "chat_id", chatID, "reply_id", replyToValue(replyToID), "content_length", len(content))

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
	logger.Info("message", "message send started", "chat_id", chatID, "reply_id", replyToValue(replyToID), "content_length", len(content))

	creds, err := c.getCredentials()
	if err != nil {
		logger.Error("message", "message send failed", "chat_id", chatID, "error", err)
		return nil, err
	}

	chat, err := c.getChatContext(ctx, chatID)
	if err != nil {
		logger.Error("message", "chat load failed", "chat_id", chatID, "error", err)
		return nil, err
	}
	if chat == nil {
		logger.Warn("message", "chat not found", "chat_id", chatID)
		return nil, fmt.Errorf("%w: chat %d not found", domain.ErrInvalidChat, chatID)
	}

	recipient, err := getChatRecipient(&domain.Chat{
		RawChat: chat.RawChat,
	}, creds.UserID)
	if err != nil {
		logger.Warn("message", "chat recipient lookup failed", "chat_id", chatID, "user_id", creds.UserID, "error", err)
		return nil, err
	}

	recipientIdentity, err := mappers.ConvertUserToIdentity(recipient)
	if err != nil {
		logger.Warn("message", "recipient identity failed", "chat_id", chatID, "user_id", recipient.ID, "error", err)
		return nil, fmt.Errorf("%w: failed to build recipient identity: %w", domain.ErrInvalidUserIdentity, err)
	}

	userIdentity, err := mappers.ConvertCredentialsToUser(creds)
	if err != nil {
		logger.Error("message", "user identity build failed", "chat_id", chatID, "user_id", creds.UserID, "error", err)
		return nil, err
	}

	clientMessageID, err := c.pendingClientMessageID(chatID, replyToID, content)
	if err != nil {
		logger.Error("message", "client message id failed", "chat_id", chatID, "error", err)
		return nil, err
	}

	message, err := c.messageManager.Send(ctx, clientMessageID, content, chatID, replyToID, chat.ChatKey, chat.SyncKey, userIdentity, recipientIdentity)
	if err != nil {
		logger.Error("message", "message send failed", "chat_id", chatID, "client_message_id", clientMessageID, "error", err)
		if rememberErr := c.rememberPendingSend(chatID, replyToID, content, clientMessageID); rememberErr != nil {
			logger.Error("message", "pending send save failed", "chat_id", chatID, "client_message_id", clientMessageID, "error", rememberErr)
			return nil, fmt.Errorf("%w: failed to remember pending send: %w", err, rememberErr)
		}
		return nil, err
	}

	if err := c.database.SaveMessage(message); err != nil {
		logger.Warn("message", "message save failed", "chat_id", chatID, "message_id", message.ID, "error", err)
	}

	if err := c.forgetPendingSend(chatID, replyToID, content); err != nil {
		logger.Error("message", "pending send delete failed", "chat_id", chatID, "message_id", message.ID, "error", err)
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

	logger.Success("message", "message sent", "chat_id", chatID, "message_id", message.ID, "author_id", userIdentity.ID, "reply_id", replyToValue(replyToID))

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
		logger.Error("message", "message send failed", "chat_id", chatID, "error", err)
		return nil, err
	}

	logger.Info("message", "sending message", "chat_id", chatID, "reply_id", replyToID, "content_length", len(content))

	var replyToPtr *int
	if replyToID >= 0 {
		replyToPtr = &replyToID
	}

	message, err := c.sendMessage(token.context(), chatID, replyToPtr, content)
	if err != nil {
		logger.Error("message", "message send failed", "chat_id", chatID, "reply_id", replyToID, "error", err)
		return nil, err
	}

	messageJSON, err := json.Marshal(message)
	if err != nil {
		logger.Error("message", "message marshal failed", "chat_id", chatID, "message_id", message.ID, "error", err)
		return nil, err
	}

	logger.Success("message", "message send completed", "chat_id", chatID, "message_id", message.ID, "reply_id", replyToID)

	return messageJSON, nil
}

func (c *BloomClient) LoadMessages(token *CancelToken, sourceType string, chatID, beforeID int) ([]domain.DecryptedMessageWithReply, error) {
	if _, err := c.getCredentials(); err != nil {
		logger.Error("message", "messages load failed", "chat_id", chatID, "source", sourceType, "before_id", beforeID, "error", err)
		return nil, err
	}

	logger.Info("message", "loading messages", "chat_id", chatID, "source", sourceType, "before_id", beforeID)

	if sourceType == "storage" {
		messages, err := c.loadMessagesFromStorage(chatID, beforeID)
		if err != nil {
			logger.Error("message", "messages load failed", "chat_id", chatID, "source", sourceType, "before_id", beforeID, "error", err)
			return nil, err
		}
		logger.Success("message", "messages loaded", "chat_id", chatID, "source", sourceType, "before_id", beforeID, "count", len(messages))
		return messages, nil
	}

	if beforeID <= 0 {
		messages, err := c.loadInitialMessages(token.context(), chatID)
		if err != nil {
			logger.Error("message", "messages load failed", "chat_id", chatID, "source", sourceType, "before_id", beforeID, "error", err)
			return nil, err
		}

		decrypted := mappers.MapDomainMessagesToDecrypted(messages)
		logger.Success("message", "messages loaded", "chat_id", chatID, "source", sourceType, "before_id", beforeID, "count", len(decrypted))
		return decrypted, nil
	}

	if sourceType == "server" {
		messages, err := c.loadMessagesFromServer(token.context(), chatID, beforeID)
		if err != nil {
			logger.Error("message", "messages load failed", "chat_id", chatID, "source", sourceType, "before_id", beforeID, "error", err)
			return nil, err
		}

		decrypted := mappers.MapDomainMessagesToDecrypted(messages)
		logger.Success("message", "messages loaded", "chat_id", chatID, "source", sourceType, "before_id", beforeID, "count", len(decrypted))
		return decrypted, nil
	}

	messages, err := c.loadMessagesAuto(token.context(), chatID, beforeID)
	if err != nil {
		logger.Error("message", "messages load failed", "chat_id", chatID, "source", sourceType, "before_id", beforeID, "error", err)
		return nil, err
	}

	decrypted := mappers.MapDomainMessagesToDecrypted(messages)
	logger.Success("message", "messages loaded", "chat_id", chatID, "source", sourceType, "before_id", beforeID, "count", len(decrypted))

	return decrypted, nil
}

func (c *BloomClient) SyncMessages(token *CancelToken, chatID, afterID int) ([]domain.DecryptedMessageWithReply, error) {
	if _, err := c.getCredentials(); err != nil {
		logger.Error("message", "messages sync failed", "chat_id", chatID, "after_id", afterID, "error", err)
		return nil, err
	}

	logger.Info("message", "syncing messages", "chat_id", chatID, "after_id", afterID)

	if afterID <= 0 {
		messages, err := c.loadInitialMessages(token.context(), chatID)
		if err != nil {
			logger.Error("message", "messages sync failed", "chat_id", chatID, "after_id", afterID, "error", err)
			return nil, err
		}

		decrypted := mappers.MapDomainMessagesToDecrypted(messages)
		logger.Success("message", "messages synced", "chat_id", chatID, "after_id", afterID, "count", len(decrypted))
		return decrypted, nil
	}

	var result []domain.Message
	cursor := afterID

	for {
		page, err := c.getChatMessagesAfter(token.context(), chatID, cursor)
		if err != nil {
			logger.Error("message", "messages sync page failed", "chat_id", chatID, "after_id", cursor, "error", err)
			return nil, err
		}
		if len(page) == 0 {
			break
		}

		if err := c.database.SaveMessages(page); err != nil {
			logger.Error("message", "messages sync save failed", "chat_id", chatID, "count", len(page), "error", err)
			return nil, err
		}

		result = append(result, page...)
		cursor = page[len(page)-1].ID

		if len(page) < 20 {
			break
		}
	}

	decrypted := mappers.MapDomainMessagesToDecrypted(result)
	logger.Success("message", "messages synced", "chat_id", chatID, "after_id", afterID, "count", len(decrypted))

	return decrypted, nil
}

func (c *BloomClient) RegisterMessagesListener(listener MessagesListener) {
	c.listenerMu.Lock()
	c.messagesListener = listener
	c.listenerMu.Unlock()

	logger.Info("message", "messages listener registered")
}

func (c *BloomClient) UnregisterMessagesListener() {
	c.listenerMu.Lock()
	c.messagesListener = nil
	c.listenerMu.Unlock()

	logger.Info("message", "messages listener unregistered")
}
