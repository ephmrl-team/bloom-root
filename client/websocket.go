package client

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ephmrl-team/bloom-root/domain"
	"github.com/ephmrl-team/bloom-root/logger"
	"github.com/ephmrl-team/bloom-root/mappers"
	"github.com/gorilla/websocket"
	"github.com/slipe-fun/skid-v4/pkg/identity"
)

const (
	wsDialTimeout = 10 * time.Second
	wsWriteWait   = 10 * time.Second
	wsPongWait    = 45 * time.Second
	wsPingPeriod  = 20 * time.Second
)

type WsEvent struct {
	Type string `json:"type"`
	Data []byte `json:"data"`
}

type ChatNewEvent struct {
	domain.Chat
	Type              string                    `json:"type"`
	ChatType          string                    `json:"chat_type"`
	UserID            string                    `json:"user_id"`
	Role              *string                   `json:"role,omitempty"`
	InvitedBy         *domain.User              `json:"invited_by,omitempty"`
	Handshake         *domain.Handshake         `json:"handshake,omitempty"`
	EncryptedGroupKey *domain.EncryptedGroupKey `json:"encrypted_group_key,omitempty"`
}

type MessageNewEvent struct {
	Type    string             `json:"type"`
	ID      int                `json:"id"`
	UserID  string             `json:"user_id"`
	ReplyTo *domain.RawMessage `json:"reply_to,omitempty"`
	*domain.RawMessageWithReply
}

type MessageSeenEvent struct {
	Type     string    `json:"type"`
	UserID   string    `json:"user_id"`
	ChatID   int       `json:"chat_id"`
	SeenAt   time.Time `json:"seen_at"`
	Messages []int     `json:"messages"`
}

func (m *MessageNewEvent) UnmarshalJSON(data []byte) error {
	var helper struct {
		Type    string             `json:"type"`
		ID      int                `json:"id"`
		UserID  string             `json:"user_id"`
		ReplyTo *domain.RawMessage `json:"reply_to,omitempty"`
	}

	if err := json.Unmarshal(data, &helper); err != nil {
		return err
	}

	var raw domain.RawMessageWithReply
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	m.Type = helper.Type
	m.ID = helper.ID
	m.UserID = helper.UserID
	m.ReplyTo = helper.ReplyTo
	m.RawMessageWithReply = &raw

	return nil
}

func (c *BloomClient) startWebSocket(ctx context.Context, wsURL string) {
	creds, err := c.getCredentials()
	if err != nil || creds.Token == "" {
		logger.Warn("websocket", "websocket not started", "reason", "no token")
		return
	}

	logger.Info("websocket", "websocket starting", "endpoint", strings.Split(wsURL, "?")[0])

	wsCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	c.wsCancelMu.Lock()
	if c.wsCancel != nil {
		c.wsCancel()
	}
	c.wsCancel = cancel
	c.wsDone = done
	c.wsCancelMu.Unlock()

	c.wsMu.Lock()
	c.wsConnected = false
	c.wsHasConnected = false
	c.wsMu.Unlock()

	go func() {
		defer close(done)
		defer c.recoverBackground("websocket")
		defer cancel()

		backoff := time.Second

		for {
			select {
			case <-wsCtx.Done():
				return
			case <-time.After(backoff):
			}

			creds, err := c.getCredentials()
			if err != nil || creds.Token == "" {
				logger.Warn("websocket", "websocket stopped", "reason", "no token")
				return
			}

			startedAt := time.Now()

			logger.Info("websocket", "websocket connecting", "backoff", backoff)

			err = c.connectAndListen(wsCtx, fmt.Sprintf("%s?token=%s", wsURL, creds.Token))
			if err != nil && time.Since(startedAt) < 5*time.Second {
				logger.Warn("websocket", "websocket connection failed", "error", err, "backoff", backoff)
				if backoff < 30*time.Second {
					backoff *= 2
				}
				continue
			}

			backoff = time.Second
		}
	}()
}

func (c *BloomClient) stopWebSocket() {
	logger.Info("websocket", "websocket stopping")

	c.wsCancelMu.Lock()
	cancel := c.wsCancel
	done := c.wsDone
	c.wsCancel = nil
	c.wsDone = nil
	if cancel != nil {
		cancel()
	}
	c.wsCancelMu.Unlock()

	if done != nil {
		<-done
	}

	c.wsMu.Lock()
	c.wsConnected = false
	c.wsHasConnected = false
	c.wsMu.Unlock()

	logger.Info("websocket", "websocket stopped")
}

func (c *BloomClient) websocketDialer() *websocket.Dialer {
	dialer := *websocket.DefaultDialer
	dialer.NetDialContext = c.dialContext
	return &dialer
}

func (c *BloomClient) connectAndListen(ctx context.Context, url string) error {
	endpoint := strings.Split(url, "?")[0]

	dialCtx, dialCancel := context.WithTimeout(ctx, wsDialTimeout)
	conn, _, err := c.websocketDialer().DialContext(dialCtx, url, nil)
	dialCancel()
	if err != nil {
		logger.Warn("websocket", "websocket dial failed", "endpoint", endpoint, "error", err)
		return fmt.Errorf("websocket dial error: %w", err)
	}
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(wsPongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(wsPongWait))
	})

	defaultPingHandler := conn.PingHandler()
	conn.SetPingHandler(func(appData string) error {
		_ = conn.SetReadDeadline(time.Now().Add(wsPongWait))
		return defaultPingHandler(appData)
	})

	done := make(chan struct{})
	defer close(done)

	go func() {
		defer c.recoverBackground("websocket-close-watcher")

		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()

	go func() {
		defer c.recoverBackground("websocket-ping")

		ticker := time.NewTicker(wsPingPeriod)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(wsWriteWait)); err != nil {
					_ = conn.Close()
					return
				}
			}
		}
	}()

	reconnected := c.markConnected()
	if reconnected {
		logger.Info("websocket", "websocket reconnected", "endpoint", endpoint)
	} else {
		logger.Success("websocket", "websocket connected", "endpoint", endpoint)
	}

	c.notifyConnectionState(true)
	if reconnected {
		c.notifyReconnect()
	}
	defer func() {
		logger.Info("websocket", "websocket disconnected", "endpoint", endpoint)
		c.markDisconnected()
		c.notifyConnectionState(false)
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			_, message, err := conn.ReadMessage()
			if err != nil {
				logger.Warn("websocket", "websocket read failed", "endpoint", endpoint, "error", err)
				return fmt.Errorf("websocket read error: %w", err)
			}

			_ = conn.SetReadDeadline(time.Now().Add(wsPongWait))

			var event WsEvent
			if err := json.Unmarshal(message, &event); err != nil {
				logger.Warn("websocket", "event unmarshal failed", "endpoint", endpoint, "error", err)
				continue
			}

			switch event.Type {
			case "chat.new":
				logger.Info("websocket", "chat.new received", "endpoint", endpoint)

				var event ChatNewEvent
				if err := json.Unmarshal(message, &event); err != nil {
					logger.Warn("websocket", "chat.new unmarshal failed", "endpoint", endpoint, "error", err)
					continue
				}

				c.handleNewChatEvent(&event)
			case "message.new":
				logger.Info("websocket", "message.new received", "endpoint", endpoint)

				var event MessageNewEvent
				if err := json.Unmarshal(message, &event); err != nil {
					logger.Warn("websocket", "message.new unmarshal failed", "endpoint", endpoint, "error", err)
					continue
				}

				c.handleNewMessageEvent(&event)
			case "message.seen":
				logger.Info("websocket", "message.seen received", "endpoint", endpoint)

				var event MessageSeenEvent
				if err := json.Unmarshal(message, &event); err != nil {
					logger.Warn("websocket", "message.seen unmarshal failed", "endpoint", endpoint, "error", err)
					continue
				}

				c.handleMessageSeenEvent(&event)
			}
		}
	}
}

func (c *BloomClient) markConnected() bool {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()

	reconnected := c.wsHasConnected
	c.wsConnected = true
	c.wsHasConnected = true
	return reconnected
}

func (c *BloomClient) markDisconnected() {
	c.wsMu.Lock()
	c.wsConnected = false
	c.wsMu.Unlock()
}

func (c *BloomClient) IsWebSocketConnected() bool {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()

	return c.wsConnected
}

func (c *BloomClient) notifyConnectionState(connected bool) {
	c.listenerMu.RLock()
	connectionListener := c.connectionListener
	c.listenerMu.RUnlock()

	if connectionListener == nil {
		return
	}

	if connected {
		connectionListener.OnConnected()
		return
	}

	connectionListener.OnDisconnected()
}

func (c *BloomClient) RegisterConnectionListener(listener ConnectionListener) {
	c.listenerMu.Lock()
	c.connectionListener = listener
	c.listenerMu.Unlock()

	logger.Info("websocket", "connection listener registered")
}

func (c *BloomClient) UnregisterConnectionListener() {
	c.listenerMu.Lock()
	c.connectionListener = nil
	c.listenerMu.Unlock()

	logger.Info("websocket", "connection listener unregistered")
}

func (c *BloomClient) notifyReconnect() {
	c.listenerMu.RLock()
	reconnectListener := c.reconnectListener
	c.listenerMu.RUnlock()

	if reconnectListener != nil {
		reconnectListener.OnReconnect()
	}
}

func (c *BloomClient) RegisterReconnectListener(listener ReconnectListener) {
	c.listenerMu.Lock()
	c.reconnectListener = listener
	c.listenerMu.Unlock()

	logger.Info("websocket", "reconnect listener registered")
}

func (c *BloomClient) UnregisterReconnectListener() {
	c.listenerMu.Lock()
	c.reconnectListener = nil
	c.listenerMu.Unlock()

	logger.Info("websocket", "reconnect listener unregistered")
}

func (c *BloomClient) handleNewChatEvent(chatEvent *ChatNewEvent) {
	if chatEvent == nil {
		logger.Warn("websocket", "chat.new ignored", "reason", "nil event")
		return
	}

	chat := chatEvent.Chat

	logger.Info("websocket", "chat.new handling", "chat_id", chat.ID, "chat_type", chatEvent.ChatType)

	if chat.Handshake == nil && chatEvent.Handshake != nil {
		chat.Handshake = chatEvent.Handshake
	}

	creds, err := c.loadCredentials()
	if err != nil {
		logger.Error("websocket", "chat.new credentials load failed", "chat_id", chat.ID, "error", err)
		return
	}

	userIdentity, err := mappers.ConvertCredentialsToUser(creds)
	if err != nil {
		logger.Error("websocket", "chat.new identity conversion failed", "chat_id", chat.ID, "error", err)
		return
	}

	if existing, err := c.database.GetChat(chat.ID); err == nil && existing != nil && len(existing.ChatKey) > 0 {
		if recipient := getChatOtherMember(&chat, creds.UserID); recipient != nil {
			if err := c.database.SaveUser(recipient); err != nil {
				logger.Error("websocket", "chat.new recipient save failed", "chat_id", chat.ID, "user_id", recipient.ID, "error", err)
			}
		}
		logger.Info("websocket", "chat.new already exists", "chat_id", chat.ID)
		c.notifyChatsUpdated()
		return
	}

	chatType := chatEvent.ChatType

	switch chatType {
	case "private":
		recipient, err := getChatRecipient(&chat, creds.UserID)
		if err != nil {
			logger.Error("websocket", "chat.new recipient lookup failed", "chat_id", chat.ID, "error", err)
			return
		}

		recipientIdentity, err := mappers.ConvertUserToIdentity(recipient)
		if err != nil {
			logger.Error("websocket", "chat.new recipient identity conversion failed", "chat_id", chat.ID, "user_id", recipient.ID, "error", err)
			return
		}

		handshakePayload, err := mappers.DecodeHandshake(chat.Handshake)
		if err != nil {
			logger.Error("websocket", "chat.new handshake decode failed", "chat_id", chat.ID, "error", err)
			return
		}

		secretKeys, err := mappers.UnmapSecretKeys(creds.SecretKeys)
		if err != nil {
			logger.Error("websocket", "chat.new secret keys unmap failed", "chat_id", chat.ID, "error", err)
			return
		}
		defer secretKeys.Wipe()

		chatKey, syncKey, err := identity.FinalizeKeyExchange(handshakePayload, userIdentity, secretKeys, recipientIdentity, nil, true)
		if err != nil {
			chatKey, syncKey, err = identity.FinalizeKeyExchange(handshakePayload, recipientIdentity, nil, userIdentity, secretKeys, false)
			if err != nil {
				logger.Error("websocket", "chat.new key exchange failed", "chat_id", chat.ID, "error", err)
				return
			}
		}

		err = c.database.SaveChat(chat, chatKey, syncKey)
		if err != nil {
			logger.Error("websocket", "chat.new save failed", "chat_id", chat.ID, "error", err)
			return
		}

		if err := c.database.SaveUser(recipient); err != nil {
			logger.Error("websocket", "chat.new recipient save failed", "chat_id", chat.ID, "user_id", recipient.ID, "error", err)
		}
	case "group":
		if chat.Members != nil && len(*chat.Members) > 0 {
			if err := c.database.SaveUsers(*chat.Members); err != nil {
				logger.Error("websocket", "chat.new group members save failed", "chat_id", chat.ID, "count", len(*chat.Members), "error", err)
			}
		}
		logger.Info("websocket", "group chat ignored", "chat_id", chat.ID)
		c.notifyChatsUpdated()
		return
	default:
		logger.Warn("websocket", "chat.new unsupported type", "chat_id", chat.ID, "chat_type", chatType)
		return
	}

	logger.Success("websocket", "chat.new applied", "chat_id", chat.ID, "chat_type", chatType)
	c.notifyChatsUpdated()
}

func (c *BloomClient) handleNewMessageEvent(messageEvent *MessageNewEvent) {
	if messageEvent == nil || messageEvent.RawMessageWithReply == nil {
		logger.Warn("websocket", "message.new ignored", "reason", "nil event")
		return
	}

	if existing, err := c.database.GetMessage(messageEvent.ID); err != nil {
		logger.Error("websocket", "message.new lookup failed", "message_id", messageEvent.ID, "error", err)
		return
	} else if existing != nil {
		logger.Info("websocket", "message.new already exists", "message_id", messageEvent.ID)
		return
	}

	creds, err := c.getCredentials()
	if err != nil {
		logger.Error("websocket", "message.new credentials load failed", "message_id", messageEvent.ID, "error", err)
		return
	}

	chat, err := c.getChatContext(context.Background(), messageEvent.ChatID)
	if err != nil {
		logger.Error("websocket", "message.new chat load failed", "message_id", messageEvent.ID, "chat_id", messageEvent.ChatID, "error", err)
		return
	}
	if chat == nil {
		logger.Warn("websocket", "message.new ignored", "reason", "chat not found", "message_id", messageEvent.ID, "chat_id", messageEvent.ChatID)
		return
	}

	recipient, err := getChatRecipient(&domain.Chat{
		RawChat: chat.RawChat,
	}, creds.UserID)
	if err != nil {
		logger.Error("websocket", "message.new recipient lookup failed", "message_id", messageEvent.ID, "chat_id", messageEvent.ChatID, "error", err)
		return
	}

	recipientIdentity, err := mappers.ConvertUserToIdentity(recipient)
	if err != nil {
		logger.Error("websocket", "message.new recipient identity conversion failed", "message_id", messageEvent.ID, "user_id", recipient.ID, "error", err)
		return
	}

	userIdentity, err := mappers.ConvertCredentialsToUser(creds)
	if err != nil {
		logger.Error("websocket", "message.new identity conversion failed", "message_id", messageEvent.ID, "error", err)
		return
	}

	decryptedMessage, err := c.messageManager.DecryptMessage(messageEvent.RawMessageWithReply, chat.ChatKey, chat.SyncKey, userIdentity, recipientIdentity)
	if err != nil {
		logger.Error("websocket", "message.new decryption failed", "message_id", messageEvent.ID, "chat_id", messageEvent.ChatID, "error", err)
		return
	}
	if decryptedMessage == nil {
		logger.Warn("websocket", "message.new ignored", "reason", "decrypted message is nil", "message_id", messageEvent.ID, "chat_id", messageEvent.ChatID)
		return
	}

	var replyToMessage *domain.MessageWithDecryptedData
	if messageEvent.ReplyTo != nil {
		decryptedReply, err := c.messageManager.DecryptMessage(&domain.RawMessageWithReply{
			RawMessage: *messageEvent.ReplyTo,
		}, chat.ChatKey, chat.SyncKey, userIdentity, recipientIdentity)
		if err != nil || decryptedReply == nil {
			logger.Warn("websocket", "message.new reply decryption failed", "message_id", messageEvent.ID, "reply_id", messageEvent.ReplyTo.ID, "error", err)
			messageEvent.RawMessageWithReply.ReplyTo = nil
			messageEvent.RawMessageWithReply.ReplyToMessage = nil
		} else {
			replyToMessage = &domain.MessageWithDecryptedData{
				RawMessageWithReply: domain.RawMessageWithReply{
					RawMessage: *messageEvent.ReplyTo,
				},
				Message: *decryptedReply,
			}
		}
	}

	msg := domain.Message{
		MessageWithDecryptedData: domain.MessageWithDecryptedData{
			RawMessageWithReply: *messageEvent.RawMessageWithReply,
			Message:             *decryptedMessage,
		},
		ReplyToMessage: replyToMessage,
	}

	err = c.database.SaveMessage(&msg)
	if err != nil {
		logger.Error("websocket", "message.new save failed", "message_id", msg.ID, "chat_id", msg.ChatID, "error", err)
		return
	}

	decryptedWithReply := mappers.MapDomainMessageToDecrypted(&msg)
	messageJSON, err := json.Marshal(decryptedWithReply)
	if err != nil {
		logger.Error("websocket", "message.new marshal failed", "message_id", msg.ID, "chat_id", msg.ChatID, "error", err)
		return
	}

	c.listenerMu.RLock()
	messagesListener := c.messagesListener
	c.listenerMu.RUnlock()

	if messagesListener != nil {
		messagesListener.OnNewMessage(messageJSON)
	}

	logger.Success("websocket", "message.new applied", "message_id", msg.ID, "chat_id", msg.ChatID, "author_id", msg.AuthorID)

	c.notifyChatsUpdated()
}

func (c *BloomClient) handleMessageSeenEvent(messageSeenEvent *MessageSeenEvent) {
	if messageSeenEvent == nil || messageSeenEvent.ChatID == 0 || len(messageSeenEvent.Messages) == 0 {
		logger.Warn("websocket", "message.seen ignored", "reason", "invalid event")
		return
	}

	if err := c.database.UpdateMessagesSeen(messageSeenEvent.ChatID, messageSeenEvent.Messages, messageSeenEvent.SeenAt); err != nil {
		logger.Error("websocket", "message.seen update failed", "chat_id", messageSeenEvent.ChatID, "count", len(messageSeenEvent.Messages), "error", err)
		return
	}

	messagesSeenJSON, err := json.Marshal(messageSeenEvent)
	if err != nil {
		logger.Error("websocket", "message.seen marshal failed", "chat_id", messageSeenEvent.ChatID, "error", err)
		return
	}

	c.listenerMu.RLock()
	messagesSeenListener := c.messagesSeenListener
	c.listenerMu.RUnlock()

	if messagesSeenListener != nil {
		messagesSeenListener.OnMessagesSeen(messagesSeenJSON)
	}

	logger.Success("websocket", "message.seen applied", "chat_id", messageSeenEvent.ChatID, "user_id", messageSeenEvent.UserID, "count", len(messageSeenEvent.Messages))

	c.notifyChatsUpdated()
}
