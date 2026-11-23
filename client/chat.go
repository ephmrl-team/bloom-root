package client

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/ephmrl-team/bloom-root/domain"
	"github.com/ephmrl-team/bloom-root/mappers"
)

type CreateChatRequest struct {
	UserID string
}

type ChatResponse struct {
	domain.RawChat
	Me              []byte                            `json:"me"`
	Recipient       []byte                            `json:"recipient"`
	LastMessage     *domain.DecryptedMessageWithReply `json:"last_message,omitempty"`
	LastReadMessage *domain.DecryptedMessageWithReply `json:"last_read_message,omitempty"`
}

func (c *BloomClient) CreateChat(token *CancelToken, receiverUser *CreateChatRequest) ([]byte, error) {
	if _, err := c.getCredentials(); err != nil {
		return nil, err
	}

	createdChat, err := c.createChat(token.context(), receiverUser)
	if err != nil {
		return nil, err
	}

	return json.Marshal(createdChat)
}

func (c *BloomClient) GetChats(token *CancelToken) ([]byte, error) {
	if _, err := c.getCredentials(); err != nil {
		return nil, err
	}

	chats, err := c.getChatsContext(token.context())
	if err != nil {
		return nil, err
	}

	return json.Marshal(chats)
}

func (c *BloomClient) GetLocalChats() ([]byte, error) {
	if _, err := c.getCredentials(); err != nil {
		return nil, err
	}

	chats, err := c.getLocalChats()
	if err != nil {
		return nil, err
	}

	return json.Marshal(chats)
}

func (c *BloomClient) RegisterChatsListener(listener ChatsListener) {
	c.listenerMu.Lock()
	c.chatsListener = listener
	c.listenerMu.Unlock()

	c.notifyChatsUpdated()
}

func (c *BloomClient) UnregisterChatsListener() {
	c.listenerMu.Lock()
	c.chatsListener = nil
	c.listenerMu.Unlock()
}

func (c *BloomClient) StartChatsSync() {
	c.syncMu.Lock()
	if c.syncCancel != nil {
		c.syncMu.Unlock()
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	c.syncCancel = cancel
	c.syncDone = done
	c.syncMu.Unlock()

	go func() {
		defer close(done)
		defer c.recoverBackground("chats-sync")

		c.syncRemoteChats(ctx)

		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.syncRemoteChats(ctx)
			}
		}
	}()
}

func (c *BloomClient) StopChatsSync() {
	c.syncMu.Lock()
	cancel := c.syncCancel
	done := c.syncDone
	c.syncCancel = nil
	c.syncDone = nil
	if cancel != nil {
		cancel()
	}
	c.syncMu.Unlock()

	if done != nil {
		<-done
	}
}

func getChatOtherMember(chat *domain.Chat, memberID string) *domain.User {
	if chat == nil || chat.Members == nil {
		return nil
	}

	for i := range *chat.Members {
		member := &(*chat.Members)[i]
		if member.ID == "" {
			continue
		}
		if member.ID != memberID {
			return member
		}
	}

	return nil
}

func getChatRecipient(chat *domain.Chat, memberID string) (*domain.User, error) {
	if chat == nil {
		return nil, domain.ErrInvalidChat
	}

	if memberID == "" {
		return nil, domain.ErrNotAuthenticated
	}

	if chat.Type != "" && chat.Type != "private" {
		return nil, fmt.Errorf("%w: %s", domain.ErrUnsupportedChatType, chat.Type)
	}

	if chat.Members != nil && len(*chat.Members) > 2 {
		return nil, fmt.Errorf("%w: chat has %d members", domain.ErrUnsupportedChatType, len(*chat.Members))
	}

	recipient := getChatOtherMember(chat, memberID)
	if recipient == nil {
		return nil, domain.ErrChatRecipientNotFound
	}

	return recipient, nil
}

func (c *BloomClient) createChat(ctx context.Context, receiverUser *CreateChatRequest) (*ChatResponse, error) {
	if receiverUser == nil {
		return nil, fmt.Errorf("%w: receiver request is nil", domain.ErrInvalidUserIdentity)
	}
	if receiverUser.UserID == "" {
		return nil, fmt.Errorf("%w: receiver user ID is empty", domain.ErrInvalidUserIdentity)
	}

	if _, err := c.getCredentials(); err != nil {
		return nil, err
	}

	creds, err := c.loadCredentials()
	if err != nil {
		return nil, err
	}

	sender, err := mappers.ConvertCredentialsToUser(creds)
	if err != nil {
		return nil, err
	}

	secretKeys, err := mappers.UnmapSecretKeys(creds.SecretKeys)
	if err != nil {
		return nil, err
	}
	defer secretKeys.Wipe()

	receiver, err := c.getOrFetchUser(ctx, receiverUser.UserID)
	if err != nil {
		return nil, err
	}
	if receiver == nil {
		return nil, fmt.Errorf("%w: receiver not found", domain.ErrInvalidUserIdentity)
	}
	if receiver.ID != receiverUser.UserID {
		return nil, fmt.Errorf("%w: receiver ID mismatch", domain.ErrIdentityMismatch)
	}

	receiverIdentity, err := mappers.ConvertUserToIdentity(receiver)
	if err != nil {
		return nil, err
	}

	createdChat, chatKey, syncKey, err := c.chatManager.Create(ctx, sender, receiverIdentity, secretKeys)
	if err != nil {
		return nil, err
	}
	if createdChat == nil {
		return nil, domain.ErrInvalidChat
	}

	err = c.database.SaveChat(*createdChat, chatKey, syncKey)
	if err != nil {
		return nil, err
	}

	recipient := receiver
	if createdChat.Members != nil {
		for i := range *createdChat.Members {
			if _, err := mappers.ConvertUserToIdentity(&(*createdChat.Members)[i]); err != nil {
				return nil, fmt.Errorf("failed to verify chat member %s: %w", (*createdChat.Members)[i].ID, err)
			}
		}
	}

	recipientJSON, err := json.Marshal(recipient)
	if err != nil {
		return nil, err
	}

	response := ChatResponse{
		RawChat:   createdChat.RawChat,
		Me:        creds.UserJSON,
		Recipient: recipientJSON,
	}

	if createdChat.Members != nil && len(*createdChat.Members) > 0 {
		err = c.database.SaveUsers(*createdChat.Members)
	} else {
		err = c.database.SaveUser(recipient)
	}
	if err != nil {
		return nil, err
	}

	return &response, nil
}

func (c *BloomClient) getChatsContext(ctx context.Context) (*[]ChatResponse, error) {
	if _, err := c.getCredentials(); err != nil {
		return nil, err
	}

	creds, err := c.loadCredentials()
	if err != nil {
		return nil, err
	}

	userIdentity, err := mappers.ConvertCredentialsToUser(creds)
	if err != nil {
		return nil, err
	}

	chats, err := c.chatManager.GetChats(ctx)
	if err != nil {
		return nil, err
	}
	if chats == nil {
		return &[]ChatResponse{}, nil
	}

	localChats, err := c.database.GetChats()
	if err != nil {
		return nil, err
	}
	localChatsMap := make(map[int]domain.ChatWithKeys, len(localChats))
	for _, lc := range localChats {
		localChatsMap[lc.ID] = lc
	}

	chatsSlice := *chats
	var result []ChatResponse
	var ids []int
	var members []domain.User
	var messages []domain.Message

	chatsMap := make(map[int]*domain.Chat, len(chatsSlice))
	resultMap := make(map[int]int)

	for i := range chatsSlice {
		chat := &chatsSlice[i]
		chatsMap[chat.ID] = chat

		recipient, err := getChatRecipient(chat, creds.UserID)
		if err != nil {
			continue
		}

		recipientIdentity, err := mappers.ConvertUserToIdentity(recipient)
		if err != nil {
			continue
		}

		members = append(members, *recipient)
		ids = append(ids, chat.ID)

		recipientJSON, err := json.Marshal(recipient)
		if err != nil {
			continue
		}

		newChatObject := ChatResponse{
			RawChat:   chat.RawChat,
			Me:        creds.UserJSON,
			Recipient: recipientJSON,
		}

		if lc, exists := localChatsMap[chat.ID]; exists {
			if chat.EncryptedLastMessage != nil {
				if lc.LastMessage != nil && lc.LastMessage.ID == chat.EncryptedLastMessage.ID {
					mapped := mappers.MapDomainMessageToDecrypted(lc.LastMessage)
					newChatObject.LastMessage = &mapped
				} else {
					decryptedLastMessage, err := c.messageManager.DecryptMessage(chat.EncryptedLastMessage, lc.ChatKey, lc.SyncKey, userIdentity, recipientIdentity)
					if err == nil {
						msg := domain.Message{
							MessageWithDecryptedData: domain.MessageWithDecryptedData{
								RawMessageWithReply: *chat.EncryptedLastMessage,
								Message:             *decryptedLastMessage,
							},
						}
						messages = append(messages, msg)
						mapped := mappers.MapDomainMessageToDecrypted(&msg)
						newChatObject.LastMessage = &mapped
					} else if lc.LastMessage != nil {
						mapped := mappers.MapDomainMessageToDecrypted(lc.LastMessage)
						newChatObject.LastMessage = &mapped
					}
				}
			} else if lc.LastMessage != nil {
				mapped := mappers.MapDomainMessageToDecrypted(lc.LastMessage)
				newChatObject.LastMessage = &mapped
			}

			if chat.EncryptedLastReadMessage != nil {
				if lc.LastReadMessage != nil && lc.LastReadMessage.ID == chat.EncryptedLastReadMessage.ID {
					mapped := mappers.MapDomainMessageToDecrypted(lc.LastReadMessage)
					newChatObject.LastReadMessage = &mapped
				} else {
					decryptedLastReadMessage, err := c.messageManager.DecryptMessage(chat.EncryptedLastReadMessage, lc.ChatKey, lc.SyncKey, userIdentity, recipientIdentity)
					if err == nil {
						msg := domain.Message{
							MessageWithDecryptedData: domain.MessageWithDecryptedData{
								RawMessageWithReply: *chat.EncryptedLastReadMessage,
								Message:             *decryptedLastReadMessage,
							},
						}
						messages = append(messages, msg)
						mapped := mappers.MapDomainMessageToDecrypted(&msg)
						newChatObject.LastReadMessage = &mapped
					} else if lc.LastReadMessage != nil {
						mapped := mappers.MapDomainMessageToDecrypted(lc.LastReadMessage)
						newChatObject.LastReadMessage = &mapped
					}
				}
			} else if lc.LastReadMessage != nil {
				mapped := mappers.MapDomainMessageToDecrypted(lc.LastReadMessage)
				newChatObject.LastReadMessage = &mapped
			}
		}

		result = append(result, newChatObject)
		resultMap[chat.ID] = len(result) - 1
	}

	missing, err := c.database.GetMissingChatIDs(ids)
	if err != nil {
		return nil, err
	}

	secretKeys, err := mappers.UnmapSecretKeys(creds.SecretKeys)
	if err != nil {
		return nil, err
	}
	defer secretKeys.Wipe()

	var missingChats []domain.ChatWithKeys
	brokenChats := make(map[int]struct{})
	for _, id := range missing {
		chat, exists := chatsMap[id]
		if !exists {
			continue
		}

		recipient, err := getChatRecipient(chat, creds.UserID)
		if err != nil {
			continue
		}

		recipientIdentity, err := mappers.ConvertUserToIdentity(recipient)
		if err != nil {
			continue
		}

		handshakePayload, err := mappers.DecodeHandshake(chat.Handshake)
		if err != nil {
			brokenChats[id] = struct{}{}
			continue
		}

		chatKey, syncKey, err := c.chatManager.FinalizeHandshake(handshakePayload, userIdentity, secretKeys, recipientIdentity)
		if err != nil {
			brokenChats[id] = struct{}{}
			continue
		}

		if chat.EncryptedLastMessage != nil {
			decLastMessage, err := c.messageManager.DecryptMessage(chat.EncryptedLastMessage, chatKey, syncKey, userIdentity, recipientIdentity)
			if err == nil {
				msg := domain.Message{
					MessageWithDecryptedData: domain.MessageWithDecryptedData{
						RawMessageWithReply: *chat.EncryptedLastMessage,
						Message:             *decLastMessage,
					},
				}
				messages = append(messages, msg)

				if idx, found := resultMap[id]; found {
					mapped := mappers.MapDomainMessageToDecrypted(&msg)
					result[idx].LastMessage = &mapped
				}
			}
		}

		if chat.EncryptedLastReadMessage != nil {
			decLastReadMessage, err := c.messageManager.DecryptMessage(chat.EncryptedLastReadMessage, chatKey, syncKey, userIdentity, recipientIdentity)
			if err == nil {
				msg := domain.Message{
					MessageWithDecryptedData: domain.MessageWithDecryptedData{
						RawMessageWithReply: *chat.EncryptedLastReadMessage,
						Message:             *decLastReadMessage,
					},
				}
				messages = append(messages, msg)

				if idx, found := resultMap[id]; found {
					mapped := mappers.MapDomainMessageToDecrypted(&msg)
					result[idx].LastReadMessage = &mapped
				}
			}
		}

		newChat := domain.ChatWithKeys{
			RawChat: chat.RawChat,
			ChatKey: chatKey,
			SyncKey: syncKey,
		}

		missingChats = append(missingChats, newChat)
	}

	if len(missingChats) > 0 {
		err = c.database.SaveChats(missingChats)
		if err != nil {
			return nil, err
		}
	}

	if len(brokenChats) > 0 {
		filtered := result[:0]
		for _, chat := range result {
			if _, broken := brokenChats[chat.ID]; broken {
				continue
			}
			filtered = append(filtered, chat)
		}
		result = filtered
	}

	if len(members) > 0 {
		if err := c.database.SaveUsers(members); err != nil {
			fmt.Println("failed to save users:", err)
		}
	}

	if len(messages) > 0 {
		if err := c.database.SaveMessages(messages); err != nil {
			fmt.Println("failed to save messages:", err)
		}
	}

	return &result, nil
}

func (c *BloomClient) getChatContext(ctx context.Context, chatID int) (*domain.ChatWithKeys, error) {
	chat, err := c.database.GetChat(chatID)
	if err != nil {
		return nil, err
	}
	if chat != nil {
		return chat, nil
	}

	creds, err := c.loadCredentials()
	if err != nil {
		return nil, err
	}

	userIdentity, err := mappers.ConvertCredentialsToUser(creds)
	if err != nil {
		return nil, err
	}

	remoteChat, err := c.chatManager.GetChat(ctx, chatID)
	if err != nil {
		return nil, err
	}
	if remoteChat == nil {
		return nil, fmt.Errorf("%w: chat %d not found", domain.ErrInvalidChat, chatID)
	}

	recipient, err := getChatRecipient(remoteChat, creds.UserID)
	if err != nil {
		return nil, err
	}

	recipientIdentity, err := mappers.ConvertUserToIdentity(recipient)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to build recipient identity: %w", domain.ErrInvalidUserIdentity, err)
	}

	handshakePayload, err := mappers.DecodeHandshake(remoteChat.Handshake)
	if err != nil {
		return nil, err
	}

	secretKeys, err := mappers.UnmapSecretKeys(creds.SecretKeys)
	if err != nil {
		return nil, err
	}
	defer secretKeys.Wipe()

	chatKey, syncKey, err := c.chatManager.FinalizeHandshake(handshakePayload, userIdentity, secretKeys, recipientIdentity)
	if err != nil {
		return nil, err
	}

	if err := c.database.SaveChat(*remoteChat, chatKey, syncKey); err != nil {
		return nil, err
	}

	if remoteChat.Members != nil {
		if err := c.database.SaveUsers(*remoteChat.Members); err != nil {
			fmt.Println("failed to save users:", err)
		}
	}

	return c.database.GetChat(chatID)
}

func (c *BloomClient) getLocalChats() ([]ChatResponse, error) {
	creds, err := c.getCredentials()
	if err != nil {
		return nil, err
	}

	chats, err := c.database.GetChats()
	if err != nil {
		return nil, err
	}

	sort.SliceStable(chats, func(i, j int) bool {
		leftLastMessage := chats[i].LastMessage
		rightLastMessage := chats[j].LastMessage

		if leftLastMessage != nil && rightLastMessage != nil {
			if !leftLastMessage.CreatedAt.Equal(rightLastMessage.CreatedAt) {
				return leftLastMessage.CreatedAt.After(rightLastMessage.CreatedAt)
			}

			return chats[i].ID > chats[j].ID
		}

		if leftLastMessage != nil {
			return true
		}
		if rightLastMessage != nil {
			return false
		}

		return chats[i].ID > chats[j].ID
	})

	result := make([]ChatResponse, 0, len(chats))
	for i := range chats {
		chat := &chats[i]

		recipient, err := getChatRecipient(&domain.Chat{
			RawChat: chat.RawChat,
		}, creds.UserID)
		if err != nil {
			continue
		}

		if _, err := mappers.ConvertUserToIdentity(recipient); err != nil {
			continue
		}

		recipientJSON, err := json.Marshal(recipient)
		if err != nil {
			continue
		}

		var lastMessage *domain.DecryptedMessageWithReply
		if chat.LastMessage != nil {
			mapped := mappers.MapDomainMessageToDecrypted(chat.LastMessage)
			lastMessage = &mapped
		}

		var lastReadMessage *domain.DecryptedMessageWithReply
		if chat.LastReadMessage != nil {
			mapped := mappers.MapDomainMessageToDecrypted(chat.LastReadMessage)
			lastReadMessage = &mapped
		}

		newChatObject := ChatResponse{
			RawChat:         chat.RawChat,
			Me:              creds.UserJSON,
			Recipient:       recipientJSON,
			LastMessage:     lastMessage,
			LastReadMessage: lastReadMessage,
		}

		result = append(result, newChatObject)
	}

	return result, nil
}

func (c *BloomClient) notifyChatsUpdated() {
	c.listenerMu.RLock()
	listener := c.chatsListener
	c.listenerMu.RUnlock()

	if listener == nil {
		return
	}

	localChatsBytes, err := c.GetLocalChats()
	if err != nil {
		fmt.Println("error", err)
		return
	}

	hash := sha256.Sum256(localChatsBytes)

	c.listenerMu.Lock()
	listener = c.chatsListener
	if listener == nil || (c.hasLastChatsHash && c.lastChatsHash == hash) {
		c.listenerMu.Unlock()
		return
	}
	c.lastChatsHash = hash
	c.hasLastChatsHash = true
	c.listenerMu.Unlock()

	listener.OnChatsUpdated(localChatsBytes)
}

func (c *BloomClient) syncRemoteChats(ctx context.Context) {
	_, err := c.getChatsContext(ctx)
	if err != nil {
		fmt.Println("error", err)
		return
	}

	c.notifyChatsUpdated()
}
