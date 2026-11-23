package chat

import (
	"context"
	"errors"
	"strconv"

	"github.com/ephmrl-team/bloom-root/crypto"
	"github.com/ephmrl-team/bloom-root/domain"
	"github.com/ephmrl-team/bloom-root/mappers"
	"github.com/slipe-fun/skid-v4/pkg/identity"
	"github.com/slipe-fun/skid-v4/pkg/messages"
	"github.com/slipe-fun/skid-v4/pkg/protocol"
	"github.com/tink-crypto/tink-go/v2/subtle/random"
)

func (c *ChatManager) Create(
	ctx context.Context,
	sender *identity.User,
	receiver *identity.User,
	secretKeys *identity.SecretKeys,
) (*domain.Chat, []byte, []byte, error) {
	handshake, chatKey, syncKey, err := identity.InitiateKeyExchange(sender, secretKeys, receiver)
	if err != nil {
		return nil, nil, nil, err
	}

	createChatResponse, err := c.chatClient.CreatePrivateChat(ctx, receiver.ID, handshake)
	if err != nil {
		return nil, nil, nil, err
	}

	return createChatResponse, chatKey, syncKey, nil
}

func (c *ChatManager) CreateGroup(
	ctx context.Context,
	sender *identity.User,
	secretKeys *identity.SecretKeys,
	title string,
	users *[]domain.User,
) (*domain.Chat, []byte, error) {
	if users == nil {
		return nil, nil, errors.New("users are nil")
	}

	groupKey := random.GetRandomBytes(32)

	var members []domain.GroupMemberRequest

	for i := range *users {
		users := *users
		user := users[i]

		recipientIdentity, err := mappers.ConvertUserToIdentity(&user)
		if err != nil {
			continue
		}

		handshake, chatKey, syncKey, err := identity.InitiateKeyExchange(sender, secretKeys, recipientIdentity)
		if err != nil {
			continue
		}

		mappedHandshake := mappers.MapHandshake(handshake)

		_ = mappedHandshake
		member := domain.GroupMemberRequest{
			MemberID:  user.ID,
			Handshake: *mappedHandshake,
		}

		encryptedGroupKey, err := messages.Encrypt(chatKey, groupKey, syncKey, sender, recipientIdentity)
		if err != nil {
			continue
		}

		member.EncryptedGroupKey = *mappers.MapEncryptedGroupKey(encryptedGroupKey)

		members = append(members, member)
	}

	createGroupRequest, err := c.chatClient.CreateGroupChat(ctx, title, members)
	if err != nil {
		return nil, nil, err
	}

	groupEncryptionKey, err := crypto.HKDF(groupKey, []byte(strconv.Itoa(createGroupRequest.ID)), protocol.Current.Label("group_encryption_key"), 32)
	if err != nil {
		return nil, nil, err
	}

	return createGroupRequest, groupEncryptionKey, nil
}
