package client

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ephmrl-team/bloom-root/api"
	authClient "github.com/ephmrl-team/bloom-root/api/auth"
	chatClient "github.com/ephmrl-team/bloom-root/api/chat"
	exchangeClient "github.com/ephmrl-team/bloom-root/api/exchange"
	messageClient "github.com/ephmrl-team/bloom-root/api/message"
	userClient "github.com/ephmrl-team/bloom-root/api/user"
	"github.com/ephmrl-team/bloom-root/database"
	"github.com/ephmrl-team/bloom-root/domain"
	authManager "github.com/ephmrl-team/bloom-root/managers/auth"
	chatManager "github.com/ephmrl-team/bloom-root/managers/chat"
	exchangeManager "github.com/ephmrl-team/bloom-root/managers/exchange"
	messageManager "github.com/ephmrl-team/bloom-root/managers/message"
	userManager "github.com/ephmrl-team/bloom-root/managers/user"
	"github.com/slipe-fun/skid-v4/pkg/identity"
)

type ChatsListener interface {
	OnChatsUpdated(chatsJSON []byte)
}

type UserListener interface {
	OnUserUpdated(userJSON []byte)
}

type MessagesListener interface {
	OnNewMessage(messageJSON []byte)
}

type MessagesSeenListener interface {
	OnMessagesSeen(messagesSeenJSON []byte)
}

type ReconnectListener interface {
	OnReconnect()
}

type ConnectionListener interface {
	OnConnected()
	OnDisconnected()
}

type BloomClient struct {
	wsURL           string
	apiClient       *api.Client
	dialContext     DialContextFunc
	authManager     *authManager.AuthManager
	exchangeManager *exchangeManager.ExchangeManager
	userManager     *userManager.UserManager
	chatManager     *chatManager.ChatManager
	messageManager  *messageManager.MessageManager
	credentials     *domain.SavedCredentials
	credentialsMu   sync.RWMutex
	database        *database.Database
	storagePath     string
	encryptionKey   []byte

	exchangeSecretKeys   *identity.SecretKeys
	exchangeUserIdentity *identity.User
	exchangeCancel       context.CancelFunc
	exchangeMu           sync.Mutex

	chatsListener           ChatsListener
	messagesListener        MessagesListener
	messagesSeenListener    MessagesSeenListener
	userListener            UserListener
	reconnectListener       ReconnectListener
	backgroundErrorListener BackgroundErrorListener
	listenerMu              sync.RWMutex
	syncCancel              context.CancelFunc
	syncDone                chan struct{}
	syncMu                  sync.Mutex

	wsConnected        bool
	wsMu               sync.Mutex
	wsCancel           context.CancelFunc
	wsDone             chan struct{}
	wsCancelMu         sync.Mutex
	wsHasConnected     bool
	connectionListener ConnectionListener

	lastChatsHash    [32]byte
	hasLastChatsHash bool

	sessionMu sync.Mutex

	initMu sync.Mutex

	sendMu       sync.Mutex
	sendInflight map[string]*inflightSend
}

type Option func(*clientOptions)

type clientOptions struct {
	dialContext DialContextFunc
}

func WithDialContext(dial DialContextFunc) Option {
	return func(options *clientOptions) {
		if dial != nil {
			options.dialContext = dial
		}
	}
}

func NewClient(baseURL, wsURL, storagePath string, encryptionKey []byte) *BloomClient {
	return NewClientWithOptions(baseURL, wsURL, storagePath, encryptionKey)
}

func NewClientWithOptions(baseURL, wsURL, storagePath string, encryptionKey []byte, opts ...Option) *BloomClient {
	options := clientOptions{
		dialContext: newDialContext(),
	}
	for _, opt := range opts {
		opt(&options)
	}

	c := api.NewClient(baseURL)
	c.SetDialContext(options.dialContext)

	ac := authClient.NewAuthClient(c)
	ec := exchangeClient.NewExchangeClient(c)
	uc := userClient.NewUserClient(c)
	cc := chatClient.NewChatClient(c)
	mc := messageClient.NewMessageClient(c)

	localKey := make([]byte, len(encryptionKey))
	copy(localKey, encryptionKey)

	return &BloomClient{
		wsURL:           wsURL,
		apiClient:       c,
		dialContext:     options.dialContext,
		authManager:     authManager.NewAuthManager(ac),
		exchangeManager: exchangeManager.NewExchangeManager(ec),
		userManager:     userManager.NewUserManager(uc),
		chatManager:     chatManager.NewChatManager(cc),
		messageManager:  messageManager.NewMessageManager(mc),
		storagePath:     storagePath,
		encryptionKey:   localKey,
		sendInflight:    make(map[string]*inflightSend),
	}
}

func (c *BloomClient) Initialize() error {
	c.initMu.Lock()
	defer c.initMu.Unlock()

	if c.database != nil {
		return nil
	}

	dbKey := make([]byte, len(c.encryptionKey))
	copy(dbKey, c.encryptionKey)
	db, err := database.NewDatabase(dbKey, c.storagePath)
	if err != nil {
		return err
	}

	if err := db.DeleteExpiredPendingSends(time.Now().UTC().Add(-pendingSendRetention)); err != nil {
		_ = db.Close()
		return fmt.Errorf("failed to delete expired pending sends: %w", err)
	}

	creds, err := c.loadCredentials()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			c.database = db
			return nil
		}
		_ = db.Close()
		return fmt.Errorf("failed to load credentials: %w", err)
	}

	c.database = db
	c.SetToken(creds.Token)
	c.startWebSocket(context.Background(), c.wsURL)
	return nil
}

func (c *BloomClient) Close() error {
	c.initMu.Lock()
	defer c.initMu.Unlock()

	c.CancelExchange()
	c.stopWebSocket()
	c.StopChatsSync()

	db := c.database
	c.database = nil

	if db != nil {
		return db.Close()
	}

	return nil
}

func (c *BloomClient) SetToken(token string) {
	c.apiClient.SetToken(token)
}
