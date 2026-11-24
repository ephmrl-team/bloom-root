package client

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/ephmrl-team/bloom-root/domain"
	"github.com/ephmrl-team/bloom-root/logger"
	"github.com/ephmrl-team/bloom-root/mappers"
	"github.com/gorilla/websocket"
	"github.com/slipe-fun/skid-v4/pkg/identity"
	"github.com/slipe-fun/skid-v4/pkg/messages"
)

const exchangeTimeout = 5 * time.Minute

type exchangeEvent struct {
	Type   string `json:"type"`
	UserID string `json:"user_id"`
}

type exchangeInit struct {
	Type   string `json:"type"`
	UserID string `json:"user_id"`
	domain.PublicKeys
}

type exchangeHandshake struct {
	Type             string `json:"type"`
	UserID           string `json:"user_id"`
	domain.Handshake `json:"handshake"`
}

type exchangeMessage struct {
	Type       string `json:"type"`
	UserID     string `json:"user_id"`
	Version    string `json:"version"`
	Ciphertext string `json:"ciphertext"`
	Nonce      string `json:"nonce"`
	Salt       string `json:"salt"`
}

type exchangeError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

type ExchangeSession struct {
	RoomID      string `json:"room_id"`
	Fingerprint string `json:"fingerprint"`
}

func validateExchangeType(exchangeType string) error {
	if exchangeType != "push" && exchangeType != "pull" {
		return fmt.Errorf("invalid exchange type: %s", exchangeType)
	}

	return nil
}

func exchangeContextError(ctx context.Context) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("%w after %s", domain.ErrExchangeTimeout, exchangeTimeout)
	}

	return ctx.Err()
}

func (c *BloomClient) sendExchangeEvent(eventType string, conn *websocket.Conn) error {
	askForHandshakeMessage := exchangeEvent{
		Type:   eventType,
		UserID: c.exchangeUserIdentity.ID,
	}

	eventBytes, err := json.Marshal(askForHandshakeMessage)
	if err != nil {
		return fmt.Errorf("failed to marshal: %w", err)
	}

	if err := conn.WriteMessage(websocket.TextMessage, eventBytes); err != nil {
		return fmt.Errorf("failed to send: %w", err)
	}

	return nil
}

func (c *BloomClient) generateExchangeSession(exchangeType string) error {
	userIdentity, secretKeys, _, _, _, _, err := identity.GenerateIdentity()
	if err != nil {
		logger.Error("exchange", "exchange identity generation failed", "type", exchangeType, "error", err)
		return err
	}

	c.exchangeMu.Lock()
	if c.exchangeSecretKeys != nil {
		c.exchangeSecretKeys.Wipe()
		c.exchangeSecretKeys = nil
	}
	c.exchangeSecretKeys = secretKeys
	c.exchangeUserIdentity = userIdentity
	c.exchangeMu.Unlock()

	logger.Success("exchange", "exchange identity generated", "type", exchangeType, "user_id", userIdentity.ID)

	return nil
}

func (c *BloomClient) GenerateExchangeFingerprint() string {
	c.exchangeMu.Lock()
	defer c.exchangeMu.Unlock()

	if c.exchangeUserIdentity == nil {
		return ""
	}

	fingerprintBytes := sha256.Sum256(c.exchangeUserIdentity.PublicKeys.MlKem768)
	return base64.StdEncoding.EncodeToString(fingerprintBytes[:])
}

func (c *BloomClient) startExchangeSession(ctx context.Context, exchangeType string) (string, string, error) {
	logger.Info("exchange", "exchange session starting", "type", exchangeType)

	roomID, err := c.exchangeManager.StartSession(ctx)
	if err != nil {
		logger.Error("exchange", "exchange session start failed", "type", exchangeType, "error", err)
		return "", "", err
	}

	if err := c.generateExchangeSession(exchangeType); err != nil {
		logger.Error("exchange", "exchange session identity failed", "type", exchangeType, "room_id", roomID, "error", err)
		return "", "", err
	}

	fingerprint := c.GenerateExchangeFingerprint()

	logger.Success("exchange", "exchange session started", "type", exchangeType, "room_id", roomID, "fingerprint", fingerprint)

	return roomID, fingerprint, nil
}

func (c *BloomClient) StartExchangeSession(token *CancelToken, exchangeType string) ([]byte, error) {
	if err := validateExchangeType(exchangeType); err != nil {
		logger.Error("exchange", "exchange session type is invalid", "type", exchangeType, "error", err)
		return nil, err
	}

	roomID, fingerprint, err := c.startExchangeSession(token.context(), exchangeType)
	if err != nil {
		return nil, err
	}

	return json.Marshal(ExchangeSession{
		RoomID:      roomID,
		Fingerprint: fingerprint,
	})
}

func (c *BloomClient) CancelExchange() {
	c.exchangeMu.Lock()
	defer c.exchangeMu.Unlock()
	if c.exchangeCancel != nil {
		c.exchangeCancel()
		c.exchangeCancel = nil
		logger.Info("exchange", "exchange cancelled")
	}
}

func (c *BloomClient) Exchange(token *CancelToken, exchangeType, roomID, fingerprint string) (recoveryKey string, err error) {
	defer func() {
		if r := recover(); r != nil {
			recoveryKey = ""
			err = fmt.Errorf("exchange panic: %v", r)
			logger.Error("exchange", "exchange panicked", "type", exchangeType, "room_id", roomID, "error", r)
		}

		if err != nil {
			logger.Error("exchange", "exchange failed", "type", exchangeType, "room_id", roomID, "error", err)
			return
		}

		logger.Success("exchange", "exchange completed", "type", exchangeType, "room_id", roomID)
	}()

	logger.Info("exchange", "exchange starting", "type", exchangeType, "room_id", roomID, "fingerprint", fingerprint)

	if err := validateExchangeType(exchangeType); err != nil {
		return "", err
	}

	if roomID == "" {
		return "", errors.New("room id is empty")
	}

	if fingerprint == "" {
		return "", errors.New("fingerprint is empty")
	}

	isScanner := c.GenerateExchangeFingerprint() != fingerprint

	if isScanner {
		logger.Info("exchange", "exchange scanner mode", "room_id", roomID)
	}

	if isScanner {
		if err := c.generateExchangeSession(exchangeType); err != nil {
			return "", err
		}
	}

	c.exchangeMu.Lock()
	initialized := c.exchangeUserIdentity != nil && c.exchangeSecretKeys != nil
	c.exchangeMu.Unlock()
	if !initialized {
		logger.Error("exchange", "exchange session is not initialized", "type", exchangeType, "room_id", roomID)
		return "", errors.New("exchange session is not initialized")
	}

	ctx, cancel := context.WithTimeout(token.context(), exchangeTimeout)

	c.exchangeMu.Lock()
	c.exchangeCancel = cancel
	c.exchangeMu.Unlock()

	defer func() {
		c.exchangeMu.Lock()
		if c.exchangeSecretKeys != nil {
			c.exchangeSecretKeys.Wipe()
			c.exchangeSecretKeys = nil
		}
		c.exchangeUserIdentity = nil
		if c.exchangeCancel != nil {
			c.exchangeCancel = nil
		}
		c.exchangeMu.Unlock()
		cancel()
	}()

	wsURL := strings.Replace(c.wsURL, "/ws", "/exchange/ws", 1)
	url := fmt.Sprintf("%s?room_id=%s", wsURL, roomID)

	var creds *domain.SavedCredentials
	if exchangeType == "push" {
		var err error
		creds, err = c.loadCredentials()
		if err != nil {
			logger.Error("exchange", "exchange credentials load failed", "type", exchangeType, "room_id", roomID, "error", err)
			return "", fmt.Errorf("failed to load credentials for push exchange: %w", err)
		}
		if len(creds.RawRecoveryKey) != 16 {
			logger.Error("exchange", "exchange recovery key is invalid", "type", exchangeType, "room_id", roomID, "length", len(creds.RawRecoveryKey))
			return "", errors.New("invalid raw recovery key length")
		}
	}

	endpoint := strings.Split(url, "?")[0]

	conn, _, err := c.websocketDialer().DialContext(ctx, url, nil)
	if err != nil {
		logger.Error("exchange", "exchange websocket dial failed", "type", exchangeType, "room_id", roomID, "endpoint", endpoint, "error", err)
		return "", fmt.Errorf("failed to dial exchange websocket: %w", err)
	}
	defer conn.Close()

	logger.Success("exchange", "exchange websocket connected", "type", exchangeType, "room_id", roomID, "endpoint", endpoint)

	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetReadDeadline(deadline); err != nil {
			logger.Error("exchange", "exchange read deadline set failed", "type", exchangeType, "room_id", roomID, "error", err)
			return "", fmt.Errorf("failed to set exchange read deadline: %w", err)
		}
	}

	identityMessage := &exchangeInit{
		Type:   "exchange.identity",
		UserID: c.exchangeUserIdentity.ID,
		PublicKeys: domain.PublicKeys{
			MlKem768: base64.StdEncoding.EncodeToString(c.exchangeUserIdentity.PublicKeys.MlKem768),
			X448:     base64.StdEncoding.EncodeToString(c.exchangeUserIdentity.PublicKeys.X448),
			Ed448:    base64.StdEncoding.EncodeToString(c.exchangeUserIdentity.PublicKeys.Ed448),
		},
	}

	identityBytes, err := json.Marshal(identityMessage)
	if err != nil {
		logger.Error("exchange", "exchange identity marshal failed", "type", exchangeType, "room_id", roomID, "error", err)
		return "", fmt.Errorf("failed to marshal: %w", err)
	}

	if err := conn.WriteMessage(websocket.TextMessage, identityBytes); err != nil {
		logger.Error("exchange", "exchange identity send failed", "type", exchangeType, "room_id", roomID, "error", err)
		return "", fmt.Errorf("failed to send: %w", err)
	}

	logger.Info("exchange", "exchange identity sent", "type", exchangeType, "room_id", roomID, "user_id", identityMessage.UserID)

	if err := c.sendExchangeEvent("exchange.ask_for_identity", conn); err != nil {
		logger.Error("exchange", "exchange ask for identity send failed", "type", exchangeType, "room_id", roomID, "error", err)
		return "", err
	}

	var (
		recipientIdentity       *identity.User
		handshake               *identity.HandshakePayload
		chatKey                 []byte
		syncKey                 []byte
		pendingHandshakeRequest bool
		pendingRecoveryKey      bool
	)

	for {
		select {
		case <-ctx.Done():
			return "", exchangeContextError(ctx)
		default:
			_, message, err := conn.ReadMessage()
			if err != nil {
				if ctx.Err() != nil {
					return "", exchangeContextError(ctx)
				}

				var netErr net.Error
				if errors.As(err, &netErr) && netErr.Timeout() {
					return "", fmt.Errorf("%w after %s", domain.ErrExchangeTimeout, exchangeTimeout)
				}

				return "", fmt.Errorf("websocket read error: %w", err)
			}

			var event WsEvent
			if err := json.Unmarshal(message, &event); err != nil {
				logger.Warn("exchange", "exchange event unmarshal failed", "type", exchangeType, "room_id", roomID, "error", err)
				continue
			}

			logger.Info("exchange", "exchange event received", "type", exchangeType, "room_id", roomID, "event", event.Type)

			switch event.Type {
			case "exchange.identity":
				var event exchangeInit
				if err := json.Unmarshal(message, &event); err != nil {
					logger.Warn("exchange", "exchange.identity unmarshal failed", "room_id", roomID, "error", err)
					continue
				}

				if event.UserID == c.exchangeUserIdentity.ID {
					continue
				}

				if isScanner {
					decodedMlKem, err := base64.StdEncoding.DecodeString(event.PublicKeys.MlKem768)
					if err != nil {
						return "", fmt.Errorf("failed to decode incoming ml_kem public key: %w", err)
					}

					incomingFingerprintBytes := sha256.Sum256(decodedMlKem)
					incomingFingerprint := base64.StdEncoding.EncodeToString(incomingFingerprintBytes[:])

					if incomingFingerprint != fingerprint {
						return "", fmt.Errorf("incoming public key fingerprint does not match QR code")
					}
				}

				recipientIdentity, err = mappers.ConvertPublicKeysToUser(event.UserID, &event.PublicKeys)
				if err != nil {
					return "", err
				}

				if exchangeType == "push" && pendingHandshakeRequest {
					pendingHandshakeRequest = false

					handshake, chatKey, syncKey, err = identity.InitiateKeyExchange(c.exchangeUserIdentity, c.exchangeSecretKeys, recipientIdentity)
					if err != nil {
						return "", err
					}

					mappedHandshake := mappers.MapHandshake(handshake)

					exchangeHandshakeMessage := exchangeHandshake{
						Type:      "exchange.handshake",
						UserID:    c.exchangeUserIdentity.ID,
						Handshake: *mappedHandshake,
					}

					handshakeBytes, err := json.Marshal(exchangeHandshakeMessage)
					if err != nil {
						return "", fmt.Errorf("failed to marshal: %w", err)
					}

					if err := conn.WriteMessage(websocket.TextMessage, handshakeBytes); err != nil {
						return "", fmt.Errorf("failed to send: %w", err)
					}
				}

				if exchangeType == "pull" {
					if err := c.sendExchangeEvent("exchange.ask_for_handshake", conn); err != nil {
						return "", err
					}
				}

			case "exchange.handshake":
				var event exchangeHandshake
				if err := json.Unmarshal(message, &event); err != nil {
					logger.Warn("exchange", "exchange.handshake unmarshal failed", "room_id", roomID, "error", err)
					continue
				}

				if event.UserID == c.exchangeUserIdentity.ID {
					continue
				}

				handshake, err = mappers.DecodeHandshake(&event.Handshake)
				if err != nil {
					return "", err
				}

				if recipientIdentity == nil {
					if err := c.sendExchangeEvent("exchange.ask_for_identity", conn); err != nil {
						return "", err
					}
					continue
				}

				if exchangeType == "pull" {
					if err := c.sendExchangeEvent("exchange.ask_for_recovery_key", conn); err != nil {
						return "", err
					}
				}

				chatKey, syncKey, err = c.chatManager.FinalizeHandshake(handshake, c.exchangeUserIdentity, c.exchangeSecretKeys, recipientIdentity)
				if err != nil {
					return "", err
				}

				if exchangeType == "push" && pendingRecoveryKey {
					pendingRecoveryKey = false

					message, err := messages.Encrypt(chatKey, creds.RawRecoveryKey, syncKey, c.exchangeUserIdentity, recipientIdentity)
					if err != nil {
						return "", err
					}

					exchangeEncryptedRecoveryKeyMessage := exchangeMessage{
						Type:       "exchange.recovery_key",
						Version:    base64.StdEncoding.EncodeToString(message.Version),
						Ciphertext: base64.StdEncoding.EncodeToString(message.Ciphertext),
						Nonce:      base64.StdEncoding.EncodeToString(message.Nonce),
						Salt:       base64.StdEncoding.EncodeToString(message.Salt),
					}

					recoveryKeyBytes, err := json.Marshal(exchangeEncryptedRecoveryKeyMessage)
					if err != nil {
						return "", fmt.Errorf("failed to marshal: %w", err)
					}

					if err := conn.WriteMessage(websocket.TextMessage, recoveryKeyBytes); err != nil {
						return "", fmt.Errorf("failed to send: %w", err)
					}
				}

			case "exchange.recovery_key":
				var event exchangeMessage
				if err := json.Unmarshal(message, &event); err != nil {
					logger.Warn("exchange", "exchange.recovery_key unmarshal failed", "room_id", roomID, "error", err)
					continue
				}

				if event.UserID == c.exchangeUserIdentity.ID {
					continue
				}

				if recipientIdentity == nil {
					if err := c.sendExchangeEvent("exchange.ask_for_identity", conn); err != nil {
						return "", err
					}
					continue
				}

				if chatKey == nil || syncKey == nil {
					if err := c.sendExchangeEvent("exchange.ask_for_handshake", conn); err != nil {
						return "", err
					}
					continue
				}

				version, err := base64.StdEncoding.DecodeString(event.Version)
				if err != nil {
					return "", err
				}

				ciphertext, err := base64.StdEncoding.DecodeString(event.Ciphertext)
				if err != nil {
					return "", err
				}

				nonce, err := base64.StdEncoding.DecodeString(event.Nonce)
				if err != nil {
					return "", err
				}

				salt, err := base64.StdEncoding.DecodeString(event.Salt)
				if err != nil {
					return "", err
				}

				decryptedMessage, err := messages.Decrypt(chatKey, messages.EncryptedMessage{
					Version:    version,
					Ciphertext: ciphertext,
					Nonce:      nonce,
					Salt:       salt,
				}, syncKey, c.exchangeUserIdentity, recipientIdentity)
				if err != nil {
					return "", fmt.Errorf("failed to decrypt recovery key: %w", err)
				}

				if err := c.sendExchangeEvent("exchange.confirm", conn); err != nil {
					logger.Error("exchange", "exchange confirm send failed", "type", exchangeType, "room_id", roomID, "error", err)
					return "", err
				}

				return hex.EncodeToString(decryptedMessage.Content), nil

			case "exchange.confirm":
				var event exchangeEvent
				if err := json.Unmarshal(message, &event); err != nil {
					logger.Warn("exchange", "exchange.confirm unmarshal failed", "room_id", roomID, "error", err)
					continue
				}

				if event.UserID == c.exchangeUserIdentity.ID {
					continue
				}

				if creds == nil {
					continue
				}

				return hex.EncodeToString(creds.RawRecoveryKey), nil

			case "exchange.ask_for_identity":
				var event exchangeEvent
				if err := json.Unmarshal(message, &event); err != nil {
					logger.Warn("exchange", "exchange.ask_for_identity unmarshal failed", "room_id", roomID, "error", err)
					continue
				}

				if event.UserID == c.exchangeUserIdentity.ID {
					continue
				}

				if err := conn.WriteMessage(websocket.TextMessage, identityBytes); err != nil {
					return "", fmt.Errorf("failed to send: %w", err)
				}

			case "exchange.ask_for_handshake":
				var event exchangeEvent
				if err := json.Unmarshal(message, &event); err != nil {
					logger.Warn("exchange", "exchange.ask_for_handshake unmarshal failed", "room_id", roomID, "error", err)
					continue
				}

				if event.UserID == c.exchangeUserIdentity.ID {
					continue
				}

				if recipientIdentity == nil {
					pendingHandshakeRequest = true
					if err := c.sendExchangeEvent("exchange.ask_for_identity", conn); err != nil {
						return "", err
					}
					continue
				}

				handshake, chatKey, syncKey, err = identity.InitiateKeyExchange(c.exchangeUserIdentity, c.exchangeSecretKeys, recipientIdentity)
				if err != nil {
					return "", err
				}

				mappedHandshake := mappers.MapHandshake(handshake)

				exchangeHandshakeMessage := exchangeHandshake{
					Type:      "exchange.handshake",
					UserID:    c.exchangeUserIdentity.ID,
					Handshake: *mappedHandshake,
				}

				handshakeBytes, err := json.Marshal(exchangeHandshakeMessage)
				if err != nil {
					return "", fmt.Errorf("failed to marshal: %w", err)
				}

				if err := conn.WriteMessage(websocket.TextMessage, handshakeBytes); err != nil {
					return "", fmt.Errorf("failed to send: %w", err)
				}

			case "exchange.ask_for_recovery_key":
				var event exchangeEvent
				if err := json.Unmarshal(message, &event); err != nil {
					logger.Warn("exchange", "exchange.ask_for_recovery_key unmarshal failed", "room_id", roomID, "error", err)
					continue
				}

				if event.UserID == c.exchangeUserIdentity.ID {
					continue
				}

				if creds == nil {
					continue
				}

				if recipientIdentity == nil {
					if err := c.sendExchangeEvent("exchange.ask_for_identity", conn); err != nil {
						return "", err
					}
					continue
				}

				if chatKey == nil || syncKey == nil {
					pendingRecoveryKey = true
					if err := c.sendExchangeEvent("exchange.ask_for_handshake", conn); err != nil {
						return "", err
					}
					continue
				}

				message, err := messages.Encrypt(chatKey, creds.RawRecoveryKey, syncKey, c.exchangeUserIdentity, recipientIdentity)
				if err != nil {
					return "", err
				}

				exchangeEncryptedRecoveryKeyMessage := exchangeMessage{
					Type:       "exchange.recovery_key",
					Version:    base64.StdEncoding.EncodeToString(message.Version),
					Ciphertext: base64.StdEncoding.EncodeToString(message.Ciphertext),
					Nonce:      base64.StdEncoding.EncodeToString(message.Nonce),
					Salt:       base64.StdEncoding.EncodeToString(message.Salt),
				}

				recoveryKeyBytes, err := json.Marshal(exchangeEncryptedRecoveryKeyMessage)
				if err != nil {
					return "", fmt.Errorf("failed to marshal: %w", err)
				}

				if err := conn.WriteMessage(websocket.TextMessage, recoveryKeyBytes); err != nil {
					return "", fmt.Errorf("failed to send: %w", err)
				}

			case "message_error":
				var event exchangeError
				if err := json.Unmarshal(message, &event); err != nil {
					logger.Warn("exchange", "message_error unmarshal failed", "room_id", roomID, "error", err)
					continue
				}

				logger.Error("exchange", "exchange server error", "type", exchangeType, "room_id", roomID, "message", event.Message)

				if event.Message == "" {
					return "", errors.New("exchange failed")
				}

				return "", errors.New(event.Message)
			}
		}
	}
}
