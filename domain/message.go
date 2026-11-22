package domain

import (
	"encoding/json"
	"time"

	"github.com/slipe-fun/skid-v4/pkg/messages"
)

type RawMessage struct {
	ID         int        `json:"id"`
	Version    string     `json:"version"`
	Ciphertext string     `json:"ciphertext"`
	Nonce      string     `json:"nonce"`
	Salt       string     `json:"salt"`
	ChatID     int        `json:"chat_id"`
	Seen       *time.Time `json:"seen,omitempty"`
	ReplyTo    *int       `json:"reply_to,omitempty"`
}

func (r *RawMessage) UnmarshalJSON(data []byte) error {
	type rawMessageHelper struct {
		ID         int             `json:"id"`
		Version    string          `json:"version"`
		Ciphertext string          `json:"ciphertext"`
		Nonce      string          `json:"nonce"`
		Salt       string          `json:"salt"`
		ChatID     int             `json:"chat_id"`
		Seen       *time.Time      `json:"seen,omitempty"`
		ReplyTo    json.RawMessage `json:"reply_to,omitempty"`
	}

	var raw rawMessageHelper
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	r.ID = raw.ID
	r.Version = raw.Version
	r.Ciphertext = raw.Ciphertext
	r.Nonce = raw.Nonce
	r.Salt = raw.Salt
	r.ChatID = raw.ChatID
	r.Seen = raw.Seen

	if len(raw.ReplyTo) > 0 && string(raw.ReplyTo) != "null" {
		var id int
		if err := json.Unmarshal(raw.ReplyTo, &id); err == nil {
			r.ReplyTo = &id
		} else {
			var obj struct {
				ID int `json:"id"`
			}
			if err := json.Unmarshal(raw.ReplyTo, &obj); err == nil {
				r.ReplyTo = &obj.ID
			}
		}
	}

	return nil
}

type RawMessageWithReply struct {
	RawMessage
	ReplyToMessage *RawMessage `json:"reply_to_message,omitempty"`
}

func (r *RawMessageWithReply) UnmarshalJSON(data []byte) error {
	type rawMessageHelper struct {
		ID         int             `json:"id"`
		Version    string          `json:"version"`
		Ciphertext string          `json:"ciphertext"`
		Nonce      string          `json:"nonce"`
		Salt       string          `json:"salt"`
		ChatID     int             `json:"chat_id"`
		Seen       *time.Time      `json:"seen,omitempty"`
		ReplyTo    json.RawMessage `json:"reply_to,omitempty"`
	}

	var raw rawMessageHelper
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	r.ID = raw.ID
	r.Version = raw.Version
	r.Ciphertext = raw.Ciphertext
	r.Nonce = raw.Nonce
	r.Salt = raw.Salt
	r.ChatID = raw.ChatID
	r.Seen = raw.Seen

	if len(raw.ReplyTo) > 0 && string(raw.ReplyTo) != "null" {
		var id int
		if err := json.Unmarshal(raw.ReplyTo, &id); err == nil {
			r.ReplyTo = &id
		} else {
			var parentMsg RawMessage
			if err := json.Unmarshal(raw.ReplyTo, &parentMsg); err == nil {
				r.ReplyToMessage = &parentMsg
				r.ReplyTo = &parentMsg.ID
			}
		}
	}

	return nil
}

type MessageWithDecryptedData struct {
	RawMessageWithReply
	messages.Message
	CreatedAt time.Time
}

type Message struct {
	MessageWithDecryptedData
	ReplyToMessage *MessageWithDecryptedData `json:"reply_to_message,omitempty"`
}

type SendMessageRequest struct {
	Version         string `json:"version"`
	Ciphertext      string `json:"ciphertext"`
	Nonce           string `json:"nonce"`
	Salt            string `json:"salt"`
	ChatID          int    `json:"chat_id"`
	ReplyTo         *int   `json:"reply_to,omitempty"`
	ClientMessageID string `json:"client_message_id,omitempty"`
}

type PendingSend struct {
	ChatID          int       `db:"chat_id" json:"chat_id"`
	ReplyTo         int       `db:"reply_to" json:"reply_to"`
	ContentHash     string    `db:"content_hash" json:"content_hash"`
	ClientMessageID string    `db:"client_message_id" json:"client_message_id"`
	CreatedAt       time.Time `db:"created_at" json:"created_at"`
}

type DecryptedMessage struct {
	ID        int        `json:"id"`
	Content   string     `json:"content"`
	AuthorID  string     `json:"author_id"`
	Timestamp int64      `json:"timestamp"`
	CreatedAt time.Time  `json:"created_at"`
	Seen      *time.Time `json:"seen,omitempty"`
}

type DecryptedMessageWithReply struct {
	DecryptedMessage
	ReplyTo *DecryptedMessage `json:"reply_to,omitempty"`
}

type MarkMessagesSeenRequest struct {
	ChatID   int   `json:"chat_id"`
	Messages []int `json:"messages"`
}

type MarkMessagesSeenResponse struct {
	Type     string    `json:"type"`
	UserID   string    `json:"user_id"`
	ChatID   int       `json:"chat_id"`
	SeenAt   time.Time `json:"seen_at"`
	Messages []int     `json:"messages"`
}
