package database

import (
	"database/sql"
	"errors"
	"time"

	"github.com/ephmrl-team/bloom-root/domain"
	"github.com/ephmrl-team/bloom-root/logger"
)

func (d *Database) GetPendingSend(chatID, replyTo int, contentHash string, notBefore time.Time) (*domain.PendingSend, error) {
	row := d.db.QueryRow(`
		SELECT chat_id, reply_to, content_hash, client_message_id, created_at
		FROM pending_sends
		WHERE chat_id = ? AND reply_to = ? AND content_hash = ? AND created_at >= ?
	`, chatID, replyTo, contentHash, notBefore.UTC())

	var pending domain.PendingSend

	if err := row.Scan(
		&pending.ChatID,
		&pending.ReplyTo,
		&pending.ContentHash,
		&pending.ClientMessageID,
		&pending.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		logger.Error("database", "pending send load failed", "chat_id", chatID, "reply_to", replyTo, "error", err)
		return nil, err
	}

	return &pending, nil
}

func (d *Database) SavePendingSend(pending *domain.PendingSend) error {
	_, err := d.db.Exec(`
		INSERT INTO pending_sends (
			chat_id,
			reply_to,
			content_hash,
			client_message_id,
			created_at
		)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(chat_id, reply_to, content_hash) DO UPDATE SET
			client_message_id = excluded.client_message_id,
			created_at = excluded.created_at
	`,
		pending.ChatID,
		pending.ReplyTo,
		pending.ContentHash,
		pending.ClientMessageID,
		pending.CreatedAt.UTC(),
	)
	if err != nil {
		logger.Error("database", "pending send save failed", "chat_id", pending.ChatID, "reply_to", pending.ReplyTo, "error", err)
		return err
	}

	logger.Info("database", "pending send saved", "chat_id", pending.ChatID, "reply_to", pending.ReplyTo)

	return nil
}

func (d *Database) DeletePendingSend(chatID, replyTo int, contentHash string) error {
	_, err := d.db.Exec(`
		DELETE FROM pending_sends
		WHERE chat_id = ? AND reply_to = ? AND content_hash = ?
	`, chatID, replyTo, contentHash)
	if err != nil {
		logger.Error("database", "pending send delete failed", "chat_id", chatID, "reply_to", replyTo, "error", err)
		return err
	}

	logger.Info("database", "pending send deleted", "chat_id", chatID, "reply_to", replyTo)

	return nil
}

func (d *Database) DeleteExpiredPendingSends(before time.Time) error {
	result, err := d.db.Exec(`
		DELETE FROM pending_sends
		WHERE created_at < ?
	`, before.UTC())
	if err != nil {
		logger.Error("database", "expired pending sends delete failed", "error", err)
		return err
	}

	deleted, err := result.RowsAffected()
	if err != nil {
		logger.Warn("database", "expired pending sends count failed", "error", err)
		return nil
	}

	logger.Info("database", "expired pending sends deleted", "count", deleted)

	return nil
}
