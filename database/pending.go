package database

import (
	"database/sql"
	"errors"
	"time"

	"github.com/ephmrl-team/bloom-root/domain"
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

	return err
}

func (d *Database) DeletePendingSend(chatID, replyTo int, contentHash string) error {
	_, err := d.db.Exec(`
		DELETE FROM pending_sends
		WHERE chat_id = ? AND reply_to = ? AND content_hash = ?
	`, chatID, replyTo, contentHash)

	return err
}

func (d *Database) DeleteExpiredPendingSends(before time.Time) error {
	_, err := d.db.Exec(`
		DELETE FROM pending_sends
		WHERE created_at < ?
	`, before.UTC())

	return err
}
