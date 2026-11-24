package database

import (
	"database/sql"
	"errors"

	"github.com/ephmrl-team/bloom-root/logger"
)

func (d *Database) GetChatOldestContiguousID(chatID int) (int, error) {
	row := d.db.QueryRow(`
		SELECT oldest_contiguous_id
		FROM chat_sync_states
		WHERE chat_id = ?
	`, chatID)

	var oldestContiguousID int

	if err := row.Scan(&oldestContiguousID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		logger.Error("database", "oldest chat state load failed", "chat_id", chatID, "error", err)
		return 0, err
	}

	return oldestContiguousID, nil
}

func (d *Database) SetChatOldestContiguousID(chatID, oldestContiguousID int) error {
	_, err := d.db.Exec(`
		INSERT INTO chat_sync_states (chat_id, oldest_contiguous_id)
		VALUES (?, ?)
		ON CONFLICT(chat_id) DO UPDATE SET
			oldest_contiguous_id = excluded.oldest_contiguous_id
	`, chatID, oldestContiguousID)
	if err != nil {
		logger.Error("database", "oldest chat state save failed", "chat_id", chatID, "oldest_id", oldestContiguousID, "error", err)
		return err
	}

	logger.Info("database", "oldest chat state saved", "chat_id", chatID, "oldest_id", oldestContiguousID)

	return nil
}

func (d *Database) GetChatNewestContiguousID(chatID int) (int, error) {
	row := d.db.QueryRow(`
		SELECT newest_contiguous_id
		FROM chat_sync_states
		WHERE chat_id = ?
	`, chatID)

	var newestContiguousID int

	if err := row.Scan(&newestContiguousID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		logger.Error("database", "newest chat state load failed", "chat_id", chatID, "error", err)
		return 0, err
	}

	return newestContiguousID, nil
}

func (d *Database) SetChatNewestContiguousID(chatID, newestContiguousID int) error {
	_, err := d.db.Exec(`
		INSERT INTO chat_sync_states (chat_id, newest_contiguous_id)
		VALUES (?, ?)
		ON CONFLICT(chat_id) DO UPDATE SET
			newest_contiguous_id = excluded.newest_contiguous_id
	`, chatID, newestContiguousID)
	if err != nil {
		logger.Error("database", "newest chat state save failed", "chat_id", chatID, "newest_id", newestContiguousID, "error", err)
		return err
	}

	logger.Info("database", "newest chat state saved", "chat_id", chatID, "newest_id", newestContiguousID)

	return nil
}
