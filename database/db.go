package database

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	sqlite "gosqlite.org"
	"gosqlite.org/vfs/crypto"

	"github.com/ephmrl-team/bloom-root/logger"
)

type Database struct {
	db      *sqlite.DB
	closeMu sync.Mutex
}

func NewDatabase(encryptionKey []byte, storagePath string) (*Database, error) {
	logger.Info("database", "database opening")

	if len(encryptionKey) != 32 {
		logger.Error("database", "database open failed", "reason", "invalid encryption key")
		return nil, fmt.Errorf("invalid encryption key")
	}

	if storagePath == "" {
		logger.Error("database", "database open failed", "reason", "storage path is empty")
		return nil, fmt.Errorf("storage path is empty")
	}

	if err := os.MkdirAll(storagePath, 0o700); err != nil {
		logger.Error("database", "storage directory creation failed", "error", err)
		return nil, fmt.Errorf("failed to create storage directory: %w", err)
	}

	dbPath := filepath.Join(storagePath, "bloom.db")

	db, err := crypto.Open(
		sqlite.Config{
			Path:    dbPath,
			Pragmas: sqlite.RecommendedPragmas(),
		},
		crypto.Options{
			Key: encryptionKey,
		},
	)
	if err != nil {
		logger.Error("database", "encrypted database open failed", "error", err)
		return nil, fmt.Errorf("failed to open encrypted database: %w", err)
	}

	schema := `
	CREATE TABLE IF NOT EXISTS users (
	    id TEXT PRIMARY KEY,
	    username TEXT NOT NULL UNIQUE,
	    display_name TEXT,
	    description TEXT,
	    ml_kem_public_key TEXT NOT NULL,
	    ecdh_public_key TEXT NOT NULL,
	    ed_public_key TEXT NOT NULL,
	    date DATETIME NOT NULL
	);

	CREATE TABLE IF NOT EXISTS chats (
	    id INTEGER PRIMARY KEY,
	    members TEXT NOT NULL,
	    handshake TEXT NOT NULL,
	    chat_key TEXT NOT NULL,
	    sync_key TEXT NOT NULL,
		created_at DATETIME NOT NULL
	);

	CREATE TABLE IF NOT EXISTS messages (
	    id INTEGER PRIMARY KEY,
	    chat_id INTEGER NOT NULL,
	    author_id TEXT NOT NULL,
	    seen DATETIME,
	    reply_to INTEGER,
	    nonce TEXT NOT NULL,
	    content TEXT NOT NULL,
		created_at DATETIME NOT NULL,

	    FOREIGN KEY (chat_id) REFERENCES chats(id) ON DELETE CASCADE,
	    FOREIGN KEY (author_id) REFERENCES users(id) ON DELETE CASCADE,
	    FOREIGN KEY (reply_to) REFERENCES messages(id) ON DELETE SET NULL
	);

	CREATE TABLE IF NOT EXISTS chat_sync_states (
	    chat_id INTEGER PRIMARY KEY,
	    oldest_contiguous_id INTEGER NOT NULL DEFAULT 0,
	    newest_contiguous_id INTEGER NOT NULL DEFAULT 0,

	    FOREIGN KEY (chat_id) REFERENCES chats(id) ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS pending_sends (
	    chat_id INTEGER NOT NULL,
	    reply_to INTEGER NOT NULL,
	    content_hash TEXT NOT NULL,
	    client_message_id TEXT NOT NULL,
	    created_at DATETIME NOT NULL,

	    UNIQUE (chat_id, reply_to, content_hash),

	    FOREIGN KEY (chat_id) REFERENCES chats(id) ON DELETE CASCADE
	);

	CREATE INDEX IF NOT EXISTS idx_pending_sends_created_at
	    ON pending_sends(created_at);

	CREATE INDEX IF NOT EXISTS idx_messages_chat_id_id
	    ON messages(chat_id, id);

	CREATE INDEX IF NOT EXISTS idx_messages_author_id
	    ON messages(author_id);

	CREATE INDEX IF NOT EXISTS idx_messages_reply_to
	    ON messages(reply_to);

	CREATE INDEX IF NOT EXISTS idx_messages_seen
	    ON messages(seen);

	CREATE INDEX IF NOT EXISTS idx_messages_created_at
    	ON messages(created_at);
	`

	if _, err := db.Exec(schema); err != nil {
		db.Close()
		logger.Error("database", "database schema creation failed", "error", err)
		return nil, fmt.Errorf("failed to create schema: %w", err)
	}

	rows, err := db.Query(`
		SELECT 1
		FROM pragma_table_info('chat_sync_states')
		WHERE name = 'newest_contiguous_id'
	`)
	if err != nil {
		db.Close()
		logger.Error("database", "database schema inspection failed", "error", err)
		return nil, fmt.Errorf("failed to inspect chat_sync_states schema: %w", err)
	}
	hasNewestContiguousID := rows.Next()
	rows.Close()

	if !hasNewestContiguousID {
		if _, err := db.Exec(`
			ALTER TABLE chat_sync_states
			ADD COLUMN newest_contiguous_id INTEGER NOT NULL DEFAULT 0
		`); err != nil {
			db.Close()
			logger.Error("database", "database schema migration failed", "error", err)
			return nil, fmt.Errorf("failed to add newest_contiguous_id: %w", err)
		}

		logger.Info("database", "database schema migrated")
	}

	logger.Success("database", "database opened")

	return &Database{db: db}, nil
}

func (d *Database) ClearAll() error {
	logger.Info("database", "database clear started")

	if d == nil || d.db == nil {
		logger.Warn("database", "database clear skipped", "reason", "database is not initialized")
		return fmt.Errorf("database is not initialized")
	}

	tx, err := d.db.Begin()
	if err != nil {
		logger.Error("database", "database clear failed", "reason", "begin transaction", "error", err)
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	for _, table := range []string{
		"pending_sends",
		"messages",
		"chat_sync_states",
		"chats",
		"users",
	} {
		if _, err := tx.Exec("DELETE FROM " + table); err != nil {
			logger.Error("database", "database table clear failed", "table", table, "error", err)
			return fmt.Errorf("failed to clear %s: %w", table, err)
		}
	}

	if err := tx.Commit(); err != nil {
		logger.Error("database", "database clear commit failed", "error", err)
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	if _, err := d.db.Exec("VACUUM"); err != nil {
		logger.Error("database", "database vacuum failed", "error", err)
		return fmt.Errorf("failed to vacuum database: %w", err)
	}

	logger.Success("database", "database cleared")

	return nil
}

func (d *Database) Close() error {
	if d == nil {
		return nil
	}

	logger.Info("database", "database closing")

	d.closeMu.Lock()
	defer d.closeMu.Unlock()

	if d.db == nil {
		logger.Info("database", "database already closed")
		return nil
	}

	db := d.db
	d.db = nil

	_, checkpointErr := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	closeErr := db.Close()
	err := errors.Join(checkpointErr, closeErr)
	if err != nil {
		logger.Error("database", "database close failed", "error", err)
		return err
	}

	logger.Success("database", "database closed")

	return nil
}
