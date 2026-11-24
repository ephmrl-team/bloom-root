package database

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/ephmrl-team/bloom-root/domain"
	"github.com/ephmrl-team/bloom-root/logger"
)

func (d *Database) SaveChat(chat domain.Chat, chatKey, syncKey []byte) error {
	membersJSON, err := json.Marshal(chat.Members)
	if err != nil {
		logger.Error("database", "chat members marshal failed", "chat_id", chat.ID, "error", err)
		return err
	}

	handshakeJSON, err := json.Marshal(chat.Handshake)
	if err != nil {
		logger.Error("database", "chat handshake marshal failed", "chat_id", chat.ID, "error", err)
		return err
	}

	encodedChatKey := base64.StdEncoding.EncodeToString(chatKey)
	encodedSyncKey := base64.StdEncoding.EncodeToString(syncKey)

	_, err = d.db.Exec(`
		INSERT INTO chats (id, members, handshake, chat_key, sync_key, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			members = excluded.members,
			handshake = excluded.handshake,
			chat_key = excluded.chat_key,
			sync_key = excluded.sync_key;
	`, chat.ID, string(membersJSON), string(handshakeJSON), encodedChatKey, encodedSyncKey, chat.CreatedAt)
	if err != nil {
		logger.Error("database", "chat save failed", "chat_id", chat.ID, "error", err)
		return err
	}

	logger.Info("database", "chat saved", "chat_id", chat.ID)

	return nil
}

func (d *Database) SaveChats(chats []domain.ChatWithKeys) error {
	tx, err := d.db.Begin()
	if err != nil {
		logger.Error("database", "chats transaction failed", "count", len(chats), "error", err)
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO chats (
			id,
			members,
			handshake,
			chat_key,
			sync_key,
			created_at
		)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			members = excluded.members,
			handshake = excluded.handshake,
			chat_key = excluded.chat_key,
			sync_key = excluded.sync_key;
	`)
	if err != nil {
		logger.Error("database", "chats prepare failed", "count", len(chats), "error", err)
		return err
	}
	defer stmt.Close()

	for _, chat := range chats {
		membersJSON, err := json.Marshal(chat.Members)
		if err != nil {
			logger.Error("database", "chat members marshal failed", "chat_id", chat.ID, "error", err)
			return err
		}

		handshakeJSON, err := json.Marshal(chat.Handshake)
		if err != nil {
			logger.Error("database", "chat handshake marshal failed", "chat_id", chat.ID, "error", err)
			return err
		}

		encodedChatKey := base64.StdEncoding.EncodeToString(chat.ChatKey)
		encodedSyncKey := base64.StdEncoding.EncodeToString(chat.SyncKey)

		_, err = stmt.Exec(
			chat.ID,
			string(membersJSON),
			string(handshakeJSON),
			encodedChatKey,
			encodedSyncKey,
			chat.CreatedAt,
		)
		if err != nil {
			logger.Error("database", "chat save failed", "chat_id", chat.ID, "error", err)
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		logger.Error("database", "chats commit failed", "count", len(chats), "error", err)
		return err
	}

	logger.Info("database", "chats saved", "count", len(chats))

	return nil
}

func (d *Database) GetChat(chatID int) (*domain.ChatWithKeys, error) {
	row := d.db.QueryRow(`
		SELECT id, members, handshake, chat_key, sync_key, created_at
		FROM chats
		WHERE id = ?
	`, chatID)

	var (
		chat           domain.RawChat
		membersJSON    string
		handshakeJSON  string
		encodedChatKey string
		encodedSyncKey string
	)

	err := row.Scan(&chat.ID, &membersJSON, &handshakeJSON, &encodedChatKey, &encodedSyncKey, &chat.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		logger.Error("database", "chat load failed", "chat_id", chatID, "error", err)
		return nil, err
	}

	if err := json.Unmarshal([]byte(membersJSON), &chat.Members); err != nil {
		logger.Error("database", "chat members unmarshal failed", "chat_id", chatID, "error", err)
		return nil, err
	}

	if err := json.Unmarshal([]byte(handshakeJSON), &chat.Handshake); err != nil {
		logger.Error("database", "chat handshake unmarshal failed", "chat_id", chatID, "error", err)
		return nil, err
	}

	chatKeyBytes, err := base64.StdEncoding.DecodeString(encodedChatKey)
	if err != nil {
		logger.Error("database", "chat key decode failed", "chat_id", chatID, "error", err)
		return nil, err
	}

	syncKeyBytes, err := base64.StdEncoding.DecodeString(encodedSyncKey)
	if err != nil {
		logger.Error("database", "chat sync key decode failed", "chat_id", chatID, "error", err)
		return nil, err
	}

	chatWithKeys := &domain.ChatWithKeys{
		RawChat: chat,
		ChatKey: chatKeyBytes,
		SyncKey: syncKeyBytes,
	}

	lastMsg, err := d.GetChatLastMessage(chatID)
	if err != nil {
		return nil, err
	}
	chatWithKeys.LastMessage = lastMsg

	lastReadMsg, err := d.GetChatLastReadMessage(chatID)
	if err != nil {
		return nil, err
	}
	chatWithKeys.LastReadMessage = lastReadMsg

	logger.Info("database", "chat loaded", "chat_id", chatID)

	return chatWithKeys, nil
}

func (d *Database) GetChats() ([]domain.ChatWithKeys, error) {
	rows, err := d.db.Query(`
		SELECT id, members, handshake, chat_key, sync_key, created_at
		FROM chats
	`)
	if err != nil {
		logger.Error("database", "chats load failed", "error", err)
		return nil, err
	}
	defer rows.Close()

	var chats []domain.ChatWithKeys
	var chatIDs []int

	for rows.Next() {
		var (
			chat           domain.RawChat
			membersJSON    string
			handshakeJSON  string
			encodedChatKey string
			encodedSyncKey string
		)

		if err := rows.Scan(
			&chat.ID,
			&membersJSON,
			&handshakeJSON,
			&encodedChatKey,
			&encodedSyncKey,
			&chat.CreatedAt,
		); err != nil {
			logger.Error("database", "chat scan failed", "error", err)
			return nil, err
		}

		if err := json.Unmarshal([]byte(membersJSON), &chat.Members); err != nil {
			logger.Error("database", "chat members unmarshal failed", "chat_id", chat.ID, "error", err)
			return nil, err
		}

		if err := json.Unmarshal([]byte(handshakeJSON), &chat.Handshake); err != nil {
			logger.Error("database", "chat handshake unmarshal failed", "chat_id", chat.ID, "error", err)
			return nil, err
		}

		chatKeyBytes, err := base64.StdEncoding.DecodeString(encodedChatKey)
		if err != nil {
			logger.Error("database", "chat key decode failed", "chat_id", chat.ID, "error", err)
			return nil, err
		}

		syncKeyBytes, err := base64.StdEncoding.DecodeString(encodedSyncKey)
		if err != nil {
			logger.Error("database", "chat sync key decode failed", "chat_id", chat.ID, "error", err)
			return nil, err
		}

		chats = append(chats, domain.ChatWithKeys{
			RawChat: chat,
			ChatKey: chatKeyBytes,
			SyncKey: syncKeyBytes,
		})

		chatIDs = append(chatIDs, chat.ID)
	}

	if err := rows.Err(); err != nil {
		logger.Error("database", "chats read failed", "error", err)
		return nil, err
	}

	if len(chatIDs) > 0 {
		lastMessagesMap, err := d.GetChatsLastMessages(chatIDs)
		if err != nil {
			return nil, err
		}

		lastReadMessagesMap, err := d.GetChatsLastReadMessages(chatIDs)
		if err != nil {
			return nil, err
		}

		for i := range chats {
			chats[i].LastMessage = lastMessagesMap[chats[i].ID]
			chats[i].LastReadMessage = lastReadMessagesMap[chats[i].ID]
		}
	}

	logger.Info("database", "chats loaded", "count", len(chats))

	return chats, nil
}

func (d *Database) EditChatMembers(chatID int, newMembers []domain.User) error {
	membersJSON, err := json.Marshal(newMembers)
	if err != nil {
		logger.Error("database", "chat members marshal failed", "chat_id", chatID, "error", err)
		return err
	}

	query := `UPDATE chats SET members = ? WHERE id = ?;`
	result, err := d.db.Exec(query, string(membersJSON), chatID)
	if err != nil {
		logger.Error("database", "chat members update failed", "chat_id", chatID, "error", err)
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		logger.Error("database", "chat members affected rows failed", "chat_id", chatID, "error", err)
		return err
	}
	if rowsAffected == 0 {
		logger.Warn("database", "chat not found", "chat_id", chatID)
		return fmt.Errorf("chat with ID %d not found", chatID)
	}

	logger.Info("database", "chat members updated", "chat_id", chatID, "count", len(newMembers))

	return nil
}

func (d *Database) GetMissingChatIDs(chatIDs []int) ([]int, error) {
	if len(chatIDs) == 0 {
		return nil, nil
	}

	placeholders := make([]string, len(chatIDs))
	args := make([]any, len(chatIDs))

	for i, id := range chatIDs {
		placeholders[i] = "?"
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT id
		FROM chats
		WHERE id IN (%s)
	`, strings.Join(placeholders, ","))

	rows, err := d.db.Query(query, args...)
	if err != nil {
		logger.Error("database", "missing chats lookup failed", "chat_count", len(chatIDs), "error", err)
		return nil, err
	}
	defer rows.Close()

	existing := make(map[int]struct{})

	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			logger.Error("database", "missing chats scan failed", "chat_count", len(chatIDs), "error", err)
			return nil, err
		}
		existing[id] = struct{}{}
	}

	if err := rows.Err(); err != nil {
		logger.Error("database", "missing chats read failed", "chat_count", len(chatIDs), "error", err)
		return nil, err
	}

	var missing []int
	for _, id := range chatIDs {
		if _, ok := existing[id]; !ok {
			missing = append(missing, id)
		}
	}

	logger.Info("database", "missing chats checked", "chat_count", len(chatIDs), "missing_count", len(missing))

	return missing, nil
}
