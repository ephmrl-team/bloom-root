package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ephmrl-team/bloom-root/crypto"
	"github.com/ephmrl-team/bloom-root/domain"
	"github.com/ephmrl-team/bloom-root/logger"
	"github.com/ephmrl-team/bloom-root/mappers"
)

func copyPublicCredentials(creds *domain.SavedCredentials) *domain.SavedCredentials {
	if creds == nil {
		return nil
	}

	return &domain.SavedCredentials{
		UserID: creds.UserID,
		PublicKeys: domain.PublicKeys{
			MlKem768: creds.PublicKeys.MlKem768,
			X448:     creds.PublicKeys.X448,
			Ed448:    creds.PublicKeys.Ed448,
		},
		UserJSON: append([]byte(nil), creds.UserJSON...),
		Token:    creds.Token,
	}
}

func writeFileAtomic(filePath string, data []byte) error {
	dir := filepath.Dir(filePath)
	tmpFile, err := os.CreateTemp(dir, ".session-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()

	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()

	if err := tmpFile.Chmod(0600); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, filePath); err != nil {
		return err
	}

	cleanup = false
	return nil
}

func (c *BloomClient) saveCredentials(creds *domain.SavedCredentials) error {
	c.sessionMu.Lock()
	defer c.sessionMu.Unlock()

	return c.saveCredentialsLocked(creds)
}

func (c *BloomClient) saveCredentialsLocked(creds *domain.SavedCredentials) error {
	if creds == nil {
		logger.Error("credentials", "credentials are nil")
		return errors.New("credentials are nil")
	}

	if _, err := mappers.ConvertCredentialsToUser(creds); err != nil {
		logger.Error("credentials", "credentials validation failed", "user_id", creds.UserID, "error", err)
		return err
	}

	plainText, err := json.Marshal(creds)
	if err != nil {
		logger.Error("credentials", "credentials marshal failed", "user_id", creds.UserID, "error", err)
		return err
	}
	defer crypto.Zero(plainText)

	if len(c.encryptionKey) != 32 {
		logger.Error("credentials", "invalid encryption key", "user_id", creds.UserID)
		return errors.New("invalid encryption key")
	}

	ciphertext, err := crypto.Encrypt(c.encryptionKey, plainText, nil)
	if err != nil {
		logger.Error("credentials", "credentials encryption failed", "user_id", creds.UserID, "error", err)
		return err
	}

	filePath := filepath.Join(c.storagePath, "session.dat")
	if err := writeFileAtomic(filePath, ciphertext); err != nil {
		logger.Error("credentials", "session file write failed", "user_id", creds.UserID, "error", err)
		return err
	}

	crypto.Zero(creds.RawRecoveryKey)
	crypto.Zero(creds.MasterKey)
	crypto.Zero(creds.SecretKeys)

	publicCreds := copyPublicCredentials(creds)

	c.credentialsMu.Lock()
	c.credentials = publicCreds
	c.credentialsMu.Unlock()

	logger.Success("credentials", "credentials saved", "user_id", creds.UserID)

	return nil
}

func (c *BloomClient) getCredentials() (*domain.SavedCredentials, error) {
	c.credentialsMu.RLock()
	creds := c.credentials
	c.credentialsMu.RUnlock()

	if creds == nil {
		return nil, domain.ErrNotAuthenticated
	}

	return creds, nil
}

func (c *BloomClient) loadCredentials() (*domain.SavedCredentials, error) {
	c.sessionMu.Lock()
	defer c.sessionMu.Unlock()

	return c.loadCredentialsLocked()
}

func (c *BloomClient) loadCredentialsLocked() (*domain.SavedCredentials, error) {
	filePath := filepath.Join(c.storagePath, "session.dat")

	ciphertext, err := os.ReadFile(filePath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			logger.Error("credentials", "session file read failed", "error", err)
		}
		return nil, err
	}

	if len(c.encryptionKey) != 32 {
		logger.Error("credentials", "invalid encryption key")
		return nil, errors.New("invalid encryption key")
	}

	plainText, err := crypto.Decrypt(c.encryptionKey, ciphertext, nil)
	if err != nil {
		logger.Error("credentials", "credentials decryption failed", "error", err)
		return nil, err
	}
	defer crypto.Zero(plainText)

	var creds domain.SavedCredentials
	err = json.Unmarshal(plainText, &creds)
	if err != nil {
		logger.Error("credentials", "credentials unmarshal failed", "error", err)
		return nil, err
	}

	if _, err := mappers.ConvertCredentialsToUser(&creds); err != nil {
		logger.Error("credentials", "credentials validation failed", "user_id", creds.UserID, "error", err)
		return nil, err
	}

	publicCreds := copyPublicCredentials(&creds)

	c.credentialsMu.Lock()
	c.credentials = publicCreds
	c.credentialsMu.Unlock()

	logger.Success("credentials", "credentials loaded", "user_id", creds.UserID)

	return &creds, nil
}

func (c *BloomClient) UpdateCredentialsUserJSON(userJSON []byte) error {
	c.sessionMu.Lock()
	defer c.sessionMu.Unlock()

	creds, err := c.loadCredentialsLocked()
	if err != nil {
		logger.Error("credentials", "credentials load failed", "error", err)
		return err
	}

	creds.UserJSON = append([]byte(nil), userJSON...)

	if err := c.saveCredentialsLocked(creds); err != nil {
		logger.Error("credentials", "credentials update failed", "user_id", creds.UserID, "error", err)
		return err
	}

	logger.Success("credentials", "credentials updated", "user_id", creds.UserID)

	return nil
}

func (c *BloomClient) ClearCredentials() {
	logger.Info("credentials", "clearing credentials")

	c.CancelExchange()
	c.stopWebSocket()
	c.StopChatsSync()

	var clearErrs []error

	c.sessionMu.Lock()
	filePath := filepath.Join(c.storagePath, "session.dat")
	if err := os.Remove(filePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		logger.Error("credentials", "session file remove failed", "error", err)
		clearErrs = append(clearErrs, fmt.Errorf("failed to remove session file: %w", err))
	}

	c.credentialsMu.Lock()
	c.credentials = nil
	c.credentialsMu.Unlock()
	c.sessionMu.Unlock()

	c.apiClient.SetToken("")
	c.updateUser(nil)

	if c.database != nil {
		if err := c.database.ClearAll(); err != nil {
			logger.Error("credentials", "database clear failed", "error", err)
			clearErrs = append(clearErrs, fmt.Errorf("failed to clear database: %w", err))
		}
	}

	if len(clearErrs) > 0 {
		c.reportBackgroundError(errors.Join(clearErrs...))
		return
	}

	logger.Success("credentials", "credentials cleared")
}
