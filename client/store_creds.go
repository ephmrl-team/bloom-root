package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ephmrl-team/bloom-root/crypto"
	"github.com/ephmrl-team/bloom-root/domain"
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
		return errors.New("credentials are nil")
	}

	if _, err := mappers.ConvertCredentialsToUser(creds); err != nil {
		return err
	}

	plainText, err := json.Marshal(creds)
	if err != nil {
		return err
	}
	defer crypto.Zero(plainText)

	if len(c.encryptionKey) != 32 {
		return errors.New("invalid encryption key")
	}

	ciphertext, err := crypto.Encrypt(c.encryptionKey, plainText, nil)
	if err != nil {
		return err
	}

	filePath := filepath.Join(c.storagePath, "session.dat")
	if err := writeFileAtomic(filePath, ciphertext); err != nil {
		return err
	}

	crypto.Zero(creds.RawRecoveryKey)
	crypto.Zero(creds.MasterKey)
	crypto.Zero(creds.SecretKeys)

	publicCreds := copyPublicCredentials(creds)

	c.credentialsMu.Lock()
	c.credentials = publicCreds
	c.credentialsMu.Unlock()

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
		return nil, err
	}

	if len(c.encryptionKey) != 32 {
		return nil, errors.New("invalid encryption key")
	}

	plainText, err := crypto.Decrypt(c.encryptionKey, ciphertext, nil)
	if err != nil {
		return nil, err
	}
	defer crypto.Zero(plainText)

	var creds domain.SavedCredentials
	err = json.Unmarshal(plainText, &creds)
	if err != nil {
		return nil, err
	}

	if _, err := mappers.ConvertCredentialsToUser(&creds); err != nil {
		return nil, err
	}

	publicCreds := copyPublicCredentials(&creds)

	c.credentialsMu.Lock()
	c.credentials = publicCreds
	c.credentialsMu.Unlock()

	return &creds, nil
}

func (c *BloomClient) UpdateCredentialsUserJSON(userJSON []byte) error {
	c.sessionMu.Lock()
	defer c.sessionMu.Unlock()

	creds, err := c.loadCredentialsLocked()
	if err != nil {
		return err
	}

	creds.UserJSON = append([]byte(nil), userJSON...)

	return c.saveCredentialsLocked(creds)
}

func (c *BloomClient) ClearCredentials() {
	c.CancelExchange()
	c.stopWebSocket()
	c.StopChatsSync()

	var clearErrs []error

	c.sessionMu.Lock()
	filePath := filepath.Join(c.storagePath, "session.dat")
	if err := os.Remove(filePath); err != nil && !errors.Is(err, os.ErrNotExist) {
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
			clearErrs = append(clearErrs, fmt.Errorf("failed to clear database: %w", err))
		}
	}

	if len(clearErrs) > 0 {
		c.reportBackgroundError(errors.Join(clearErrs...))
	}
}
