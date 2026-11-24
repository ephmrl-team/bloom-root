package client

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/ephmrl-team/bloom-root/crypto"
	"github.com/ephmrl-team/bloom-root/domain"
	"github.com/ephmrl-team/bloom-root/logger"
	"github.com/ephmrl-team/bloom-root/mappers"
	"github.com/slipe-fun/skid-v4/pkg/identity"
)

type RegisterResult struct {
	UserJSON       []byte `json:"user_json"`
	RawRecoveryKey string `json:"raw_recovery_key"`
}

type LoginResult struct {
	UserJSON []byte `json:"user_json"`
}

func (c *BloomClient) Register(token *CancelToken) (*RegisterResult, error) {
	logger.Info("auth", "registration started")

	userIdentity, secret, rawRecoveryKey, recoveryKey, masterKey, lookupID, err := identity.GenerateIdentity()
	if err != nil {
		logger.Error("auth", "identity generation failed", "error", err)
		return nil, err
	}
	defer secret.Wipe()

	registerResponse, err := c.authManager.Register(token.context(), userIdentity, secret, masterKey, recoveryKey, lookupID)
	if err != nil {
		logger.Error("auth", "registration request failed", "error", err)
		return nil, err
	}

	if err := mappers.VerifyIdentity(userIdentity.ID, &userIdentity.PublicKeys); err != nil {
		logger.Error("auth", "identity verification failed", "user_id", userIdentity.ID, "error", err)
		return nil, err
	}
	if registerResponse.User.ID != userIdentity.ID {
		err := fmt.Errorf("%w: register response user ID mismatch", domain.ErrIdentityMismatch)
		logger.Error("auth", "registered user mismatch", "expected_user_id", userIdentity.ID, "actual_user_id", registerResponse.User.ID, "error", err)
		return nil, err
	}
	if _, err := mappers.ConvertUserToIdentity(&registerResponse.User); err != nil {
		logger.Error("auth", "registered user verification failed", "user_id", registerResponse.User.ID, "error", err)
		return nil, fmt.Errorf("failed to verify registered user: %w", err)
	}

	mappedSecretKeys, err := mappers.MapSecretKeys(secret)
	if err != nil {
		logger.Error("auth", "secret keys mapping failed", "user_id", registerResponse.User.ID, "error", err)
		return nil, err
	}

	userBytes, err := json.Marshal(registerResponse.User)
	if err != nil {
		logger.Error("auth", "user marshal failed", "user_id", registerResponse.User.ID, "error", err)
		return nil, err
	}

	rawRecoveryKeyHex := hex.EncodeToString(rawRecoveryKey)

	c.saveCredentials(&domain.SavedCredentials{
		UserID:         registerResponse.User.ID,
		RawRecoveryKey: rawRecoveryKey,
		MasterKey:      masterKey,
		PublicKeys: *mappers.MapPublicKeys(
			userIdentity.PublicKeys.MlKem768,
			userIdentity.PublicKeys.X448,
			userIdentity.PublicKeys.Ed448,
		),
		SecretKeys: *mappedSecretKeys,
		UserJSON:   userBytes,
		Token:      registerResponse.Token,
	})

	if err := c.database.SaveUser(&registerResponse.User); err != nil {
		logger.Error("auth", "user save failed", "user_id", registerResponse.User.ID, "error", err)
	}

	c.apiClient.SetToken(registerResponse.Token)
	c.startWebSocket(context.Background(), c.wsURL)

	c.updateUser(userBytes)

	logger.Success("auth", "registration completed", "user_id", registerResponse.User.ID)

	return &RegisterResult{
		UserJSON:       userBytes,
		RawRecoveryKey: rawRecoveryKeyHex,
	}, nil
}

func (c *BloomClient) Login(token *CancelToken, rawRecoveryKey string) (*LoginResult, error) {
	logger.Info("auth", "login started")

	rawRecoveryKeyBytes, err := hex.DecodeString(rawRecoveryKey)
	if err != nil {
		logger.Error("auth", "recovery key decode failed", "error", err)
		return nil, err
	}

	recoveryKeyBytes, lookupID, err := identity.NewRecoveryKey(rawRecoveryKeyBytes)
	if err != nil {
		logger.Error("auth", "recovery key parse failed", "error", err)
		return nil, err
	}
	defer crypto.Zero(recoveryKeyBytes)

	beginLoginResponse, err := c.authManager.BeginLogin(token.context(), lookupID)
	if err != nil {
		logger.Error("auth", "login begin failed", "error", err)
		return nil, err
	}

	userIdentity, err := mappers.ConvertPublicKeysToUser(beginLoginResponse.UserID, &domain.PublicKeys{
		MlKem768: beginLoginResponse.Keys.IdentityKeys.IdentityPublicKeys.MlKemPublicKey,
		X448:     beginLoginResponse.Keys.IdentityKeys.IdentityPublicKeys.EcdhPublicKey,
		Ed448:    beginLoginResponse.Keys.IdentityKeys.IdentityPublicKeys.EdPublicKey,
	})
	if err != nil {
		logger.Error("auth", "login identity conversion failed", "user_id", beginLoginResponse.UserID, "error", err)
		return nil, err
	}

	finishLoginResult, masterKey, secretKeys, err := c.authManager.FinishLogin(token.context(), beginLoginResponse, userIdentity, recoveryKeyBytes)
	if err != nil {
		logger.Error("auth", "login finish failed", "user_id", beginLoginResponse.UserID, "error", err)
		return nil, err
	}
	defer secretKeys.Wipe()

	if finishLoginResult.User.ID != userIdentity.ID {
		err := fmt.Errorf("%w: login response user ID mismatch", domain.ErrIdentityMismatch)
		logger.Error("auth", "logged in user mismatch", "expected_user_id", userIdentity.ID, "actual_user_id", finishLoginResult.User.ID, "error", err)
		return nil, err
	}
	if _, err := mappers.ConvertUserToIdentity(&finishLoginResult.User); err != nil {
		logger.Error("auth", "logged in user verification failed", "user_id", finishLoginResult.User.ID, "error", err)
		return nil, fmt.Errorf("failed to verify logged in user: %w", err)
	}

	userBytes, err := json.Marshal(finishLoginResult.User)
	if err != nil {
		logger.Error("auth", "user marshal failed", "user_id", finishLoginResult.User.ID, "error", err)
		return nil, err
	}

	mappedSecretKeys, err := mappers.MapSecretKeys(secretKeys)
	if err != nil {
		logger.Error("auth", "secret keys mapping failed", "user_id", finishLoginResult.User.ID, "error", err)
		return nil, err
	}

	c.saveCredentials(&domain.SavedCredentials{
		UserID:         finishLoginResult.User.ID,
		RawRecoveryKey: rawRecoveryKeyBytes,
		MasterKey:      masterKey,
		PublicKeys: *mappers.MapPublicKeys(
			userIdentity.PublicKeys.MlKem768,
			userIdentity.PublicKeys.X448,
			userIdentity.PublicKeys.Ed448,
		),
		SecretKeys: *mappedSecretKeys,
		UserJSON:   userBytes,
		Token:      finishLoginResult.Token,
	})

	if err := c.database.SaveUser(&finishLoginResult.User); err != nil {
		logger.Error("auth", "user save failed", "user_id", finishLoginResult.User.ID, "error", err)
	}

	c.apiClient.SetToken(finishLoginResult.Token)
	c.startWebSocket(context.Background(), c.wsURL)

	c.updateUser(userBytes)

	logger.Success("auth", "login completed", "user_id", finishLoginResult.User.ID)

	return &LoginResult{
		UserJSON: userBytes,
	}, nil
}

func (c *BloomClient) RestoreSession() (*LoginResult, error) {
	logger.Info("auth", "session restore started")

	creds, err := c.loadCredentials()
	if err != nil {
		logger.Error("auth", "session restore failed", "error", err)
		return nil, err
	}

	c.apiClient.SetToken(creds.Token)

	c.updateUser(creds.UserJSON)

	logger.Success("auth", "session restored", "user_id", creds.UserID)

	return &LoginResult{
		UserJSON: creds.UserJSON,
	}, nil
}
