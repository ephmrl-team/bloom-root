package client

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/ephmrl-team/bloom-root/crypto"
	"github.com/ephmrl-team/bloom-root/domain"
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
	userIdentity, secret, rawRecoveryKey, recoveryKey, masterKey, lookupID, err := identity.GenerateIdentity()
	if err != nil {
		return nil, err
	}
	defer secret.Wipe()

	registerResponse, err := c.authManager.Register(token.context(), userIdentity, secret, masterKey, recoveryKey, lookupID)
	if err != nil {
		return nil, err
	}

	if err := mappers.VerifyIdentity(userIdentity.ID, &userIdentity.PublicKeys); err != nil {
		return nil, err
	}
	if registerResponse.User.ID != userIdentity.ID {
		return nil, fmt.Errorf("%w: register response user ID mismatch", domain.ErrIdentityMismatch)
	}
	if _, err := mappers.ConvertUserToIdentity(&registerResponse.User); err != nil {
		return nil, fmt.Errorf("failed to verify registered user: %w", err)
	}

	mappedSecretKeys, err := mappers.MapSecretKeys(secret)
	if err != nil {
		return nil, err
	}

	userBytes, err := json.Marshal(registerResponse.User)
	if err != nil {
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

	c.database.SaveUser(&registerResponse.User)

	c.apiClient.SetToken(registerResponse.Token)
	c.startWebSocket(context.Background(), c.wsURL)

	c.updateUser(userBytes)

	return &RegisterResult{
		UserJSON:       userBytes,
		RawRecoveryKey: rawRecoveryKeyHex,
	}, nil
}

func (c *BloomClient) Login(token *CancelToken, rawRecoveryKey string) (*LoginResult, error) {
	rawRecoveryKeyBytes, err := hex.DecodeString(rawRecoveryKey)
	if err != nil {
		return nil, err
	}

	recoveryKeyBytes, lookupID, err := identity.NewRecoveryKey(rawRecoveryKeyBytes)
	if err != nil {
		return nil, err
	}
	defer crypto.Zero(recoveryKeyBytes)

	beginLoginResponse, err := c.authManager.BeginLogin(token.context(), lookupID)
	if err != nil {
		return nil, err
	}

	userIdentity, err := mappers.ConvertPublicKeysToUser(beginLoginResponse.UserID, &domain.PublicKeys{
		MlKem768: beginLoginResponse.Keys.IdentityKeys.IdentityPublicKeys.MlKemPublicKey,
		X448:     beginLoginResponse.Keys.IdentityKeys.IdentityPublicKeys.EcdhPublicKey,
		Ed448:    beginLoginResponse.Keys.IdentityKeys.IdentityPublicKeys.EdPublicKey,
	})
	if err != nil {
		return nil, err
	}

	finishLoginResult, masterKey, secretKeys, err := c.authManager.FinishLogin(token.context(), beginLoginResponse, userIdentity, recoveryKeyBytes)
	if err != nil {
		return nil, err
	}
	defer secretKeys.Wipe()

	if finishLoginResult.User.ID != userIdentity.ID {
		return nil, fmt.Errorf("%w: login response user ID mismatch", domain.ErrIdentityMismatch)
	}
	if _, err := mappers.ConvertUserToIdentity(&finishLoginResult.User); err != nil {
		return nil, fmt.Errorf("failed to verify logged in user: %w", err)
	}

	userBytes, err := json.Marshal(finishLoginResult.User)
	if err != nil {
		return nil, err
	}

	mappedSecretKeys, err := mappers.MapSecretKeys(secretKeys)
	if err != nil {
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

	c.database.SaveUser(&finishLoginResult.User)

	c.apiClient.SetToken(finishLoginResult.Token)
	c.startWebSocket(context.Background(), c.wsURL)

	c.updateUser(userBytes)

	return &LoginResult{
		UserJSON: userBytes,
	}, nil
}

func (c *BloomClient) RestoreSession() (*LoginResult, error) {
	creds, err := c.loadCredentials()
	if err != nil {
		return nil, err
	}

	c.apiClient.SetToken(creds.Token)

	c.updateUser(creds.UserJSON)

	return &LoginResult{
		UserJSON: creds.UserJSON,
	}, nil
}
