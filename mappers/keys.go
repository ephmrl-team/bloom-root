package mappers

import (
	"encoding/base64"
	"fmt"

	"github.com/ephmrl-team/bloom-root/domain"
	"github.com/slipe-fun/skid-v4/pkg/identity"
	"github.com/slipe-fun/skid-v4/pkg/messages"
)

func MapPublicKeys(mlKem768, x448, ed448 []byte) *domain.PublicKeys {
	return &domain.PublicKeys{
		MlKem768: base64.StdEncoding.EncodeToString(mlKem768),
		X448:     base64.StdEncoding.EncodeToString(x448),
		Ed448:    base64.StdEncoding.EncodeToString(ed448),
	}
}

func MapSecretKeys(secretKeys *identity.SecretKeys) (*[]byte, error) {
	if secretKeys == nil {
		return nil, domain.ErrNilSecretKeys
	}

	packSecretKeys, err := secretKeys.Pack()
	if err != nil {
		return nil, err
	}

	return &packSecretKeys, nil
}

func UnmapSecretKeys(mappedSecretKeys []byte) (*identity.SecretKeys, error) {
	return identity.Unpack(mappedSecretKeys)
}

func UnmapPublicKeys(publicKeys *domain.PublicKeys) (*identity.PublicKeys, error) {
	if publicKeys == nil {
		return nil, domain.ErrNilPublicKeys
	}

	mlKem768Bytes, err := base64.StdEncoding.DecodeString(publicKeys.MlKem768)
	if err != nil {
		return nil, err
	}

	x448Bytes, err := base64.StdEncoding.DecodeString(publicKeys.X448)
	if err != nil {
		return nil, err
	}

	ed448Bytes, err := base64.StdEncoding.DecodeString(publicKeys.Ed448)
	if err != nil {
		return nil, err
	}

	return &identity.PublicKeys{
		MlKem768: mlKem768Bytes,
		X448:     x448Bytes,
		Ed448:    ed448Bytes,
	}, nil
}

func VerifyIdentity(userID string, publicKeys *identity.PublicKeys) error {
	if userID == "" {
		return fmt.Errorf("%w: user ID is empty", domain.ErrIncompleteUser)
	}

	if publicKeys == nil {
		return domain.ErrNilPublicKeys
	}

	if len(publicKeys.MlKem768) == 0 || len(publicKeys.X448) == 0 || len(publicKeys.Ed448) == 0 {
		return fmt.Errorf("%w: user %s has empty public keys", domain.ErrIncompleteUser, userID)
	}

	derivedID := identity.GenerateUserID(publicKeys.Ed448, publicKeys.X448, publicKeys.MlKem768)
	if derivedID != userID {
		return fmt.Errorf("%w: got %s, expected %s", domain.ErrIdentityMismatch, userID, derivedID)
	}

	return nil
}

func ConvertPublicKeysToUser(userID string, publicKeys *domain.PublicKeys) (*identity.User, error) {
	if userID == "" {
		return nil, fmt.Errorf("%w: user ID is empty", domain.ErrIncompleteUser)
	}

	decodedKeys, err := UnmapPublicKeys(publicKeys)
	if err != nil {
		return nil, fmt.Errorf("failed to decode public keys for user %s: %w", userID, err)
	}

	if err := VerifyIdentity(userID, decodedKeys); err != nil {
		return nil, err
	}

	return &identity.User{
		ID:         userID,
		PublicKeys: *decodedKeys,
	}, nil
}

func ConvertUserToIdentity(user *domain.User) (*identity.User, error) {
	if user == nil {
		return nil, domain.ErrNilUser
	}

	return ConvertPublicKeysToUser(user.ID, &domain.PublicKeys{
		MlKem768: user.MlKemPublicKey,
		X448:     user.EcdhPublicKey,
		Ed448:    user.EdPublicKey,
	})
}

func ConvertCredentialsToUser(creds *domain.SavedCredentials) (*identity.User, error) {
	if creds == nil {
		return nil, domain.ErrNotAuthenticated
	}

	user, err := ConvertPublicKeysToUser(creds.UserID, &creds.PublicKeys)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", domain.ErrInvalidUserIdentity, err)
	}

	return user, nil
}

func MapEncryptedGroupKey(encryptedGroupKey *messages.EncryptedMessage) *domain.EncryptedGroupKey {
	return &domain.EncryptedGroupKey{
		Version:    base64.StdEncoding.EncodeToString(encryptedGroupKey.Version),
		Ciphertext: base64.StdEncoding.EncodeToString(encryptedGroupKey.Ciphertext),
		Nonce:      base64.StdEncoding.EncodeToString(encryptedGroupKey.Nonce),
		Salt:       base64.StdEncoding.EncodeToString(encryptedGroupKey.Salt),
	}
}
