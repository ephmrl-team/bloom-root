package auth

import (
	"encoding/base64"
	"encoding/json"

	"github.com/cloudflare/circl/sign/ed448"
	"github.com/ephmrl-team/bloom-root/domain"
	"github.com/slipe-fun/skid-v4/pkg/identity"
)

func NewKeysRequest(
	user *identity.User,
	authLookupID string,
	encryptedSecretKeys *identity.EncryptedSecretKeys,
	encryptedMasterKey *identity.EncryptedMasterKey,
) *domain.KeysRequest {
	return &domain.KeysRequest{
		AuthLookupID: authLookupID,
		IdentityKeys: domain.IdentityKeysRequest{
			EncryptedSecretKeys: domain.EncryptedKey{
				Version:    base64.StdEncoding.EncodeToString(encryptedSecretKeys.Version),
				Ciphertext: base64.StdEncoding.EncodeToString(encryptedSecretKeys.Ciphertext),
				Nonce:      base64.StdEncoding.EncodeToString(encryptedSecretKeys.Nonce),
				Salt:       base64.StdEncoding.EncodeToString(encryptedSecretKeys.Salt),
				Signature:  base64.StdEncoding.EncodeToString(encryptedSecretKeys.Signature),
			},
			IdentityPublicKeys: domain.IdentityPublicKeys{
				MlKemPublicKey: base64.StdEncoding.EncodeToString(user.PublicKeys.MlKem768),
				EcdhPublicKey:  base64.StdEncoding.EncodeToString(user.PublicKeys.X448),
				EdPublicKey:    base64.StdEncoding.EncodeToString(user.PublicKeys.Ed448),
			},
		},
		EncryptedMasterKey: domain.EncryptedKey{
			Version:    base64.StdEncoding.EncodeToString(encryptedMasterKey.Version),
			Ciphertext: base64.StdEncoding.EncodeToString(encryptedMasterKey.Ciphertext),
			Nonce:      base64.StdEncoding.EncodeToString(encryptedMasterKey.Nonce),
			Salt:       base64.StdEncoding.EncodeToString(encryptedMasterKey.Salt),
			Signature:  base64.StdEncoding.EncodeToString(encryptedMasterKey.Signature),
		},
	}
}

func DecodeBeginLoginResponse(response *domain.BeginLoginResponse, user *identity.User) (*identity.EncryptedMasterKey, *identity.EncryptedSecretKeys, *domain.LoginChallenge, error) {
	masterKeyVersion, err := base64.StdEncoding.DecodeString(response.Keys.EncryptedMasterKey.Version)
	if err != nil {
		return nil, nil, nil, err
	}

	masterKeyCiphertext, err := base64.StdEncoding.DecodeString(response.Keys.EncryptedMasterKey.Ciphertext)
	if err != nil {
		return nil, nil, nil, err
	}

	masterKeyNonce, err := base64.StdEncoding.DecodeString(response.Keys.EncryptedMasterKey.Nonce)
	if err != nil {
		return nil, nil, nil, err
	}

	masterKeySalt, err := base64.StdEncoding.DecodeString(response.Keys.EncryptedMasterKey.Salt)
	if err != nil {
		return nil, nil, nil, err
	}

	masterKeySignature, err := base64.StdEncoding.DecodeString(response.Keys.EncryptedMasterKey.Signature)
	if err != nil {
		return nil, nil, nil, err
	}

	secretKeysVersion, err := base64.StdEncoding.DecodeString(response.Keys.IdentityKeys.EncryptedSecretKeys.Version)
	if err != nil {
		return nil, nil, nil, err
	}

	secretKeysCiphertext, err := base64.StdEncoding.DecodeString(response.Keys.IdentityKeys.EncryptedSecretKeys.Ciphertext)
	if err != nil {
		return nil, nil, nil, err
	}

	secretKeysNonce, err := base64.StdEncoding.DecodeString(response.Keys.IdentityKeys.EncryptedSecretKeys.Nonce)
	if err != nil {
		return nil, nil, nil, err
	}

	secretKeysSalt, err := base64.StdEncoding.DecodeString(response.Keys.IdentityKeys.EncryptedSecretKeys.Salt)
	if err != nil {
		return nil, nil, nil, err
	}

	secretKeysSignature, err := base64.StdEncoding.DecodeString(response.Keys.IdentityKeys.EncryptedSecretKeys.Signature)
	if err != nil {
		return nil, nil, nil, err
	}

	encryptedMasterKeyBytes := identity.EncryptedMasterKey{
		Version:    masterKeyVersion,
		Ciphertext: masterKeyCiphertext,
		Nonce:      masterKeyNonce,
		Salt:       masterKeySalt,
		Signature:  masterKeySignature,
	}

	encryptedSecretKeysBytes := identity.EncryptedSecretKeys{
		Version:    secretKeysVersion,
		Ciphertext: secretKeysCiphertext,
		Nonce:      secretKeysNonce,
		Salt:       secretKeysSalt,
		Signature:  secretKeysSignature,
	}

	challenge := domain.LoginChallenge{
		Challenge: response.Challenge,
		UserID:    user.ID,
	}

	return &encryptedMasterKeyBytes, &encryptedSecretKeysBytes, &challenge, nil
}

func NewFinishLoginRequest(user *identity.User, secretKeys *identity.SecretKeys, challenge *domain.LoginChallenge) (*domain.FinishLoginRequest, error) {
	message, err := json.Marshal(challenge)
	if err != nil {
		return nil, err
	}

	signature := ed448.Sign(secretKeys.Ed448, message, "")

	return &domain.FinishLoginRequest{
		UserID:    user.ID,
		Signature: base64.StdEncoding.EncodeToString(signature),
	}, nil
}
