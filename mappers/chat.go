package mappers

import (
	"encoding/base64"
	"fmt"

	"github.com/ephmrl-team/bloom-root/domain"
	"github.com/slipe-fun/skid-v4/pkg/identity"
)

func MapHandshake(handshake *identity.HandshakePayload) *domain.Handshake {
	return &domain.Handshake{
		Version:             base64.StdEncoding.EncodeToString(handshake.Version),
		ReceiverCipherText:  base64.StdEncoding.EncodeToString(handshake.ReceiverCiphertext),
		SenderEphemeralX448: base64.StdEncoding.EncodeToString(handshake.SenderEphemeralX448),
		SenderSignature:     base64.StdEncoding.EncodeToString(handshake.SenderSignature),
		EncryptedSyncKey: domain.EncryptedSyncKey{
			CipherText: base64.StdEncoding.EncodeToString(handshake.EncryptedSyncKey.Ciphertext),
			Nonce:      base64.StdEncoding.EncodeToString(handshake.EncryptedSyncKey.Nonce),
		},
	}
}

func DecodeHandshake(handshake *domain.Handshake) (*identity.HandshakePayload, error) {
	if handshake == nil {
		return nil, domain.ErrHandshakeMissing
	}

	if handshake.Version == "" ||
		handshake.ReceiverCipherText == "" ||
		handshake.SenderEphemeralX448 == "" ||
		handshake.SenderSignature == "" ||
		handshake.EncryptedSyncKey.CipherText == "" ||
		handshake.EncryptedSyncKey.Nonce == "" {
		return nil, fmt.Errorf("%w: one or more fields are empty", domain.ErrHandshakeIncomplete)
	}

	versionBytes, err := base64.StdEncoding.DecodeString(handshake.Version)
	if err != nil {
		return nil, err
	}

	receiverCiphertextBytes, err := base64.StdEncoding.DecodeString(handshake.ReceiverCipherText)
	if err != nil {
		return nil, err
	}

	senderEphemeralX448Bytes, err := base64.StdEncoding.DecodeString(handshake.SenderEphemeralX448)
	if err != nil {
		return nil, err
	}

	senderSignatureBytes, err := base64.StdEncoding.DecodeString(handshake.SenderSignature)
	if err != nil {
		return nil, err
	}

	encryptedSyncKeyCiphertextBytes, err := base64.StdEncoding.DecodeString(handshake.EncryptedSyncKey.CipherText)
	if err != nil {
		return nil, err
	}

	encryptedSyncKeyNonceBytes, err := base64.StdEncoding.DecodeString(handshake.EncryptedSyncKey.Nonce)
	if err != nil {
		return nil, err
	}

	return &identity.HandshakePayload{
		Version:             versionBytes,
		ReceiverCiphertext:  receiverCiphertextBytes,
		SenderEphemeralX448: senderEphemeralX448Bytes,
		SenderSignature:     senderSignatureBytes,
		EncryptedSyncKey: identity.EncryptedSyncKey{
			Ciphertext: encryptedSyncKeyCiphertextBytes,
			Nonce:      encryptedSyncKeyNonceBytes,
		},
	}, nil
}
