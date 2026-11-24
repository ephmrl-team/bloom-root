package domain

import "errors"

var (
	ErrNotAuthenticated      = errors.New("unauthorized: client is not logged in")
	ErrChatRecipientNotFound = errors.New("chat recipient not found")
	ErrUnsupportedChatType   = errors.New("unsupported chat type")
	ErrInvalidChat           = errors.New("invalid chat")
	ErrInvalidUserIdentity   = errors.New("invalid user identity")
	ErrIdentityMismatch      = errors.New("user ID does not match public keys")
	ErrHandshakeMissing      = errors.New("handshake is missing")
	ErrHandshakeIncomplete   = errors.New("handshake is incomplete")
	ErrNilUser               = errors.New("user is nil")
	ErrIncompleteUser        = errors.New("user is incomplete")
	ErrNilSecretKeys         = errors.New("secret keys are nil")
	ErrNilPublicKeys         = errors.New("public keys are nil")
	ErrExchangeTimeout       = errors.New("exchange session timed out")
)
