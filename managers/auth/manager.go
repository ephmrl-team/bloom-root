package auth

import "github.com/ephmrl-team/bloom-root/api/auth"

type AuthManager struct {
	authClient *auth.AuthClient
}

func NewAuthManager(authClient *auth.AuthClient) *AuthManager {
	return &AuthManager{
		authClient: authClient,
	}
}
