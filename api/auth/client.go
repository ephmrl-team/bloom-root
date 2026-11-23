package auth

import (
	"github.com/ephmrl-team/bloom-root/api"
)

type AuthClient struct {
	client *api.Client
}

func NewAuthClient(client *api.Client) *AuthClient {
	return &AuthClient{
		client: client,
	}
}
