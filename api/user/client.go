package user

import (
	"github.com/ephmrl-team/bloom-root/api"
)

type UserClient struct {
	client *api.Client
}

func NewUserClient(client *api.Client) *UserClient {
	return &UserClient{
		client: client,
	}
}
