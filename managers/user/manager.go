package user

import "github.com/ephmrl-team/bloom-root/api/user"

type UserManager struct {
	userClient *user.UserClient
}

func NewUserManager(userClient *user.UserClient) *UserManager {
	return &UserManager{
		userClient: userClient,
	}
}
