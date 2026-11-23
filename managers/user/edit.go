package user

import (
	"context"

	"github.com/ephmrl-team/bloom-root/api/user"
	"github.com/ephmrl-team/bloom-root/domain"
)

func (u *UserManager) Edit(ctx context.Context, username, displayName, description *string) (*domain.EditUserResponse, error) {
	editUserRequest := user.NewEditUserRequest(username, displayName, description)

	return u.userClient.Edit(ctx, editUserRequest)
}
