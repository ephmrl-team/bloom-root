package user

import (
	"context"

	"github.com/ephmrl-team/bloom-root/domain"
)

func (u *UserManager) GetMe(ctx context.Context) (*domain.User, error) {
	return u.userClient.GetMe(ctx)
}
