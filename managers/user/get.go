package user

import (
	"context"

	"github.com/ephmrl-team/bloom-root/domain"
)

func (u *UserManager) Get(ctx context.Context, userID string) (*domain.User, error) {
	return u.userClient.Get(ctx, userID)
}
