package user

import (
	"context"

	"github.com/ephmrl-team/bloom-root/domain"
)

func (u *UserManager) GetByIDs(ctx context.Context, ids []string) (*[]domain.User, error) {
	return u.userClient.GetByIDs(ctx, ids)
}
