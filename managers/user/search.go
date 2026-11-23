package user

import (
	"context"

	"github.com/ephmrl-team/bloom-root/domain"
)

func (u *UserManager) Search(ctx context.Context, query string) (*[]domain.User, error) {
	return u.userClient.Search(ctx, query)
}
