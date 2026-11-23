package user

import (
	"context"

	"github.com/ephmrl-team/bloom-root/api"
	"github.com/ephmrl-team/bloom-root/domain"
)

func (u *UserClient) GetMe(ctx context.Context) (*domain.User, error) {
	return api.Send[struct{}, domain.User](ctx, u.client, "GET", "/user/me", nil)
}
