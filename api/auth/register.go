package auth

import (
	"context"

	"github.com/ephmrl-team/bloom-root/api"
	"github.com/ephmrl-team/bloom-root/domain"
)

func (a *AuthClient) Register(ctx context.Context, keys *domain.KeysRequest) (*domain.RegisterResponse, error) {
	return api.Send[domain.KeysRequest, domain.RegisterResponse](ctx, a.client, "POST", "/auth/register", keys)
}
