package user

import (
	"context"

	"github.com/ephmrl-team/bloom-root/api"
	"github.com/ephmrl-team/bloom-root/domain"
)

func (u *UserClient) Edit(ctx context.Context, editUserRequest *domain.EditUserRequest) (*domain.EditUserResponse, error) {
	return api.Send[domain.EditUserRequest, domain.EditUserResponse](ctx, u.client, "POST", "/user/edit", editUserRequest)
}
