package user

import (
	"context"
	"fmt"

	"github.com/ephmrl-team/bloom-root/api"
	"github.com/ephmrl-team/bloom-root/domain"
)

func (u *UserClient) Get(ctx context.Context, userID string) (*domain.User, error) {
	return api.Send[struct{}, domain.User](
		ctx,
		u.client,
		"GET",
		fmt.Sprintf("/user/%s", userID),
		nil,
	)
}
