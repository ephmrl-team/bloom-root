package exchange

import (
	"context"

	"github.com/ephmrl-team/bloom-root/api"
	"github.com/ephmrl-team/bloom-root/domain"
)

func (c *ExchangeClient) StartSession(ctx context.Context) (*domain.StartExchangeSessionResponse, error) {
	return api.Send[struct{}, domain.StartExchangeSessionResponse](ctx, c.client, "POST", "/exchange/session", nil)
}
