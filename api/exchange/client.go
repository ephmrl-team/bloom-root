package exchange

import (
	"github.com/ephmrl-team/bloom-root/api"
)

type ExchangeClient struct {
	client *api.Client
}

func NewExchangeClient(client *api.Client) *ExchangeClient {
	return &ExchangeClient{
		client: client,
	}
}
