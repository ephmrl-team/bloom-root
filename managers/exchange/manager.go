package exchange

import (
	"github.com/ephmrl-team/bloom-root/api/exchange"
)

type ExchangeManager struct {
	exchangeClient *exchange.ExchangeClient
}

func NewExchangeManager(exchangeClient *exchange.ExchangeClient) *ExchangeManager {
	return &ExchangeManager{
		exchangeClient: exchangeClient,
	}
}
