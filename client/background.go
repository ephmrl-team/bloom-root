package client

import (
	"encoding/json"
	"fmt"

	"github.com/ephmrl-team/bloom-root/logger"
)

type BackgroundErrorListener interface {
	OnBackgroundError(errorJSON []byte)
}

func (c *BloomClient) RegisterBackgroundErrorListener(listener BackgroundErrorListener) {
	c.listenerMu.Lock()
	c.backgroundErrorListener = listener
	c.listenerMu.Unlock()
}

func (c *BloomClient) UnregisterBackgroundErrorListener() {
	c.listenerMu.Lock()
	c.backgroundErrorListener = nil
	c.listenerMu.Unlock()
}

func (c *BloomClient) reportBackgroundError(err error) {
	c.listenerMu.RLock()
	listener := c.backgroundErrorListener
	c.listenerMu.RUnlock()

	logger.Error("background", "background error", "error", err)

	if listener == nil {
		return
	}

	payload, marshalErr := json.Marshal(struct {
		Error string `json:"error"`
	}{Error: err.Error()})
	if marshalErr != nil {
		payload = []byte(err.Error())
	}

	listener.OnBackgroundError(payload)
}

func (c *BloomClient) recoverBackground(component string) {
	if r := recover(); r != nil {
		c.reportBackgroundError(fmt.Errorf("%s panic: %v", component, r))
	}
}
