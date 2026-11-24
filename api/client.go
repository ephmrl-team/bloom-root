package api

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ephmrl-team/bloom-root/logger"
	"resty.dev/v3"
)

const (
	defaultDialTimeout           = 10 * time.Second
	defaultTLSHandshakeTimeout   = 10 * time.Second
	defaultResponseHeaderTimeout = 10 * time.Second
	defaultClientTimeout         = 30 * time.Second
	defaultRequestTimeout        = 30 * time.Second
	defaultRetryCount            = 3
	defaultRetryWaitTime         = 200 * time.Millisecond
	defaultRetryMaxWaitTime      = 5 * time.Second
)

type ApiError struct {
	Code    string `json:"error"`
	Message string `json:"message"`
}

func (e *ApiError) Error() string {
	return fmt.Sprintf("api error [%s]: %s", e.Code, e.Message)
}

type Client struct {
	httpClient *resty.Client
	baseURL    string
	token      string
	tokenMu    sync.RWMutex
}

func NewClient(baseURL string) *Client {
	restyClient := resty.NewWithTransportSettings(&resty.TransportSettings{
		DialerTimeout:         defaultDialTimeout,
		TLSHandshakeTimeout:   defaultTLSHandshakeTimeout,
		ResponseHeaderTimeout: defaultResponseHeaderTimeout,
	})

	restyClient.SetBaseURL(baseURL)
	restyClient.SetTimeout(defaultClientTimeout)
	restyClient.SetRetryWaitTime(defaultRetryWaitTime)
	restyClient.SetRetryMaxWaitTime(defaultRetryMaxWaitTime)
	restyClient.AddRetryConditions(
		resty.RetryConditionStatusTooManyRequests,
		resty.RetryConditionStatus5XX,
	)

	client := &Client{
		httpClient: restyClient,
		baseURL:    baseURL,
	}

	restyClient.AddRequestMiddleware(func(cli *resty.Client, req *resty.Request) error {
		token := client.getToken()
		if token != "" {
			req.SetAuthToken(token)
		}
		return nil
	})

	return client
}

func (c *Client) SetDialContext(dial func(ctx context.Context, network, address string) (net.Conn, error)) {
	transport, ok := c.httpClient.Transport().(*http.Transport)
	if !ok {
		return
	}

	cloned := transport.Clone()
	cloned.DialContext = dial
	c.httpClient.SetTransport(cloned)
}

func (c *Client) SetToken(token string) {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	c.token = token
}

func (c *Client) getToken() string {
	c.tokenMu.RLock()
	defer c.tokenMu.RUnlock()
	return c.token
}

func requestPath(path string) string {
	if index := strings.IndexByte(path, '?'); index >= 0 {
		return path[:index]
	}
	return path
}

func Send[Req any, Resp any](ctx context.Context, c *Client, method, path string, body *Req) (*Resp, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultRequestTimeout)
		defer cancel()
	}

	startedAt := time.Now()
	req := c.httpClient.R().SetContext(ctx)

	if method == http.MethodGet {
		req.SetRetryCount(defaultRetryCount)
	}

	if body != nil {
		req.SetBody(body)
	}

	var respData Resp
	var apiError ApiError

	resp, err := req.
		SetResult(&respData).
		SetResultError(&apiError).
		Execute(method, path)

	if err != nil {
		logger.Error("api", "request failed", "method", method, "path", requestPath(path), "duration", time.Since(startedAt), "error", err)
		return nil, fmt.Errorf("network error: %w", err)
	}

	if resp.IsStatusFailure() {
		if apiError.Code != "" {
			logger.Warn("api", "request failed", "method", method, "path", requestPath(path), "status", resp.StatusCode(), "duration", time.Since(startedAt), "code", apiError.Code, "message", apiError.Message)
			return nil, &apiError
		}
		logger.Warn("api", "request failed", "method", method, "path", requestPath(path), "status", resp.StatusCode(), "duration", time.Since(startedAt))
		return nil, fmt.Errorf("http error %d: %s", resp.StatusCode(), resp.String())
	}

	logger.Info("api", "request completed", "method", method, "path", requestPath(path), "status", resp.StatusCode(), "duration", time.Since(startedAt))

	return &respData, nil
}
