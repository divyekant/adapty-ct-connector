package clevertap

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// AuthError is returned when CleverTap responds with 401 or 403.
type AuthError struct {
	StatusCode int
}

func (e *AuthError) Error() string {
	return fmt.Sprintf("clevertap: authentication failed (HTTP %d)", e.StatusCode)
}

// Client is an HTTP client for the CleverTap upload API.
type Client struct {
	accountID      string
	passcode       string
	baseURL        string
	httpClient     *http.Client
	MaxRetries     int
	InitialBackoff time.Duration
}

// NewClient creates a Client targeting baseURL directly (useful for tests or custom endpoints).
func NewClient(accountID, passcode, baseURL string) *Client {
	return &Client{
		accountID:      accountID,
		passcode:       passcode,
		baseURL:        baseURL,
		httpClient:     &http.Client{Timeout: 30 * time.Second},
		MaxRetries:     3,
		InitialBackoff: 1 * time.Second,
	}
}

// NewClientFromRegion creates a Client whose endpoint is https://{region}.api.clevertap.com/1/upload.
func NewClientFromRegion(accountID, passcode, region string) *Client {
	url := fmt.Sprintf("https://%s.api.clevertap.com/1/upload", region)
	return NewClient(accountID, passcode, url)
}

// Upload sends an UploadRequest to CleverTap.
// It retries on 429 and 5xx responses using exponential backoff.
// 401/403 returns *AuthError immediately. 400 returns a plain error immediately.
func (c *Client) Upload(req UploadRequest) (*UploadResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("clevertap: marshal request: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt <= c.MaxRetries; attempt++ {
		if attempt > 0 {
			backoff := c.InitialBackoff * (1 << (attempt - 1))
			time.Sleep(backoff)
		}

		result, done, err := c.attempt(body)
		if done {
			return result, err
		}
		lastErr = err
	}

	return nil, fmt.Errorf("clevertap: max retries exceeded: %w", lastErr)
}

// attempt performs a single HTTP request. It returns (result, true, err) when the
// caller should stop retrying — either because it succeeded or hit a non-retryable
// error. It returns (nil, false, err) to signal the caller should retry.
func (c *Client) attempt(body []byte) (*UploadResponse, bool, error) {
	httpReq, err := http.NewRequest(http.MethodPost, c.baseURL, bytes.NewReader(body))
	if err != nil {
		return nil, true, fmt.Errorf("clevertap: build request: %w", err)
	}
	httpReq.Header.Set("X-CleverTap-Account-Id", c.accountID)
	httpReq.Header.Set("X-CleverTap-Passcode", c.passcode)
	httpReq.Header.Set("Content-Type", "application/json; charset=utf-8")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		// Transport error — treat as retryable.
		return nil, false, fmt.Errorf("clevertap: do request: %w", err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusOK:
		var result UploadResponse
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return nil, true, fmt.Errorf("clevertap: decode response: %w", err)
		}
		return &result, true, nil

	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return nil, true, &AuthError{StatusCode: resp.StatusCode}

	case resp.StatusCode == http.StatusBadRequest:
		return nil, true, fmt.Errorf("clevertap: bad request (HTTP 400)")

	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return nil, false, fmt.Errorf("clevertap: server error (HTTP %d)", resp.StatusCode)

	default:
		return nil, true, fmt.Errorf("clevertap: unexpected status %d", resp.StatusCode)
	}
}
