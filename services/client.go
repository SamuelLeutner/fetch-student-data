package services

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/SamuelLeutner/fetch-student-data/config"
	"github.com/SamuelLeutner/fetch-student-data/models"
)

type SheetWriter interface {
	EnsureSheetExists(ctx context.Context, sheetName string) error
	Clear(ctx context.Context, sheetName string) error
	SetHeaders(ctx context.Context, sheetName string, headers []string) error
	AppendRows(ctx context.Context, sheetName string, rows [][]interface{}) error
}

type JacadClient struct {
	Config      *config.Config
	Client      *http.Client
	Writer      SheetWriter
	token       string
	tokenExpiry time.Time
	muAuth      sync.Mutex
}

func NewJacadClient(config *config.Config, writer SheetWriter) *JacadClient {
	return &JacadClient{
		Config: config,
		Client: &http.Client{Timeout: 60 * time.Second},
		Writer: writer,
	}
}

func (c *JacadClient) MakeRequest(ctx context.Context, method, url string, headers map[string]string, body io.Reader) ([]byte, error) {
	var lastErr error

	for attempt := 0; attempt <= c.Config.MaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			log.Printf("Request '%s %s' cancelled via context before attempt %d: %v", method, strings.Split(url, "?")[0], attempt+1, ctx.Err())
			return nil, fmt.Errorf("request '%s %s' cancelled via context: %w", method, strings.Split(url, "?")[0], ctx.Err())
		default:
		}

		req, err := http.NewRequestWithContext(ctx, method, url, body)
		if err != nil {
			return nil, fmt.Errorf("error creating request on attempt %d: %w", attempt+1, err)
		}

		if headers != nil {
			for key, value := range headers {
				req.Header.Set(key, value)
			}
		}

		log.Printf("Request (%s): %s (Attempt %d/%d)...", method, strings.Split(url, "?")[0], attempt+1, c.Config.MaxRetries+1)

		resp, err := c.Client.Do(req)

		if err != nil {
			lastErr = fmt.Errorf("http client error on attempt %d: %w", attempt+1, err)
		} else if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			bodyBytes, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr == nil {
				lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
			} else {
				lastErr = fmt.Errorf("HTTP %d: Error reading body: %w", resp.StatusCode, readErr)
			}
		} else if resp.StatusCode == http.StatusUnauthorized {
			bodyBytes, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				return nil, fmt.Errorf("HTTP %d: error reading error response body: %w", resp.StatusCode, readErr)
			}
			return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
		} else if resp.StatusCode >= 400 {
			bodyBytes, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				return nil, fmt.Errorf("HTTP %d: error reading error response body: %w", resp.StatusCode, readErr)
			}
			log.Printf("HTTP %d error: %s", resp.StatusCode, string(bodyBytes))
			return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
		} else {
			defer resp.Body.Close()
			bodyBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				return nil, fmt.Errorf("error reading response body on success: %w", err)
			}
			return bodyBytes, nil
		}

		if attempt < c.Config.MaxRetries {
			delay := c.Config.RetryDelay * time.Duration(1<<attempt)
			log.Printf("Request failed (attempt %d/%d): %v. Waiting %s before retrying...", attempt+1, c.Config.MaxRetries+1, lastErr, delay)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				log.Printf("Context cancelled during retry wait for %s: %v", url, ctx.Err())
				return nil, fmt.Errorf("request cancelled during retry wait after %d attempts for %s: %w", attempt+1, url, ctx.Err())
			}
		} else {
			break
		}
	}
	return nil, fmt.Errorf("request failed after %d attempts: %w", c.Config.MaxRetries+1, lastErr)
}

func (c *JacadClient) FetchPage(ctx context.Context, endpoint string, page, pageSize int, params map[string]string) ([]models.Enrollment, *models.Page, error) {
	q := url.Values{}
	q.Set("currentPage", fmt.Sprintf("%d", page))
	q.Set("pageSize", fmt.Sprintf("%d", pageSize))
	for k, v := range params {
		q.Set(k, v)
	}

	url := fmt.Sprintf("%s%s?%s", c.Config.APIBase, endpoint, q.Encode())

	token, err := c.GetAuthToken(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, fmt.Errorf("failed to get token for page %d due to context cancellation: %w", page, ctx.Err())
		}
		return nil, nil, fmt.Errorf("failed to get token for page %d: %w", page, err)
	}

	headers := map[string]string{
		"Authorization": "Bearer " + token,
		"Content-Type":  "application/json",
	}

	body, err := c.MakeRequest(ctx, http.MethodGet, url, headers, nil)
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, fmt.Errorf("fetching page %d cancelled via context: %w", page, ctx.Err())
		}
		return nil, nil, fmt.Errorf("error fetching page %d from %s: %w", page, endpoint, err)
	}

	var apiResp models.APIResponse[models.Enrollment]
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, nil, fmt.Errorf("error parsing API response from page %d: %w", page, err)
	}

	return apiResp.Elements, apiResp.Page, nil
}
