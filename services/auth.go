package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

func (c *JacadClient) GetAuthToken(ctx context.Context) (string, error) {
	c.muAuth.Lock()
	defer c.muAuth.Unlock()

	if c.token != "" && time.Now().Before(c.tokenExpiry) {
		return c.token, nil
	}

	log.Println("Token expired or not available. Authenticating with Jacad...")
	
	authURL := c.Config.APIBase + c.Config.Endpoints["AUTH"]
	authHeaders := map[string]string{
		"token": c.Config.UserToken,
	}

	authBody, err := c.MakeRequest(ctx, http.MethodPost, authURL, authHeaders, nil)
	if err != nil {
		if ctx.Err() != nil {
			return "", fmt.Errorf("failed to get new auth token due to context cancellation: %w", ctx.Err())
		}
		return "", fmt.Errorf("failed to get new auth token: %w", err)
	}

	var authResp struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(authBody, &authResp); err != nil {
		return "", fmt.Errorf("failed to parse auth token response: %w", err)
	}
	if authResp.Token == "" {
		return "", fmt.Errorf("auth token response was empty")
	}

	c.token = authResp.Token
	c.tokenExpiry = time.Now().Add(1 * time.Hour)
	log.Println("New token obtained successfully.")
	return c.token, nil
}
