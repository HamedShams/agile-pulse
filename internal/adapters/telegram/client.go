/* Copyright (c) 2025 Hamed Shams <https://hamedshams.com>
 * SPDX-License-Identifier: BSD-3-Clause */
package telegram

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "time"

    "github.com/example/agile-pulse/internal/config"
    "github.com/rs/zerolog"
)

type Client struct {
    token string
    http  *http.Client
    log   zerolog.Logger
}

func NewClient(cfg config.Config, log zerolog.Logger) *Client {
    return &Client{ token: cfg.TelegramToken, http: &http.Client{ Timeout: 10 * time.Second }, log: log }
}

func (c *Client) SendMessage(ctx context.Context, chatID int64, text string) error {
    if c.token == "" || chatID == 0 { return fmt.Errorf("telegram: missing token or chat id") }
    url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", c.token)
    body := map[string]any{"chat_id": chatID, "text": text, "parse_mode": "Markdown", "disable_web_page_preview": true}
    b, _ := json.Marshal(body)
    req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(b))
    req.Header.Set("Content-Type", "application/json")
    resp, err := c.http.Do(req)
    if err != nil { return err }
    defer resp.Body.Close()
    if resp.StatusCode >= 300 {
        var bodyBytes []byte
        bodyBytes, _ = io.ReadAll(resp.Body)
        return fmt.Errorf("telegram sendMessage status=%d body=%s", resp.StatusCode, string(bodyBytes))
    }
    return nil
}

// SendMessagePlain sends without parse_mode to avoid markdown parsing errors
func (c *Client) SendMessagePlain(ctx context.Context, chatID int64, text string) error {
    if c.token == "" || chatID == 0 { return fmt.Errorf("telegram: missing token or chat id") }
    url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", c.token)
    body := map[string]any{"chat_id": chatID, "text": text, "disable_web_page_preview": true}
    b, _ := json.Marshal(body)
    req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(b))
    req.Header.Set("Content-Type", "application/json")
    resp, err := c.http.Do(req)
    if err != nil { return err }
    defer resp.Body.Close()
    if resp.StatusCode >= 300 {
        var bodyBytes []byte
        bodyBytes, _ = io.ReadAll(resp.Body)
        return fmt.Errorf("telegram sendMessage status=%d body=%s", resp.StatusCode, string(bodyBytes))
    }
    return nil
}

// SendMarkdownV2 sends a message using MarkdownV2 parse mode.
func (c *Client) SendMarkdownV2(ctx context.Context, chatID int64, text string) error {
    if c.token == "" || chatID == 0 { return fmt.Errorf("telegram: missing token or chat id") }
    url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", c.token)
    body := map[string]any{"chat_id": chatID, "text": text, "parse_mode": "MarkdownV2", "disable_web_page_preview": true}
    b, _ := json.Marshal(body)
    req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(b))
    req.Header.Set("Content-Type", "application/json")
    resp, err := c.http.Do(req)
    if err != nil { return err }
    defer resp.Body.Close()
    if resp.StatusCode >= 300 {
        var bodyBytes []byte
        bodyBytes, _ = io.ReadAll(resp.Body)
        return fmt.Errorf("telegram sendMessage status=%d body=%s", resp.StatusCode, string(bodyBytes))
    }
    return nil
}
func (c *Client) ResolveUsername(ctx context.Context, username string) (int64, error) {
    if c.token == "" || username == "" { return 0, fmt.Errorf("telegram: missing token or username") }
    url := fmt.Sprintf("https://api.telegram.org/bot%s/getChat", c.token)
    body := map[string]any{"chat_id": username}
    b, _ := json.Marshal(body)
    req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(b))
    req.Header.Set("Content-Type", "application/json")
    resp, err := c.http.Do(req)
    if err != nil { return 0, err }
    defer resp.Body.Close()
    if resp.StatusCode >= 300 { return 0, fmt.Errorf("telegram getChat status=%d", resp.StatusCode) }
    var r struct{ OK bool `json:"ok"`; Result struct{ ID int64 `json:"id"` } `json:"result"` }
    if err := json.NewDecoder(resp.Body).Decode(&r); err != nil { return 0, err }
    if !r.OK || r.Result.ID == 0 { return 0, fmt.Errorf("telegram: invalid getChat response") }
    return r.Result.ID, nil
}

// SetWebhook registers the webhook URL and secret with Telegram
func (c *Client) SetWebhook(ctx context.Context, webhookURL string, secretToken string) error {
	if c.token == "" || webhookURL == "" || secretToken == "" { return fmt.Errorf("telegram: missing token, url or secret") }
	url := fmt.Sprintf("https://api.telegram.org/bot%s/setWebhook", c.token)
	body := map[string]any{
		"url": webhookURL,
		"secret_token": secretToken,
		"drop_pending_updates": true,
		"allowed_updates": []string{"message", "callback_query"},
	}
	b, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil { return err }
	defer resp.Body.Close()
	if resp.StatusCode >= 300 { return fmt.Errorf("telegram setWebhook status=%d", resp.StatusCode) }
	return nil
}

