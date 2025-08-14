package openai

import (
    "bytes"
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "net/http"
    "strings"

    "github.com/example/agile-pulse/internal/config"
    "github.com/rs/zerolog"
)

type Client struct {
    key   string
    model string
    http  *http.Client
    log   zerolog.Logger
}

func NewClient(cfg config.Config, log zerolog.Logger) *Client {
    return &Client{ key: cfg.OpenAIKey, model: cfg.OpenAIModel, http: &http.Client{ Timeout: cfg.OpenAITimeout }, log: log }
}

func (c *Client) ExtractIssue(ctx context.Context, payload any) (map[string]any, error) {
    if strings.TrimSpace(c.key) == "" { return nil, errors.New("openai: missing key") }
    body := map[string]any{
        "model": c.model,
        "messages": []map[string]string{
            {"role":"system","content":"You are an agile analyst. Extract blockers, QA churn, root causes, risks, and notes from this issue payload. Return concise JSON with keys: blockers[], qa_churn, suspected_causes[], risks[], notes[]."},
            {"role":"user","content": fmt.Sprintf("%v", payload)},
        },
        "temperature": 0.1,
    }
    b, _ := json.Marshal(body)
    req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.openai.com/v1/chat/completions", bytes.NewReader(b))
    req.Header.Set("Authorization", "Bearer "+c.key)
    req.Header.Set("Content-Type", "application/json")
    resp, err := c.http.Do(req)
    if err != nil { return nil, err }
    defer resp.Body.Close()
    if resp.StatusCode >= 300 { return nil, fmt.Errorf("openai status=%d", resp.StatusCode) }
    var out struct{ Choices []struct{ Message struct{ Content string `json:"content"` } `json:"message"` } `json:"choices"` }
    if err := json.NewDecoder(resp.Body).Decode(&out); err != nil { return nil, err }
    if len(out.Choices) == 0 { return nil, errors.New("openai: no choices") }
    var m map[string]any
    if err := json.Unmarshal([]byte(out.Choices[0].Message.Content), &m); err != nil { return nil, err }
    return m, nil
}
func (c *Client) Summarize(ctx context.Context, kpis map[string]float64, findings []map[string]any) (string, error) {
    if strings.TrimSpace(c.key) == "" { return "", errors.New("openai: missing key") }
    payload := map[string]any{"kpis": kpis, "findings": findings}
    body := map[string]any{
        "model": c.model,
        "messages": []map[string]string{
            {"role":"system","content":"You are a senior agile coach. Given KPIs and extracted findings, produce a concise, actionable weekly summary with anomalies and suggested actions."},
            {"role":"user","content": fmt.Sprintf("%v", payload)},
        },
        "temperature": 0.2,
    }
    b, _ := json.Marshal(body)
    req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.openai.com/v1/chat/completions", bytes.NewReader(b))
    req.Header.Set("Authorization", "Bearer "+c.key)
    req.Header.Set("Content-Type", "application/json")
    resp, err := c.http.Do(req)
    if err != nil { return "", err }
    defer resp.Body.Close()
    if resp.StatusCode >= 300 { return "", fmt.Errorf("openai status=%d", resp.StatusCode) }
    var out struct{ Choices []struct{ Message struct{ Content string `json:"content"` } `json:"message"` } `json:"choices"` }
    if err := json.NewDecoder(resp.Body).Decode(&out); err != nil { return "", err }
    if len(out.Choices) == 0 { return "", errors.New("openai: no choices") }
    return out.Choices[0].Message.Content, nil
}

