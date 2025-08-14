/* Copyright (c) 2025 Hamed Shams <https://hamedshams.com>
 * SPDX-License-Identifier: BSD-3-Clause */
package jira

import (
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"

    "github.com/example/agile-pulse/internal/config"
    "github.com/rs/zerolog"
)

type Client struct {
    baseURL string
    token   string
    basic   string
    user    string
    pass    string
    http    *http.Client
    log     zerolog.Logger
    apiVer  string
}
// Issue fetches a single issue with full fields and optional changelog via expand
func (c *Client) Issue(ctx context.Context, key string, expandChangelog bool) (any, error) {
    if key == "" { return nil, errors.New("jira: empty issue key") }
    q := url.Values{}
    q.Set("fields", "*all")
    if expandChangelog {
        q.Set("expand", "changelog")
    }
    path := "/rest/api/3/issue/"+url.PathEscape(key)
    if c.apiVer == "2" { path = "/rest/api/2/issue/"+url.PathEscape(key) }
    u := c.apiURL(path, q)
    return c.doJSON(ctx, http.MethodGet, u, nil)
}

func NewClient(cfg config.Config, log zerolog.Logger) *Client {
    return &Client{
        baseURL: cfg.JiraBaseURL,
        token:   cfg.JiraPAT,
        basic:   getenvBasic(),
        user:    cfg.JiraUsername,
        pass:    cfg.JiraPassword,
        http:    &http.Client{ Timeout: cfg.HTTPTimeout },
        log:     log,
        apiVer:  cfg.JiraAPIVersion,
    }
}

// getenvBasic reads JIRA_BASIC_AUTH from environment if present (format: user:pass base64), optional
func getenvBasic() string {
    v := ""
    if s := strings.TrimSpace(os.Getenv("JIRA_BASIC_AUTH")); s != "" { v = s }
    return v
}

func (c *Client) apiURL(path string, q url.Values) string {
    base := strings.TrimRight(c.baseURL, "/")
    if !strings.HasPrefix(path, "/") { path = "/" + path }
    u := base + path
    if q != nil && len(q) > 0 { u = u + "?" + q.Encode() }
    return u
}

func (c *Client) doJSON(ctx context.Context, method, u string, body any) (map[string]any, error) {
    if c.baseURL == "" { return nil, errors.New("jira: empty baseURL") }
    var r io.Reader
    if body != nil {
        b, err := json.Marshal(body)
        if err != nil { return nil, err }
        r = strings.NewReader(string(b))
    }
    var lastErr error
    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequestWithContext(ctx, method, u, r)
        if err != nil { return nil, err }
        if body != nil { req.Header.Set("Content-Type", "application/json") }
        if c.token != "" {
            req.Header.Set("Authorization", "Bearer "+c.token)
        } else if c.user != "" && c.pass != "" {
            req.SetBasicAuth(c.user, c.pass)
        } else if c.basic != "" {
            req.Header.Set("Authorization", "Basic "+c.basic)
        }
        resp, err := c.http.Do(req)
        if err != nil { lastErr = err } else {
            defer resp.Body.Close()
            if resp.StatusCode >= 300 {
                b, _ := io.ReadAll(resp.Body)
                // retry on 429/5xx
                if resp.StatusCode == 429 || resp.StatusCode >= 500 {
                    lastErr = fmt.Errorf("jira api status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
                } else {
                    return nil, fmt.Errorf("jira api status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
                }
            } else {
                var out map[string]any
                if err := json.NewDecoder(resp.Body).Decode(&out); err != nil { return nil, err }
                return out, nil
            }
        }
        // backoff
        time.Sleep(time.Duration(300*(1<<attempt)) * time.Millisecond)
    }
    return nil, lastErr
}

func (c *Client) Search(ctx context.Context, jql string, startAt, max int) (any, error) {
    if jql == "" { return nil, errors.New("jira: empty jql") }
    if c.apiVer == "2" {
        q := url.Values{}
        q.Set("jql", jql)
        if startAt > 0 { q.Set("startAt", fmt.Sprint(startAt)) }
        if max > 0 { q.Set("maxResults", fmt.Sprint(max)) }
        q.Set("fields", "*all")
        u := c.apiURL("/rest/api/2/search", q)
        return c.doJSON(ctx, http.MethodGet, u, nil)
    }
    // default to v3
    body := map[string]any{"jql": jql, "startAt": startAt, "maxResults": max}
    u := c.apiURL("/rest/api/3/search", url.Values{"fields": []string{"*all"}})
    return c.doJSON(ctx, http.MethodPost, u, body)
}
func (c *Client) Comments(ctx context.Context, key string, startAt, max int) (any, error) {
    if key == "" { return nil, errors.New("jira: empty issue key") }
    q := url.Values{}
    if startAt > 0 { q.Set("startAt", fmt.Sprint(startAt)) }
    if max > 0 { q.Set("maxResults", fmt.Sprint(max)) }
    path := "/rest/api/3/issue/"+url.PathEscape(key)+"/comment"
    if c.apiVer == "2" { path = "/rest/api/2/issue/"+url.PathEscape(key)+"/comment" }
    u := c.apiURL(path, q)
    return c.doJSON(ctx, http.MethodGet, u, nil)
}
func (c *Client) Changelog(ctx context.Context, key string, startAt, max int) (any, error) {
    if key == "" { return nil, errors.New("jira: empty issue key") }
    q := url.Values{}
    if startAt > 0 { q.Set("startAt", fmt.Sprint(startAt)) }
    if max > 0 { q.Set("maxResults", fmt.Sprint(max)) }
    path := "/rest/api/3/issue/"+url.PathEscape(key)+"/changelog"
    if c.apiVer == "2" { path = "/rest/api/2/issue/"+url.PathEscape(key)+"/changelog" }
    u := c.apiURL(path, q)
    return c.doJSON(ctx, http.MethodGet, u, nil)
}
func (c *Client) Worklogs(ctx context.Context, key string, startAt, max int) (any, error) {
    if key == "" { return nil, errors.New("jira: empty issue key") }
    q := url.Values{}
    if startAt > 0 { q.Set("startAt", fmt.Sprint(startAt)) }
    if max > 0 { q.Set("maxResults", fmt.Sprint(max)) }
    path := "/rest/api/3/issue/"+url.PathEscape(key)+"/worklog"
    if c.apiVer == "2" { path = "/rest/api/2/issue/"+url.PathEscape(key)+"/worklog" }
    u := c.apiURL(path, q)
    return c.doJSON(ctx, http.MethodGet, u, nil)
}

// Boards lists Jira Software boards (Agile API)
func (c *Client) Boards(ctx context.Context, startAt, max int) (any, error) {
    q := url.Values{}
    if startAt > 0 { q.Set("startAt", fmt.Sprint(startAt)) }
    if max > 0 { q.Set("maxResults", fmt.Sprint(max)) }
    u := c.apiURL("/rest/agile/1.0/board", q)
    return c.doJSON(ctx, http.MethodGet, u, nil)
}

// BoardIssues lists issues on a board (Agile API). Optional JQL filter.
func (c *Client) BoardIssues(ctx context.Context, boardID int64, startAt, max int, jql string) (any, error) {
    if boardID <= 0 { return nil, errors.New("jira: invalid board id") }
    q := url.Values{}
    if startAt > 0 { q.Set("startAt", fmt.Sprint(startAt)) }
    if max > 0 { q.Set("maxResults", fmt.Sprint(max)) }
    if strings.TrimSpace(jql) != "" { q.Set("jql", jql) }
    q.Set("fields", "*all")
    path := "/rest/agile/1.0/board/" + strconv.FormatInt(boardID, 10) + "/issue"
    u := c.apiURL(path, q)
    return c.doJSON(ctx, http.MethodGet, u, nil)
}

// ResolveBoardsByNames returns board IDs matching provided names (case-sensitive as in Jira UI)
func (c *Client) ResolveBoardsByNames(ctx context.Context, names []string) ([]int64, error) {
    if len(names) == 0 { return nil, nil }
    wanted := map[string]struct{}{}
    for _, n := range names { n = strings.TrimSpace(n); if n != "" { wanted[n] = struct{}{} } }
    out := make([]int64, 0, len(wanted))
    start := 0
    for {
        page, err := c.Boards(ctx, start, 50)
        if err != nil { return nil, err }
        foundAny := false
        if m, ok := page.(map[string]any); ok {
            if vals, ok := m["values"].([]any); ok {
                for _, b0 := range vals {
                    if b, _ := b0.(map[string]any); b != nil {
                        name, _ := b["name"].(string)
                        if _, want := wanted[name]; want {
                            var id int64
                            switch vv := b["id"].(type) {
                            case float64: id = int64(vv)
                            case int64: id = vv
                            }
                            if id > 0 { out = append(out, id); delete(wanted, name) }
                        }
                    }
                }
                if len(vals) > 0 { foundAny = true }
            }
        }
        if len(wanted) == 0 || !foundAny { break }
        start += 50
    }
    return out, nil
}

// Fields lists all fields (for discovering Flagged customfield)
func (c *Client) Fields(ctx context.Context) (any, error) {
    u := c.apiURL("/rest/api/2/field", nil)
    // Note: this endpoint returns an array; adapt doJSON by manual request
    req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
    if err != nil { return nil, err }
    if c.token != "" { req.Header.Set("Authorization", "Bearer "+c.token) }
    if c.user != "" && c.pass != "" { req.SetBasicAuth(c.user, c.pass) }
    if c.basic != "" { req.Header.Set("Authorization", "Basic "+c.basic) }
    resp, err := c.http.Do(req)
    if err != nil { return nil, err }
    defer resp.Body.Close()
    if resp.StatusCode >= 300 {
        b, _ := io.ReadAll(resp.Body)
        return nil, fmt.Errorf("jira api status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(b)))
    }
    var out []map[string]any
    if err := json.NewDecoder(resp.Body).Decode(&out); err != nil { return nil, err }
    return out, nil
}

