/* Copyright (c) 2025 Hamed Shams <https://hamedshams.com>
 * SPDX-License-Identifier: BSD-3-Clause */
package config

import (
    "encoding/json"
    "log"
    "os"
    "strconv"
    "strings"
    "time"
)

type Config struct {
    AppEnv   string
    TZ       string
    HTTPAddr string

    DBDSN string

    PublicBaseURL string

    JiraBaseURL    string
    JiraPAT        string
    JiraUsername   string
    JiraPassword   string
    JiraProjects   []string
    JiraDefaultJQL string
    JiraAPIVersion string
    JiraBoardNames []string
    JiraExpediteJQL string
    JiraFieldsFile  string
    JiraFieldMap    map[string]string // name -> id

    OpenAIKey     string
    OpenAIModel   string
    OpenAITimeout time.Duration

    TelegramToken         string
    TelegramWebhookSecret string
    TelegramChatIDs       []int64
    TelegramChatUsernames []string

    DigestCron     string
    MaxConcurrency int
    HTTPTimeout    time.Duration

    WorkersJira    int
    WorkersLLM     int
    LLMBudgetTokens int
    LLMMaxIssues    int
}

func getenv(key, def string) string {
    v := os.Getenv(key)
    if v == "" { return def }
    return v
}

func atoi(key string, def int) int {
    v := os.Getenv(key)
    if v == "" { return def }
    i, err := strconv.Atoi(v)
    if err != nil { return def }
    return i
}

func dur(key string, def time.Duration) time.Duration {
    v := os.Getenv(key)
    if v == "" { return def }
    d, err := time.ParseDuration(v)
    if err != nil { return def }
    return d
}

func parseInt64s(csv string) []int64 {
    if csv == "" { return nil }
    parts := strings.Split(csv, ",")
    out := make([]int64, 0, len(parts))
    for _, p := range parts {
        p = strings.TrimSpace(p)
        if p == "" { continue }
        n, err := strconv.ParseInt(p, 10, 64)
        if err == nil { out = append(out, n) }
    }
    return out
}

func parseStrings(csv string) []string {
    if csv == "" { return nil }
    parts := strings.Split(csv, ",")
    out := make([]string, 0, len(parts))
    for _, p := range parts {
        p = strings.TrimSpace(p)
        if p == "" { continue }
        out = append(out, p)
    }
    return out
}

func Load() Config {
    cfg := Config{
        AppEnv:   getenv("APP_ENV", "dev"),
        TZ:       getenv("APP_TZ", "Asia/Tehran"),
        HTTPAddr: getenv("HTTP_ADDR", ":8080"),

        DBDSN: getenv("DB_DSN", "postgres://postgres:postgres@localhost:5432/agilepulse?sslmode=disable"),

        PublicBaseURL: getenv("PUBLIC_BASE_URL", "http://localhost:8080"),

        JiraBaseURL:    getenv("JIRA_BASE_URL", ""),
        JiraPAT:        getenv("JIRA_PAT", ""),
        JiraUsername:   getenv("JIRA_USERNAME", ""),
        JiraPassword:   getenv("JIRA_PASSWORD", ""),
        JiraProjects:   strings.Split(strings.TrimSpace(getenv("JIRA_PROJECTS", "SNAPPDR")), ","),
        JiraDefaultJQL: getenv("JIRA_DEFAULT_JQL", "updated >= -7d"),
        JiraAPIVersion: getenv("JIRA_API_VERSION", "2"),
        JiraBoardNames: parseStrings(getenv("JIRA_BOARD_NAMES", "")),
        JiraExpediteJQL: getenv("JIRA_EXPEDITE_JQL", ""),
        JiraFieldsFile:  getenv("JIRA_FIELDS_FILE", "/config/jira_fields.json"),

        OpenAIKey:     getenv("OPENAI_API_KEY", ""),
        OpenAIModel:   getenv("OPENAI_MODEL", "o3-mini"),
        OpenAITimeout: dur("OPENAI_TIMEOUT", 15*time.Second),

        TelegramToken:         getenv("TELEGRAM_BOT_TOKEN", ""),
        TelegramWebhookSecret: getenv("TELEGRAM_WEBHOOK_SECRET", "change-me"),
        TelegramChatIDs:       parseInt64s(getenv("TELEGRAM_CHAT_IDS", "")),
        TelegramChatUsernames: parseStrings(getenv("TELEGRAM_CHAT_USERNAMES", "")),

        DigestCron:     getenv("CRON_SPEC", "0 10 * * FRI"),
        MaxConcurrency: atoi("MAX_CONCURRENCY", 8),
        HTTPTimeout:    dur("HTTP_TIMEOUT", 15*time.Second),
        WorkersJira:    atoi("WORKERS_JIRA", 6),
        WorkersLLM:     atoi("WORKERS_LLM", 3),
        LLMBudgetTokens: atoi("LLM_TOKEN_BUDGET", 300000),
        LLMMaxIssues:    atoi("LLM_MAX_ISSUES", 60),
    }

    // Fallback: if TELEGRAM_CHAT_IDS provided but non-numeric, treat as usernames
    if len(cfg.TelegramChatIDs) == 0 {
        raw := strings.TrimSpace(getenv("TELEGRAM_CHAT_IDS", ""))
        if raw != "" {
            // contains any letter or '@' → assume usernames
            for _, r := range raw {
                if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '@' || r == '_' {
                    cfg.TelegramChatUsernames = parseStrings(raw)
                    break
                }
            }
        }
    }

    // set global timezone if available
    if loc, err := time.LoadLocation(cfg.TZ); err == nil {
        time.Local = loc
    } else {
        log.Printf("warning: cannot load TZ %s: %v", cfg.TZ, err)
    }

    // Optional: load Jira custom fields mapping from file (name->id)
    if data, err := os.ReadFile(cfg.JiraFieldsFile); err == nil {
        type fieldDef struct { ID string `json:"id"`; Name string `json:"name"` }
        var arr []fieldDef
        if err := json.Unmarshal(data, &arr); err == nil {
            m := map[string]string{}
            for _, f := range arr {
                n := strings.TrimSpace(f.Name)
                if n != "" && f.ID != "" { m[n] = f.ID }
            }
            if len(m) > 0 { cfg.JiraFieldMap = m }
        }
    } else {
        // try relative path fallback
        if data2, err2 := os.ReadFile("config/jira_fields.json"); err2 == nil {
            type fieldDef struct { ID string `json:"id"`; Name string `json:"name"` }
            var arr []fieldDef
            if err := json.Unmarshal(data2, &arr); err == nil {
                m := map[string]string{}
                for _, f := range arr { n := strings.TrimSpace(f.Name); if n != "" && f.ID != "" { m[n] = f.ID } }
                if len(m) > 0 { cfg.JiraFieldMap = m }
            }
        }
    }
    return cfg
}

