/* Copyright (c) 2025 Hamed Shams <https://hamedshams.com>
 * SPDX-License-Identifier: BSD-3-Clause */
package main

import (
    "context"
    "os"
    "os/signal"
    "strings"
    "syscall"
    "time"
)

func main() {
    cfg := Load()
    log := newLogger(cfg)
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // DB
    db := MustOpen(ctx, cfg, log)
    defer db.Close()

    // Adapters
    jc := NewJiraClient(cfg, log)
    llm := NewOpenAIClient(cfg, log)
    tg  := NewTelegramClient(cfg, log)

    // Services
    repository := NewRepository(db, log)
    svc := NewService(cfg, log, repository, jc, llm, tg)

    // Resolve boards by names on startup using contains+project preference (aligned with TestJiraFetch)
    if len(cfg.JiraBoardNames) > 0 {
        ctx2, cancel2 := context.WithTimeout(ctx, 20*time.Second); defer cancel2()
        preferredProject := ""
        if len(cfg.JiraProjects) > 0 { preferredProject = strings.TrimSpace(cfg.JiraProjects[0]) }
        ids, err := svc.ResolveBoardsStartup(ctx2, preferredProject)
        if err != nil {
            log.Error().Err(err).Strs("names", cfg.JiraBoardNames).Msg("jira board resolve failed; falling back to JQL-only mode")
        } else {
            log.Info().Ints64("board_ids", ids).Msg("jira boards resolved")
        }
    }

    // HTTP server (Gin)
    router := newRouter(cfg, log, svc)

    // Register Telegram webhook only if PUBLIC_BASE_URL is HTTPS
    if cfg.TelegramWebhookSecret != "" && strings.HasPrefix(strings.ToLower(cfg.PublicBaseURL), "https://") {
        go func(){
            ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second); defer cancel()
            base := strings.TrimRight(cfg.PublicBaseURL, "/")
            webhookURL := base + "/telegram/webhook/" + cfg.TelegramWebhookSecret
            if err := tg.SetWebhook(ctx, webhookURL, cfg.TelegramWebhookSecret); err != nil {
                log.Error().Err(err).Str("url", webhookURL).Msg("telegram setWebhook failed")
            } else {
                log.Info().Str("url", webhookURL).Msg("telegram setWebhook ok")
            }
        }()
    }

    // Cron
    cron := startCron(cfg, log, svc, repository)
    defer cron.Stop()

    // graceful shutdown
    errCh := make(chan error, 1)
    go func() { errCh <- router.Run(cfg.HTTPAddr) }()

    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

    select {
    case <-sigCh:
        log.Info().Msg("shutting down...")
    case err := <-errCh:
        if err != nil { log.Error().Err(err).Msg("http server error") }
    }

    time.Sleep(500 * time.Millisecond)
}

