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

    "github.com/HamedShams/agile-pulse/internal/config"
    applog "github.com/HamedShams/agile-pulse/internal/logger"
    "github.com/HamedShams/agile-pulse/internal/repo"
    apphttp "github.com/HamedShams/agile-pulse/internal/http"
    "github.com/HamedShams/agile-pulse/internal/jobs"
    "github.com/HamedShams/agile-pulse/internal/adapters/jira"
    "github.com/HamedShams/agile-pulse/internal/adapters/openai"
    "github.com/HamedShams/agile-pulse/internal/adapters/telegram"
    "github.com/HamedShams/agile-pulse/internal/services"
)

func main() {
    cfg := config.Load()
    log := applog.New(cfg)

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // DB
    db := repo.MustOpen(ctx, cfg, log)
    defer db.Close()

    // Adapters
    jc := jira.NewClient(cfg, log)
    llm := openai.NewClient(cfg, log)
    tg  := telegram.NewClient(cfg, log)

    // Services
    repository := repo.NewRepository(db, log)
    svc := services.New(cfg, log, repository, jc, llm, tg)

    // Resolve boards by names on startup (cache)
    if len(cfg.JiraBoardNames) > 0 {
        ctx2, cancel2 := context.WithTimeout(ctx, 20*time.Second); defer cancel2()
        ids, err := jc.ResolveBoardsByNames(ctx2, cfg.JiraBoardNames)
        if err != nil {
            log.Error().Err(err).Strs("names", cfg.JiraBoardNames).Msg("jira board resolve failed; falling back to JQL-only mode")
        } else {
            log.Info().Ints64("board_ids", ids).Msg("jira boards resolved")
        }
    }

    // HTTP server (Gin)
    router := apphttp.NewRouter(cfg, log, svc)

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
    cron := jobs.NewCron(cfg, log, svc, repository)
    cron.Start()
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

