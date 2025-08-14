/* Copyright (c) 2025 Hamed Shams <https://hamedshams.com>
 * SPDX-License-Identifier: BSD-3-Clause */
package http

import (
    "context"
    "github.com/gin-gonic/gin"
    "github.com/example/agile-pulse/internal/config"
    "github.com/rs/zerolog"
)

func NewRouter(cfg config.Config, log zerolog.Logger, svc any) *gin.Engine {
    if cfg.AppEnv != "dev" { gin.SetMode(gin.ReleaseMode) }
    r := gin.New()
    r.Use(gin.Recovery())
    r.Use(func(c *gin.Context){
        c.Next()
        log.Info().Str("m", c.Request.Method).Str("p", c.FullPath()).Int("s", c.Writer.Status()).Msg("http")
    })

    h := NewHandlers(cfg, log, svc)

    r.GET("/healthz", h.Healthz)
    r.GET("/admin/last-run", h.LastRun)
    r.POST("/admin/run", h.RunNow)
    r.POST("/admin/jira-test", func(c *gin.Context){ go func(){ _ = h.svc.TestJiraFetch(context.Background()) }(); c.JSON(202, gin.H{"status":"queued"}) })
    // Support both header-authenticated and path-secret webhook endpoints
    r.POST("/telegram/webhook", h.TelegramWebhook)
    r.POST("/telegram/webhook/:secret", h.TelegramWebhook)

    return r
}

