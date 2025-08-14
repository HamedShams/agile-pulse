/* Copyright (c) 2025 Hamed Shams <https://hamedshams.com>
 * SPDX-License-Identifier: BSD-3-Clause */
package http

import (
    "net/http"
    "context"

    "github.com/gin-gonic/gin"
    "github.com/example/agile-pulse/internal/config"
    "github.com/rs/zerolog"
)

type service interface {
    RunWeeklyDigest(ctx context.Context) error
    RunOnDemandDigest(ctx context.Context, chatID int64, sinceDays int) error
    SendHelp(ctx context.Context, chatID int64) error
    TestJiraFetch(ctx context.Context) error
    GetLastRun(ctx context.Context) (any, error)
}

type Handlers struct {
    cfg config.Config
    log zerolog.Logger
    svc service
}

func NewHandlers(cfg config.Config, log zerolog.Logger, svc any) *Handlers {
    return &Handlers{cfg: cfg, log: log, svc: svc.(service)}
}

func (h *Handlers) Healthz(c *gin.Context) {
    c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (h *Handlers) LastRun(c *gin.Context) {
    lr, err := h.svc.GetLastRun(c.Request.Context())
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }
    c.JSON(http.StatusOK, lr)
}

func (h *Handlers) RunNow(c *gin.Context) {
    // Run in background detached from the HTTP request to avoid context cancellation
    go func(){ _ = h.svc.RunWeeklyDigest(context.Background()) }()
    c.JSON(http.StatusAccepted, gin.H{"status": "queued"})
}

func (h *Handlers) TelegramWebhook(c *gin.Context) {
    headerSecret := c.GetHeader("X-Telegram-Bot-Api-Secret-Token")
    pathSecret := c.Param("secret")
    // Accept either header secret (preferred) or path secret
    if headerSecret != h.cfg.TelegramWebhookSecret && pathSecret != h.cfg.TelegramWebhookSecret {
        c.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
        return
    }
    h.log.Info().Str("ip", c.ClientIP()).Str("ua", c.GetHeader("User-Agent")).Msg("telegram webhook received")

    // Parse minimal update payload for commands
    var upd struct {
        Message *struct {
            Chat struct { ID int64 `json:"id"` } `json:"chat"`
            Text string `json:"text"`
        } `json:"message"`
    }
    if err := c.ShouldBindJSON(&upd); err == nil && upd.Message != nil {
        chatID := upd.Message.Chat.ID
        text := upd.Message.Text
        // accept only configured chats if provided
        allowed := len(h.cfg.TelegramChatIDs) == 0
        if !allowed {
            for _, id := range h.cfg.TelegramChatIDs { if id == chatID { allowed = true; break } }
        }
        if allowed {
            switch text {
            case "/report 7d":
                go func(){ _ = h.svc.RunOnDemandDigest(c.Request.Context(), chatID, 7) }()
            case "/report 30d":
                go func(){ _ = h.svc.RunOnDemandDigest(c.Request.Context(), chatID, 30) }()
            case "/start", "/help":
                go func(){ _ = h.svc.SendHelp(c.Request.Context(), chatID) }()
            }
        }
    }

    c.JSON(http.StatusOK, gin.H{"ok": true})
}

