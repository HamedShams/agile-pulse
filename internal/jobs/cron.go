package jobs

import (
    "context"
    "time"

    "github.com/HamedShams/agile-pulse/internal/config"
    "github.com/HamedShams/agile-pulse/internal/repo"
    "github.com/robfig/cron/v3"
    "github.com/rs/zerolog"
)

type service interface { RunWeeklyDigest(ctx context.Context) error }

type Cron struct {
    cfg  config.Config
    log  zerolog.Logger
    svc  service
    repo *repo.Repository
    c    *cron.Cron
}

func NewCron(cfg config.Config, log zerolog.Logger, svc service, r *repo.Repository) *Cron {
    loc, _ := time.LoadLocation(cfg.TZ)
    c := cron.New(cron.WithLocation(loc), cron.WithParser(cron.NewParser(cron.Minute|cron.Hour|cron.Dom|cron.Month|cron.Dow)))
    cr := &Cron{cfg: cfg, log: log, svc: svc, repo: r, c: c}
    _, _ = c.AddFunc(cfg.DigestCron, cr.weekly)
    return cr
}

func (cr *Cron) Start(){ cr.c.Start() }
func (cr *Cron) Stop(){ cr.c.Stop() }

func (cr *Cron) weekly(){
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute); defer cancel()
    const lockKey int64 = 424242
    ok, err := cr.repo.TryAdvisoryLock(ctx, lockKey)
    if err != nil { cr.log.Error().Err(err).Msg("cron: lock error"); return }
    if !ok { cr.log.Info().Msg("cron: already running elsewhere"); return }
    defer func(){ _ = cr.repo.AdvisoryUnlock(context.Background(), lockKey) }()
    cr.log.Info().Msg("cron: weekly digest")
    if err := cr.svc.RunWeeklyDigest(ctx); err != nil { cr.log.Error().Err(err).Msg("cron: digest failed") }
}

