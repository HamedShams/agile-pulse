package logger

import (
    "os"
    "time"

    "github.com/example/agile-pulse/internal/config"
    "github.com/rs/zerolog"
    "github.com/rs/zerolog/log"
)

func New(cfg config.Config) zerolog.Logger {
    if cfg.AppEnv == "dev" {
        output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
        logger := zerolog.New(output).With().Timestamp().Logger()
        log.Logger = logger
        return logger
    }
    zerolog.TimeFieldFormat = time.RFC3339
    logger := zerolog.New(os.Stdout).With().Timestamp().Logger()
    log.Logger = logger
    return logger
}

