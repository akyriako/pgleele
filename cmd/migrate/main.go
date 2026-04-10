package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/akyriako/pgleele/migrations"
	"github.com/caarlos0/env/v11"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type config struct {
	LogLevel    int    `env:"LOG_LEVEL" envDefault:"0"`
	PostgresDSN string `env:"POSTGRES_DSN,required"`
}

const (
	exitCodeConfigurationError int = 78
)

var (
	cfg config
)

func init() {
	err := env.Parse(&cfg)
	if err != nil {
		slog.Error(fmt.Sprintf("parsing env variables failed: %s", err.Error()))
		os.Exit(exitCodeConfigurationError)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.Level(cfg.LogLevel),
	}))

	slog.SetDefault(logger)
}

func main() {
	db, err := sql.Open("pgx", cfg.PostgresDSN)
	if err != nil {
		slog.Error(fmt.Sprintf("opening database failed: %s", err.Error()))
		os.Exit(1)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		slog.Error(fmt.Sprintf("ping database failed: %s", err.Error()))
		os.Exit(1)
	}

	entries, err := migrations.Files.ReadDir(".")
	if err != nil {
		slog.Error(fmt.Sprintf("reading embedded migrations failed: %s", err.Error()))
		os.Exit(1)
	}

	for _, e := range entries {
		slog.Info("embedded file", "name", e.Name())
	}

	src, err := iofs.New(migrations.Files, ".")
	if err != nil {
		slog.Error(fmt.Sprintf("opening migrations failed: %s", err.Error()))
		os.Exit(1)
	}

	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	m, err := migrate.NewWithInstance("iofs", src, "postgres", driver)
	if err != nil {
		slog.Error(fmt.Sprintf("creating new migration instance failed: %s", err.Error()))
		os.Exit(1)
	}

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		slog.Error(fmt.Sprintf("running migrations failed: %s", err.Error()))
		os.Exit(1)
	}

	version, dirty, err := m.Version()
	if err != nil {
		slog.Error(fmt.Sprintf("getting migration version failed: %s", err.Error()))
		os.Exit(1)
	}

	slog.Info("migration version", "version", version, "dirty", dirty)
}
