package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/akyriako/pgleele/pkg/pglock"
	"github.com/caarlos0/env/v11"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/klog/v2"
)

type config struct {
	PostgresDSN string `env:"POSTGRES_DSN,required"`

	ClusterName string `env:"K8S_CLUSTER_NAME,required"`
	Namespace   string `env:"K8S_NAMESPACE,required"`
	PodName     string `env:"K8S_POD_NAME"`

	ElectionNamespace string `env:"ELECTION_NAMESPACE" envDefault:"global"`
	ElectionName      string `env:"ELECTION_NAME" envDefault:"global-leader"`

	LeaseDuration time.Duration `env:"LEASE_DURATION" envDefault:"15s"`
	RenewDeadline time.Duration `env:"RENEW_DEADLINE" envDefault:"10s"`
	RetryPeriod   time.Duration `env:"RETRY_PERIOD" envDefault:"2s"`
}

const (
	exitCodeConfigurationError int = 78
)

var (
	cfg config
)

func init() {
	klog.InitFlags(nil)

	err := env.Parse(&cfg)
	if err != nil {
		klog.Error(fmt.Sprintf("parsing env variables failed: %s", err.Error()))
		os.Exit(exitCodeConfigurationError)
	}

	if cfg.PodName == "" {
		hostname, err := os.Hostname()
		if err != nil {
			klog.Error(fmt.Sprintf("getting hostname failed: %s", err.Error()))
			os.Exit(exitCodeConfigurationError)
		}
		cfg.PodName = fmt.Sprintf("%s-%d", hostname, os.Getpid())
	}

	if cfg.RenewDeadline >= cfg.LeaseDuration {
		klog.Error(fmt.Sprintf("renewing deadline exceeds lease duration (%v)", cfg.LeaseDuration))
		os.Exit(exitCodeConfigurationError)
	}

	if cfg.RetryPeriod > cfg.RenewDeadline {
		klog.Error(fmt.Sprintf("retrying period exceeds renewing deadline (%v)", cfg.RenewDeadline))
		os.Exit(exitCodeConfigurationError)
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	poolConfig, err := pgxpool.ParseConfig(cfg.PostgresDSN)
	if err != nil {
		klog.Error(fmt.Sprintf("parsing postgres dsn failed: %v", err))
		os.Exit(1)
	}

	poolConfig.ConnConfig.ConnectTimeout = 3 * time.Second
	poolConfig.MaxConns = 10
	poolConfig.MinConns = 1

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		klog.Error(fmt.Sprintf("creating postgres pool failed: %v", err))
		os.Exit(1)
	}
	defer pool.Close()

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		klog.Error(fmt.Sprintf("pinging postgres failed: %v", err))
		os.Exit(1)
	}

	identity := fmt.Sprintf("%s/%s/%s", cfg.ClusterName, cfg.Namespace, cfg.PodName)
	lock := pglock.New(pool, cfg.ElectionNamespace, cfg.ElectionName, identity)

	lockCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err = lock.CreateBootstrapElectionRecord(lockCtx, int(cfg.LeaseDuration.Seconds()))
	if err != nil {
		klog.Error(fmt.Sprintf("creating bootstrap lock failed: %v", err))
		os.Exit(1)
	}

	lec := leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   cfg.LeaseDuration,
		RenewDeadline:   cfg.RenewDeadline,
		RetryPeriod:     cfg.RetryPeriod,
		Name:            cfg.ElectionName,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				klog.Infof("elected as leader: identity='%s'", identity)
				leaderRun(ctx, identity)
			},
			OnStoppedLeading: func() {
				klog.Infof("stepping down...")
				//os.Exit(0)

				// Do what you have to do or call a delegate
			},
			OnNewLeader: func(id string) {
				if id == identity {
					return
				}
				klog.Infof("current leader: identity='%s'", id)
			},
		},
	}

	elector, err := leaderelection.NewLeaderElector(lec)
	if err != nil {
		klog.Error("creating new leader elector failed: %v", err)
		os.Exit(1)
	}

	elector.Run(ctx)
}

func leaderRun(ctx context.Context, identity string) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	klog.Infof("leader workload started: identity='%s'", identity)

	for {
		select {
		case <-ctx.Done():
			klog.Infof("leader workload stopping: identity='%s'", identity)
			return
		case <-ticker.C:
			// do what you have to do here
			klog.Infof("leader workload tick: identity='%s'", identity)
		}
	}
}
