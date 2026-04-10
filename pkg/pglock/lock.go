package pglock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	_ "k8s.io/client-go/tools/leaderelection/resourcelock"
)

var (
	ErrAlreadyExists   = errors.New("leader lock already exists")
	ErrVersionConflict = errors.New("leader lock update conflict")
	ErrNotFound        = errors.New("leader lock not found")
)

type Lock struct {
	pool *pgxpool.Pool

	namespace string
	name      string
	identity  string

	mu          sync.Mutex
	lastVersion int64
}

func New(pool *pgxpool.Pool, namespace string, name string, identity string) *Lock {
	return &Lock{
		pool:      pool,
		namespace: namespace,
		name:      name,
		identity:  identity,
	}
}

func (l *Lock) String() string {
	return fmt.Sprintf("postgres://leader-election/%s/%s", l.namespace, l.name)
}

func (l *Lock) Describe() string {
	return l.String()
}

func (l *Lock) Identity() string {
	return l.identity
}

func (l *Lock) RecordEvent(_ string) {
}

func (l *Lock) Get(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	const q = `
SELECT record_json, version
FROM leader_election_locks
WHERE namespace = $1 AND name = $2
`

	var raw []byte
	var version int64

	err := l.pool.QueryRow(ctx, q, l.namespace, l.name).Scan(&raw, &version)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, ErrNotFound
		}
		return nil, nil, fmt.Errorf("getting leader lock failed: %w", err)
	}

	var rec resourcelock.LeaderElectionRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, nil, fmt.Errorf("unmarshalling leader record failed: %w", err)
	}

	l.mu.Lock()
	l.lastVersion = version
	l.mu.Unlock()

	return &rec, raw, nil
}

func (l *Lock) Create(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	raw, err := json.Marshal(ler)
	if err != nil {
		return fmt.Errorf("marshalling leader record failed: %w", err)
	}

	const q = `
INSERT INTO leader_election_locks(namespace, name, record_json, version, updated_at)
VALUES ($1, $2, $3, 1, now())
`
	_, err = l.pool.Exec(ctx, q, l.namespace, l.name, raw)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return fmt.Errorf("creating leader lock failed: %w", err)
	}

	l.mu.Lock()
	l.lastVersion = 1
	l.mu.Unlock()

	return nil
}

func (l *Lock) Update(ctx context.Context, ler resourcelock.LeaderElectionRecord) error {
	raw, err := json.Marshal(ler)
	if err != nil {
		return fmt.Errorf("marshalling leader record failed: %w", err)
	}

	l.mu.Lock()
	expectedVersion := l.lastVersion
	l.mu.Unlock()

	if expectedVersion == 0 {
		return fmt.Errorf("updating called before get; no cached version")
	}

	const q = `
UPDATE leader_election_locks
SET record_json = $3,
    version = version + 1,
    updated_at = now()
WHERE namespace = $1
  AND name = $2
  AND version = $4
RETURNING version
`
	var newVersion int64
	err = l.pool.QueryRow(ctx, q, l.namespace, l.name, raw, expectedVersion).Scan(&newVersion)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrVersionConflict
		}
		return fmt.Errorf("updating leader lock failed: %w", err)
	}

	l.mu.Lock()
	l.lastVersion = newVersion
	l.mu.Unlock()

	return nil
}

func (l *Lock) CreateBootstrapElectionRecord(ctx context.Context, leaseDurationInSeconds int) error {
	err := l.Create(ctx, resourcelock.LeaderElectionRecord{
		HolderIdentity:       "",
		LeaseDurationSeconds: leaseDurationInSeconds,
		LeaderTransitions:    0,
	})
	if err != nil {
		if !errors.Is(err, ErrAlreadyExists) {
			return err
		}
	}

	return nil
}
