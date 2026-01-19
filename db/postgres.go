// Package db - PostgreSQL implementation of PricingStore
package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	_ "github.com/lib/pq"
)

// PostgresStore implements PricingStore using PostgreSQL
type PostgresStore struct {
	db *sql.DB
}

// Config holds database configuration
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSLMode  string
}

// NewPostgresStore creates a new PostgreSQL pricing store
func NewPostgresStore(cfg Config) (*PostgresStore, error) {
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Database, cfg.SSLMode,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Verify connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &PostgresStore{db: db}, nil
}

// NewPostgresStoreFromURL creates a new PostgreSQL pricing store from a URL
// URL format: postgres://user:password@host:port/dbname?sslmode=disable
func NewPostgresStoreFromURL(databaseURL string) (*PostgresStore, error) {
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Verify connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &PostgresStore{db: db}, nil
}

// Ping checks database connectivity
func (s *PostgresStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Close closes the database connection
func (s *PostgresStore) Close() error {
	return s.db.Close()
}

// CreateSnapshot inserts a new pricing snapshot
func (s *PostgresStore) CreateSnapshot(ctx context.Context, snapshot *PricingSnapshot) error {
	query := `
		INSERT INTO pricing_snapshots 
		(id, cloud, region, provider_alias, source, fetched_at, valid_from, valid_to, hash, version, is_active)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`
	_, err := s.db.ExecContext(ctx, query,
		snapshot.ID, snapshot.Cloud, snapshot.Region, snapshot.ProviderAlias,
		snapshot.Source, snapshot.FetchedAt, snapshot.ValidFrom, snapshot.ValidTo,
		snapshot.Hash, snapshot.Version, snapshot.IsActive,
	)
	return err
}

// GetSnapshot retrieves a snapshot by ID
func (s *PostgresStore) GetSnapshot(ctx context.Context, id uuid.UUID) (*PricingSnapshot, error) {
	query := `
		SELECT id, cloud, region, provider_alias, source, fetched_at, valid_from, valid_to, hash, version, is_active, created_at
		FROM pricing_snapshots WHERE id = $1
	`
	snapshot := &PricingSnapshot{}
	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&snapshot.ID, &snapshot.Cloud, &snapshot.Region, &snapshot.ProviderAlias,
		&snapshot.Source, &snapshot.FetchedAt, &snapshot.ValidFrom, &snapshot.ValidTo,
		&snapshot.Hash, &snapshot.Version, &snapshot.IsActive, &snapshot.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return snapshot, err
}

// GetActiveSnapshot retrieves the active snapshot for a cloud/region/alias
func (s *PostgresStore) GetActiveSnapshot(ctx context.Context, cloud CloudProvider, region, alias string) (*PricingSnapshot, error) {
	query := `
		SELECT id, cloud, region, provider_alias, source, fetched_at, valid_from, valid_to, hash, version, is_active, created_at
		FROM pricing_snapshots 
		WHERE cloud = $1 AND region = $2 AND provider_alias = $3 AND is_active = TRUE
	`
	snapshot := &PricingSnapshot{}
	err := s.db.QueryRowContext(ctx, query, cloud, region, alias).Scan(
		&snapshot.ID, &snapshot.Cloud, &snapshot.Region, &snapshot.ProviderAlias,
		&snapshot.Source, &snapshot.FetchedAt, &snapshot.ValidFrom, &snapshot.ValidTo,
		&snapshot.Hash, &snapshot.Version, &snapshot.IsActive, &snapshot.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return snapshot, err
}

// ActivateSnapshot activates a snapshot (deactivates others)
func (s *PostgresStore) ActivateSnapshot(ctx context.Context, id uuid.UUID) error {
	_, err := s.db.ExecContext(ctx, "SELECT activate_snapshot($1)", id)
	return err
}

// ListSnapshots lists snapshots for a cloud/region
func (s *PostgresStore) ListSnapshots(ctx context.Context, cloud CloudProvider, region string) ([]*PricingSnapshot, error) {
	query := `
		SELECT id, cloud, region, provider_alias, source, fetched_at, valid_from, valid_to, hash, version, is_active, created_at
		FROM pricing_snapshots 
		WHERE cloud = $1 AND region = $2
		ORDER BY created_at DESC
	`
	rows, err := s.db.QueryContext(ctx, query, cloud, region)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var snapshots []*PricingSnapshot
	for rows.Next() {
		s := &PricingSnapshot{}
		err := rows.Scan(
			&s.ID, &s.Cloud, &s.Region, &s.ProviderAlias,
			&s.Source, &s.FetchedAt, &s.ValidFrom, &s.ValidTo,
			&s.Hash, &s.Version, &s.IsActive, &s.CreatedAt,
		)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, s)
	}
	return snapshots, nil
}

// UpsertRateKey inserts or returns existing rate key
func (s *PostgresStore) UpsertRateKey(ctx context.Context, key *RateKey) (*RateKey, error) {
	attrsJSON, err := json.Marshal(key.Attributes)
	if err != nil {
		return nil, err
	}

	query := `
		INSERT INTO pricing_rate_keys (id, cloud, service, product_family, region, attributes)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (cloud, service, product_family, region, attributes) 
		DO UPDATE SET id = pricing_rate_keys.id
		RETURNING id, created_at
	`
	err = s.db.QueryRowContext(ctx, query,
		key.ID, key.Cloud, key.Service, key.ProductFamily, key.Region, attrsJSON,
	).Scan(&key.ID, &key.CreatedAt)
	return key, err
}

// GetRateKey retrieves a rate key
func (s *PostgresStore) GetRateKey(ctx context.Context, cloud CloudProvider, service, productFamily, region string, attrs map[string]string) (*RateKey, error) {
	attrsJSON, err := json.Marshal(attrs)
	if err != nil {
		return nil, err
	}

	query := `
		SELECT id, cloud, service, product_family, region, attributes, created_at
		FROM pricing_rate_keys
		WHERE cloud = $1 AND service = $2 AND product_family = $3 AND region = $4 AND attributes = $5
	`
	key := &RateKey{}
	var attrsBytes []byte
	err = s.db.QueryRowContext(ctx, query, cloud, service, productFamily, region, attrsJSON).Scan(
		&key.ID, &key.Cloud, &key.Service, &key.ProductFamily, &key.Region, &attrsBytes, &key.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	json.Unmarshal(attrsBytes, &key.Attributes)
	return key, nil
}

// CreateRate inserts a pricing rate
func (s *PostgresStore) CreateRate(ctx context.Context, rate *PricingRate) error {
	query := `
		INSERT INTO pricing_rates 
		(id, snapshot_id, rate_key_id, unit, price, currency, confidence, tier_min, tier_max, effective_date)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`
	_, err := s.db.ExecContext(ctx, query,
		rate.ID, rate.SnapshotID, rate.RateKeyID, rate.Unit,
		rate.Price, rate.Currency, rate.Confidence,
		rate.TierMin, rate.TierMax, rate.EffectiveDate,
	)
	return err
}

// BulkCreateRates inserts multiple rates efficiently
func (s *PostgresStore) BulkCreateRates(ctx context.Context, rates []*PricingRate) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO pricing_rates 
		(id, snapshot_id, rate_key_id, unit, price, currency, confidence, tier_min, tier_max, effective_date)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, rate := range rates {
		_, err := stmt.ExecContext(ctx,
			rate.ID, rate.SnapshotID, rate.RateKeyID, rate.Unit,
			rate.Price, rate.Currency, rate.Confidence,
			rate.TierMin, rate.TierMax, rate.EffectiveDate,
		)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// ResolveRate looks up a rate from the active snapshot
func (s *PostgresStore) ResolveRate(ctx context.Context, cloud CloudProvider, service, productFamily, region string, attrs map[string]string, unit, alias string) (*ResolvedRate, error) {
	attrsJSON, err := json.Marshal(attrs)
	if err != nil {
		return nil, err
	}

	query := `
		SELECT pr.price, pr.currency, pr.confidence, pr.tier_min, pr.tier_max, ps.id, ps.source
		FROM pricing_snapshots ps
		JOIN pricing_rate_keys rk ON rk.cloud = ps.cloud AND rk.region = ps.region
		JOIN pricing_rates pr ON pr.snapshot_id = ps.id AND pr.rate_key_id = rk.id
		WHERE ps.cloud = $1
		  AND ps.region = $2
		  AND ps.provider_alias = $3
		  AND ps.is_active = TRUE
		  AND rk.service = $4
		  AND rk.product_family = $5
		  AND rk.attributes @> $6
		  AND pr.unit = $7
		ORDER BY pr.tier_min NULLS FIRST
		LIMIT 1
	`
	
	rate := &ResolvedRate{}
	err = s.db.QueryRowContext(ctx, query, cloud, region, alias, service, productFamily, attrsJSON, unit).Scan(
		&rate.Price, &rate.Currency, &rate.Confidence, &rate.TierMin, &rate.TierMax, &rate.SnapshotID, &rate.Source,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return rate, err
}

// ResolveTieredRates returns all tiers for a rate
func (s *PostgresStore) ResolveTieredRates(ctx context.Context, cloud CloudProvider, service, productFamily, region string, attrs map[string]string, unit, alias string) ([]TieredRate, error) {
	attrsJSON, err := json.Marshal(attrs)
	if err != nil {
		return nil, err
	}

	query := `
		SELECT pr.price, pr.confidence, pr.tier_min, pr.tier_max
		FROM pricing_snapshots ps
		JOIN pricing_rate_keys rk ON rk.cloud = ps.cloud AND rk.region = ps.region
		JOIN pricing_rates pr ON pr.snapshot_id = ps.id AND pr.rate_key_id = rk.id
		WHERE ps.cloud = $1
		  AND ps.region = $2
		  AND ps.provider_alias = $3
		  AND ps.is_active = TRUE
		  AND rk.service = $4
		  AND rk.product_family = $5
		  AND rk.attributes @> $6
		  AND pr.unit = $7
		ORDER BY pr.tier_min NULLS FIRST
	`
	
	rows, err := s.db.QueryContext(ctx, query, cloud, region, alias, service, productFamily, attrsJSON, unit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tiers []TieredRate
	for rows.Next() {
		var t TieredRate
		var tierMin, tierMax *decimal.Decimal
		err := rows.Scan(&t.Price, &t.Confidence, &tierMin, &tierMax)
		if err != nil {
			return nil, err
		}
		if tierMin != nil {
			t.Min = *tierMin
		}
		t.Max = tierMax
		tiers = append(tiers, t)
	}
	return tiers, nil
}

// PostgresTx wraps a database transaction
type PostgresTx struct {
	tx *sql.Tx
}

// BeginTx starts a new transaction
func (s *PostgresStore) BeginTx(ctx context.Context) (Tx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &PostgresTx{tx: tx}, nil
}

// CreateSnapshot creates a snapshot within a transaction
func (t *PostgresTx) CreateSnapshot(ctx context.Context, snapshot *PricingSnapshot) error {
	query := `
		INSERT INTO pricing_snapshots 
		(id, cloud, region, provider_alias, source, fetched_at, valid_from, valid_to, hash, version, is_active)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`
	_, err := t.tx.ExecContext(ctx, query,
		snapshot.ID, snapshot.Cloud, snapshot.Region, snapshot.ProviderAlias,
		snapshot.Source, snapshot.FetchedAt, snapshot.ValidFrom, snapshot.ValidTo,
		snapshot.Hash, snapshot.Version, snapshot.IsActive,
	)
	return err
}

// UpsertRateKey inserts or returns existing rate key within a transaction
func (t *PostgresTx) UpsertRateKey(ctx context.Context, key *RateKey) (*RateKey, error) {
	attrsJSON, err := json.Marshal(key.Attributes)
	if err != nil {
		return nil, err
	}

	query := `
		INSERT INTO pricing_rate_keys (id, cloud, service, product_family, region, attributes)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (cloud, service, product_family, region, attributes) 
		DO UPDATE SET id = pricing_rate_keys.id
		RETURNING id, created_at
	`
	err = t.tx.QueryRowContext(ctx, query,
		key.ID, key.Cloud, key.Service, key.ProductFamily, key.Region, attrsJSON,
	).Scan(&key.ID, &key.CreatedAt)
	return key, err
}

// CreateRate creates a rate within a transaction
func (t *PostgresTx) CreateRate(ctx context.Context, rate *PricingRate) error {
	query := `
		INSERT INTO pricing_rates 
		(id, snapshot_id, rate_key_id, unit, price, currency, confidence, tier_min, tier_max, effective_date)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`
	_, err := t.tx.ExecContext(ctx, query,
		rate.ID, rate.SnapshotID, rate.RateKeyID, rate.Unit,
		rate.Price, rate.Currency, rate.Confidence,
		rate.TierMin, rate.TierMax, rate.EffectiveDate,
	)
	return err
}

// ActivateSnapshot activates a snapshot within a transaction
func (t *PostgresTx) ActivateSnapshot(ctx context.Context, id uuid.UUID) error {
	// Deactivate existing snapshots for same cloud/region/alias
	_, err := t.tx.ExecContext(ctx, `
		UPDATE pricing_snapshots SET is_active = FALSE 
		WHERE id IN (
			SELECT ps2.id FROM pricing_snapshots ps2 
			JOIN pricing_snapshots ps1 ON ps1.cloud = ps2.cloud 
				AND ps1.region = ps2.region 
				AND ps1.provider_alias = ps2.provider_alias
			WHERE ps1.id = $1 AND ps2.id != $1 AND ps2.is_active = TRUE
		)
	`, id)
	if err != nil {
		return err
	}
	
	// Activate the new snapshot
	_, err = t.tx.ExecContext(ctx, `
		UPDATE pricing_snapshots SET is_active = TRUE WHERE id = $1
	`, id)
	return err
}

// Commit commits the transaction
func (t *PostgresTx) Commit() error {
	return t.tx.Commit()
}

// Rollback rolls back the transaction
func (t *PostgresTx) Rollback() error {
	return t.tx.Rollback()
}

// FindSnapshotByHash finds a snapshot with matching content hash
func (s *PostgresStore) FindSnapshotByHash(ctx context.Context, cloud CloudProvider, region, alias, hash string) (*PricingSnapshot, error) {
	query := `
		SELECT id, cloud, region, provider_alias, source, fetched_at, valid_from, valid_to, hash, version, is_active, created_at
		FROM pricing_snapshots 
		WHERE cloud = $1 AND region = $2 AND provider_alias = $3 AND hash = $4
		ORDER BY created_at DESC
		LIMIT 1
	`
	snapshot := &PricingSnapshot{}
	err := s.db.QueryRowContext(ctx, query, cloud, region, alias, hash).Scan(
		&snapshot.ID, &snapshot.Cloud, &snapshot.Region, &snapshot.ProviderAlias,
		&snapshot.Source, &snapshot.FetchedAt, &snapshot.ValidFrom, &snapshot.ValidTo,
		&snapshot.Hash, &snapshot.Version, &snapshot.IsActive, &snapshot.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return snapshot, err
}

// CountRates returns the count of rates in a snapshot
func (s *PostgresStore) CountRates(ctx context.Context, snapshotID uuid.UUID) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, 
		"SELECT COUNT(*) FROM pricing_rates WHERE snapshot_id = $1", 
		snapshotID,
	).Scan(&count)
	return count, err
}
