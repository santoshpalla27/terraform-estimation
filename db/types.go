// Package db - Pricing database types and interface
// Provides snapshot-based, immutable pricing storage.
package db

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// CloudProvider represents a cloud provider
type CloudProvider string

const (
	AWS   CloudProvider = "aws"
	Azure CloudProvider = "azure"
	GCP   CloudProvider = "gcp"
)

// PricingSnapshot represents a point-in-time pricing capture
type PricingSnapshot struct {
	ID            uuid.UUID     `db:"id" json:"id"`
	Cloud         CloudProvider `db:"cloud" json:"cloud"`
	Region        string        `db:"region" json:"region"`
	ProviderAlias string        `db:"provider_alias" json:"provider_alias"`
	Source        string        `db:"source" json:"source"`
	FetchedAt     time.Time     `db:"fetched_at" json:"fetched_at"`
	ValidFrom     time.Time     `db:"valid_from" json:"valid_from"`
	ValidTo       *time.Time    `db:"valid_to" json:"valid_to,omitempty"`
	Hash          string        `db:"hash" json:"hash"`
	Version       string        `db:"version" json:"version"`
	IsActive      bool          `db:"is_active" json:"is_active"`
	CreatedAt     time.Time     `db:"created_at" json:"created_at"`
}

// RateKey represents a unique pricing lookup key
type RateKey struct {
	ID            uuid.UUID         `db:"id" json:"id"`
	Cloud         CloudProvider     `db:"cloud" json:"cloud"`
	Service       string            `db:"service" json:"service"`
	ProductFamily string            `db:"product_family" json:"product_family"`
	Region        string            `db:"region" json:"region"`
	Attributes    map[string]string `db:"attributes" json:"attributes"`
	CreatedAt     time.Time         `db:"created_at" json:"created_at"`
}

// PricingRate represents a price for a rate key within a snapshot
type PricingRate struct {
	ID            uuid.UUID       `db:"id" json:"id"`
	SnapshotID    uuid.UUID       `db:"snapshot_id" json:"snapshot_id"`
	RateKeyID     uuid.UUID       `db:"rate_key_id" json:"rate_key_id"`
	Unit          string          `db:"unit" json:"unit"`
	Price         decimal.Decimal `db:"price" json:"price"`
	Currency      string          `db:"currency" json:"currency"`
	Confidence    float64         `db:"confidence" json:"confidence"`
	TierMin       *decimal.Decimal `db:"tier_min" json:"tier_min,omitempty"`
	TierMax       *decimal.Decimal `db:"tier_max" json:"tier_max,omitempty"`
	EffectiveDate *time.Time      `db:"effective_date" json:"effective_date,omitempty"`
	CreatedAt     time.Time       `db:"created_at" json:"created_at"`
}

// ResolvedRate is the result of a pricing lookup
type ResolvedRate struct {
	Price      decimal.Decimal
	Currency   string
	Confidence float64
	TierMin    *decimal.Decimal
	TierMax    *decimal.Decimal
	SnapshotID uuid.UUID
	Source     string
}

// TieredRate represents a pricing tier
type TieredRate struct {
	Min        decimal.Decimal
	Max        *decimal.Decimal // nil = unlimited
	Price      decimal.Decimal
	Confidence float64
}

// PricingStore is the interface for pricing database operations
type PricingStore interface {
	// Snapshots
	CreateSnapshot(ctx context.Context, snapshot *PricingSnapshot) error
	GetSnapshot(ctx context.Context, id uuid.UUID) (*PricingSnapshot, error)
	GetActiveSnapshot(ctx context.Context, cloud CloudProvider, region, alias string) (*PricingSnapshot, error)
	ActivateSnapshot(ctx context.Context, id uuid.UUID) error
	ListSnapshots(ctx context.Context, cloud CloudProvider, region string) ([]*PricingSnapshot, error)
	FindSnapshotByHash(ctx context.Context, cloud CloudProvider, region, alias, hash string) (*PricingSnapshot, error)

	// Rate Keys
	UpsertRateKey(ctx context.Context, key *RateKey) (*RateKey, error)
	GetRateKey(ctx context.Context, cloud CloudProvider, service, productFamily, region string, attrs map[string]string) (*RateKey, error)

	// Rates
	CreateRate(ctx context.Context, rate *PricingRate) error
	BulkCreateRates(ctx context.Context, rates []*PricingRate) error
	CountRates(ctx context.Context, snapshotID uuid.UUID) (int, error)
	
	// Resolution
	ResolveRate(ctx context.Context, cloud CloudProvider, service, productFamily, region string, attrs map[string]string, unit, alias string) (*ResolvedRate, error)
	ResolveTieredRates(ctx context.Context, cloud CloudProvider, service, productFamily, region string, attrs map[string]string, unit, alias string) ([]TieredRate, error)

	// Transactions
	BeginTx(ctx context.Context) (Tx, error)

	// Health
	Ping(ctx context.Context) error
	Close() error
}

// Tx is a transaction interface for atomic operations
type Tx interface {
	CreateSnapshot(ctx context.Context, snapshot *PricingSnapshot) error
	UpsertRateKey(ctx context.Context, key *RateKey) (*RateKey, error)
	CreateRate(ctx context.Context, rate *PricingRate) error
	ActivateSnapshot(ctx context.Context, id uuid.UUID) error
	Commit() error
	Rollback() error
}

// SnapshotBuilder helps construct pricing snapshots
type SnapshotBuilder struct {
	snapshot *PricingSnapshot
	rates    []*RateKey
}

// NewSnapshotBuilder creates a new snapshot builder
func NewSnapshotBuilder(cloud CloudProvider, region, source string) *SnapshotBuilder {
	return &SnapshotBuilder{
		snapshot: &PricingSnapshot{
			ID:            uuid.New(),
			Cloud:         cloud,
			Region:        region,
			ProviderAlias: "default",
			Source:        source,
			FetchedAt:     time.Now(),
			ValidFrom:     time.Now(),
			Version:       "1.0",
		},
		rates: make([]*RateKey, 0),
	}
}

// WithAlias sets the provider alias
func (b *SnapshotBuilder) WithAlias(alias string) *SnapshotBuilder {
	b.snapshot.ProviderAlias = alias
	return b
}

// WithValidRange sets the validity period
func (b *SnapshotBuilder) WithValidRange(from, to time.Time) *SnapshotBuilder {
	b.snapshot.ValidFrom = from
	b.snapshot.ValidTo = &to
	return b
}

// Build finalizes the snapshot
func (b *SnapshotBuilder) Build(hash string) *PricingSnapshot {
	b.snapshot.Hash = hash
	return b.snapshot
}
