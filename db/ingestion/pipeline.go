// Package ingestion - Pricing data ingestion pipeline
// Strictly separated from estimation: fetch → normalize → validate → backup → commit
package ingestion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"terraform-cost/db"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// Phase represents a pipeline execution phase
type Phase string

const (
	PhaseFetch     Phase = "fetch"
	PhaseNormalize Phase = "normalize"
	PhaseValidate  Phase = "validate"
	PhaseBackup    Phase = "backup"
	PhaseCommit    Phase = "commit"
)

// PipelineConfig configures the ingestion pipeline
type PipelineConfig struct {
	// Provider is the cloud provider (aws, azure, gcp)
	Provider db.CloudProvider

	// Region to ingest
	Region string

	// Alias for multi-account (default if empty)
	Alias string

	// DryRun validates only, no DB writes
	DryRun bool

	// BackupDir is where to write backups
	BackupDir string

	// MinCoveragePercent is the minimum required coverage (default 95%)
	MinCoveragePercent float64

	// Timeout for the entire pipeline
	Timeout time.Duration
}

// DefaultPipelineConfig returns production defaults
func DefaultPipelineConfig() *PipelineConfig {
	return &PipelineConfig{
		Alias:              "default",
		DryRun:             false,
		BackupDir:          "./pricing-backups",
		MinCoveragePercent: 95.0, // Very high coverage required
		Timeout:            30 * time.Minute,
	}
}

// PipelineResult contains the outcome of a pipeline run
type PipelineResult struct {
	// Success indicates all phases completed
	Success bool `json:"success"`

	// SnapshotID of the created snapshot (nil if dry-run or failure)
	SnapshotID *uuid.UUID `json:"snapshot_id,omitempty"`

	// Phases completed
	PhasesCompleted []Phase `json:"phases_completed"`

	// FailedPhase if any
	FailedPhase Phase `json:"failed_phase,omitempty"`

	// Error message if failed
	Error string `json:"error,omitempty"`

	// Stats from the run
	Stats PipelineStats `json:"stats"`

	// BackupPath where backup was written
	BackupPath string `json:"backup_path,omitempty"`

	// Duration of the run
	Duration time.Duration `json:"duration"`
}

// PipelineStats contains statistics from a run
type PipelineStats struct {
	RawPricesCount       int     `json:"raw_prices_count"`
	NormalizedRatesCount int     `json:"normalized_rates_count"`
	UniqueServicesCount  int     `json:"unique_services_count"`
	CoveragePercent      float64 `json:"coverage_percent"`
	ContentHash          string  `json:"content_hash"`
}

// RawPrice represents a raw price record from a cloud API
type RawPrice struct {
	SKU           string            `json:"sku"`
	ServiceCode   string            `json:"service_code"`
	ProductFamily string            `json:"product_family"`
	Region        string            `json:"region"`
	Unit          string            `json:"unit"`
	PricePerUnit  string            `json:"price_per_unit"`
	Currency      string            `json:"currency"`
	Attributes    map[string]string `json:"attributes"`
	TierStart     *float64          `json:"tier_start,omitempty"`
	TierEnd       *float64          `json:"tier_end,omitempty"`
	EffectiveDate *time.Time        `json:"effective_date,omitempty"`
}

// NormalizedRate is the output of normalization
type NormalizedRate struct {
	RateKey    db.RateKey      `json:"rate_key"`
	Unit       string          `json:"unit"`
	Price      decimal.Decimal `json:"price"`
	Currency   string          `json:"currency"`
	Confidence float64         `json:"confidence"`
	TierMin    *decimal.Decimal `json:"tier_min,omitempty"`
	TierMax    *decimal.Decimal `json:"tier_max,omitempty"`
}

// PriceFetcher fetches raw prices from a cloud API
type PriceFetcher interface {
	// Cloud returns the cloud provider
	Cloud() db.CloudProvider

	// FetchRegion fetches all prices for a region (NO DB WRITES)
	FetchRegion(ctx context.Context, region string) ([]RawPrice, error)

	// SupportedRegions returns supported regions
	SupportedRegions() []string

	// SupportedServices returns services this fetcher covers
	SupportedServices() []string
}

// PriceNormalizer converts raw prices to normalized rates
type PriceNormalizer interface {
	// Cloud returns the cloud provider
	Cloud() db.CloudProvider

	// Normalize converts raw prices to normalized rates
	Normalize(raw []RawPrice) ([]NormalizedRate, error)
}

// Pipeline orchestrates the full ingestion flow with 5 strict phases
type Pipeline struct {
	fetcher    PriceFetcher
	normalizer PriceNormalizer
	validator  *IngestionValidator
	backupMgr  *BackupManager
	store      db.PricingStore
}

// NewPipeline creates a new ingestion pipeline
func NewPipeline(
	fetcher PriceFetcher,
	normalizer PriceNormalizer,
	store db.PricingStore,
) *Pipeline {
	return &Pipeline{
		fetcher:    fetcher,
		normalizer: normalizer,
		validator:  NewIngestionValidator(),
		backupMgr:  NewBackupManager(),
		store:      store,
	}
}

// Execute runs the full 5-phase ingestion pipeline
func (p *Pipeline) Execute(ctx context.Context, config *PipelineConfig) (*PipelineResult, error) {
	if config == nil {
		config = DefaultPipelineConfig()
	}

	start := time.Now()
	result := &PipelineResult{
		PhasesCompleted: make([]Phase, 0, 5),
	}

	// Apply timeout
	if config.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, config.Timeout)
		defer cancel()
	}

	// ========================================
	// PHASE A: FETCH (NO DB WRITES)
	// ========================================
	rawPrices, err := p.phaseFetch(ctx, config)
	if err != nil {
		result.FailedPhase = PhaseFetch
		result.Error = err.Error()
		result.Duration = time.Since(start)
		return result, nil
	}
	result.PhasesCompleted = append(result.PhasesCompleted, PhaseFetch)
	result.Stats.RawPricesCount = len(rawPrices)

	// ========================================
	// PHASE B: NORMALIZE
	// ========================================
	normalizedRates, err := p.phaseNormalize(ctx, rawPrices)
	if err != nil {
		result.FailedPhase = PhaseNormalize
		result.Error = err.Error()
		result.Duration = time.Since(start)
		return result, nil
	}
	result.PhasesCompleted = append(result.PhasesCompleted, PhaseNormalize)
	result.Stats.NormalizedRatesCount = len(normalizedRates)
	result.Stats.UniqueServicesCount = countUniqueServices(normalizedRates)
	result.Stats.ContentHash = calculateHash(normalizedRates)

	// ========================================
	// PHASE C: VALIDATE (NON-NEGOTIABLE)
	// ========================================
	validationErr := p.phaseValidate(ctx, config, normalizedRates)
	if validationErr != nil {
		result.FailedPhase = PhaseValidate
		result.Error = validationErr.Error()
		result.Duration = time.Since(start)
		return result, nil
	}
	result.PhasesCompleted = append(result.PhasesCompleted, PhaseValidate)

	// ========================================
	// PHASE D: BACKUP (MANDATORY)
	// ========================================
	backupPath, err := p.phaseBackup(ctx, config, normalizedRates, result.Stats)
	if err != nil {
		result.FailedPhase = PhaseBackup
		result.Error = err.Error()
		result.Duration = time.Since(start)
		return result, nil
	}
	result.PhasesCompleted = append(result.PhasesCompleted, PhaseBackup)
	result.BackupPath = backupPath

	// ========================================
	// DRY-RUN CHECK
	// ========================================
	if config.DryRun {
		result.Success = true
		result.Duration = time.Since(start)
		return result, nil
	}

	// ========================================
	// PHASE E: ATOMIC DATABASE COMMIT
	// ========================================
	snapshotID, err := p.phaseCommit(ctx, config, normalizedRates, result.Stats.ContentHash)
	if err != nil {
		result.FailedPhase = PhaseCommit
		result.Error = err.Error()
		result.Duration = time.Since(start)
		return result, nil
	}
	result.PhasesCompleted = append(result.PhasesCompleted, PhaseCommit)
	result.SnapshotID = &snapshotID

	result.Success = true
	result.Duration = time.Since(start)
	return result, nil
}

// phaseFetch downloads raw pricing (NO DB WRITES)
func (p *Pipeline) phaseFetch(ctx context.Context, config *PipelineConfig) ([]RawPrice, error) {
	rawPrices, err := p.fetcher.FetchRegion(ctx, config.Region)
	if err != nil {
		return nil, fmt.Errorf("fetch failed for %s/%s: %w", config.Provider, config.Region, err)
	}

	if len(rawPrices) == 0 {
		return nil, fmt.Errorf("fetch returned 0 prices for %s/%s", config.Provider, config.Region)
	}

	return rawPrices, nil
}

// phaseNormalize transforms raw prices to canonical rates
func (p *Pipeline) phaseNormalize(ctx context.Context, rawPrices []RawPrice) ([]NormalizedRate, error) {
	normalized, err := p.normalizer.Normalize(rawPrices)
	if err != nil {
		return nil, fmt.Errorf("normalization failed: %w", err)
	}

	if len(normalized) == 0 {
		return nil, fmt.Errorf("normalization produced 0 rates")
	}

	return normalized, nil
}

// phaseValidate runs governance checks (abort on failure)
func (p *Pipeline) phaseValidate(ctx context.Context, config *PipelineConfig, rates []NormalizedRate) error {
	// Configure validator
	p.validator.SetMinCoveragePercent(config.MinCoveragePercent)

	// Get previous snapshot for coverage comparison
	prevSnapshot, _ := p.store.GetActiveSnapshot(ctx, config.Provider, config.Region, config.Alias)
	var prevRateCount int
	if prevSnapshot != nil {
		prevRateCount, _ = p.store.CountRates(ctx, prevSnapshot.ID)
	}

	// Run all validations
	return p.validator.ValidateAll(rates, prevRateCount)
}

// phaseBackup writes snapshot dump to local file
func (p *Pipeline) phaseBackup(ctx context.Context, config *PipelineConfig, rates []NormalizedRate, stats PipelineStats) (string, error) {
	backup := &SnapshotBackup{
		Provider:      config.Provider,
		Region:        config.Region,
		Alias:         config.Alias,
		Timestamp:     time.Now(),
		ContentHash:   stats.ContentHash,
		RateCount:     len(rates),
		SchemaVersion: "1.0",
		Rates:         rates,
	}

	return p.backupMgr.WriteBackup(config.BackupDir, backup)
}

// phaseCommit atomically writes to database
func (p *Pipeline) phaseCommit(ctx context.Context, config *PipelineConfig, rates []NormalizedRate, contentHash string) (uuid.UUID, error) {
	// Check for existing snapshot with same hash (idempotency)
	existing, _ := p.store.FindSnapshotByHash(ctx, config.Provider, config.Region, config.Alias, contentHash)
	if existing != nil {
		// Already ingested, return existing
		return existing.ID, nil
	}

	// Create snapshot in a single transaction
	snapshot := &db.PricingSnapshot{
		ID:            uuid.New(),
		Cloud:         config.Provider,
		Region:        config.Region,
		ProviderAlias: config.Alias,
		Source:        "manual_ingestion_pipeline",
		FetchedAt:     time.Now(),
		ValidFrom:     time.Now(),
		Hash:          contentHash,
		Version:       "1.0",
		IsActive:      false, // Not active until commit succeeds
	}

	// Begin transaction
	tx, err := p.store.BeginTx(ctx)
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Rollback on any error
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Insert snapshot
	if err = tx.CreateSnapshot(ctx, snapshot); err != nil {
		return uuid.Nil, fmt.Errorf("failed to create snapshot: %w", err)
	}

	// Insert all rates
	for _, nr := range rates {
		nr.RateKey.ID = uuid.New()
		key, err := tx.UpsertRateKey(ctx, &nr.RateKey)
		if err != nil {
			return uuid.Nil, fmt.Errorf("failed to upsert rate key: %w", err)
		}

		rate := &db.PricingRate{
			ID:         uuid.New(),
			SnapshotID: snapshot.ID,
			RateKeyID:  key.ID,
			Unit:       nr.Unit,
			Price:      nr.Price,
			Currency:   nr.Currency,
			Confidence: nr.Confidence,
			TierMin:    nr.TierMin,
			TierMax:    nr.TierMax,
		}
		if err = tx.CreateRate(ctx, rate); err != nil {
			return uuid.Nil, fmt.Errorf("failed to create rate: %w", err)
		}
	}

	// Mark snapshot as ready and activate
	if err = tx.ActivateSnapshot(ctx, snapshot.ID); err != nil {
		return uuid.Nil, fmt.Errorf("failed to activate snapshot: %w", err)
	}

	// Commit transaction
	if err = tx.Commit(); err != nil {
		return uuid.Nil, fmt.Errorf("commit failed: %w", err)
	}

	return snapshot.ID, nil
}

// calculateHash computes a deterministic hash of rates
func calculateHash(rates []NormalizedRate) string {
	// Sort for determinism
	sorted := make([]NormalizedRate, len(rates))
	copy(sorted, rates)
	sort.Slice(sorted, func(i, j int) bool {
		ki := rateKeyString(sorted[i].RateKey)
		kj := rateKeyString(sorted[j].RateKey)
		return ki < kj
	})

	hasher := sha256.New()
	for _, r := range sorted {
		hasher.Write([]byte(rateKeyString(r.RateKey)))
		hasher.Write([]byte(r.Unit))
		hasher.Write([]byte(r.Price.String()))
	}

	return hex.EncodeToString(hasher.Sum(nil))
}

func rateKeyString(k db.RateKey) string {
	attrs := make([]string, 0, len(k.Attributes))
	for k, v := range k.Attributes {
		attrs = append(attrs, k+"="+v)
	}
	sort.Strings(attrs)
	return fmt.Sprintf("%s|%s|%s|%s|%s", k.Cloud, k.Service, k.ProductFamily, k.Region, strings.Join(attrs, ","))
}

func countUniqueServices(rates []NormalizedRate) int {
	services := make(map[string]bool)
	for _, r := range rates {
		services[r.RateKey.Service] = true
	}
	return len(services)
}

// NormalizeAttributes canonicalizes attribute keys and values
func NormalizeAttributes(raw map[string]string) map[string]string {
	result := make(map[string]string)
	for k, v := range raw {
		key := strings.ToLower(strings.ReplaceAll(k, " ", "_"))
		val := strings.ToLower(strings.TrimSpace(v))
		result[key] = val
	}
	return result
}

// ParsePrice parses a price string to decimal
func ParsePrice(s string) (decimal.Decimal, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return decimal.Zero, nil
	}
	return decimal.NewFromString(s)
}

// MarshalRateKey converts a RateKey to JSON for storage
func MarshalRateKey(k db.RateKey) ([]byte, error) {
	return json.Marshal(k.Attributes)
}
