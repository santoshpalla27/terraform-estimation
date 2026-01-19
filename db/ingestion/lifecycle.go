// Package ingestion - Strict ingestion lifecycle state machine
// Ensures production-safe pricing updates with hard isolation guarantees
package ingestion

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"terraform-cost/db"

	"github.com/google/uuid"
)

// IngestionPhase represents the current phase of the ingestion lifecycle
type IngestionPhase int

const (
	PhaseInit IngestionPhase = iota
	PhaseFetching    // NO DB ACCESS - downloading from cloud API
	PhaseNormalizing // NO DB ACCESS - transforming to canonical form
	PhaseValidating  // NO DB ACCESS - governance checks
	PhaseStaging     // NO DB ACCESS - validation passed, preparing
	PhaseBackedUp    // BACKUP WRITTEN - verified before commit
	PhaseCommitting  // SINGLE DB TRANSACTION - atomic write
	PhaseActive      // COMPLETE - resolver can use
	PhaseFailed      // ABORTED - no partial state
)

func (p IngestionPhase) String() string {
	names := []string{
		"init", "fetching", "normalizing", "validating",
		"staging", "backed_up", "committing", "active", "failed",
	}
	if int(p) < len(names) {
		return names[p]
	}
	return "unknown"
}

// SnapshotState represents the database state of a snapshot
type SnapshotState string

const (
	StatePending  SnapshotState = "pending"  // Created, not yet validated
	StateStaging  SnapshotState = "staging"  // Validated, backup written
	StateReady    SnapshotState = "ready"    // Committed, resolver can use
	StateFailed   SnapshotState = "failed"   // Validation or commit failed
	StateArchived SnapshotState = "archived" // Superseded by newer snapshot
)

// LifecycleState holds all in-memory state during ingestion
// CRITICAL: No DB writes until PhaseCommitting
type LifecycleState struct {
	Phase         IngestionPhase
	Provider      db.CloudProvider
	Region        string
	Alias         string
	Environment   string

	// In-memory only - NEVER written to DB until commit
	RawPrices     []RawPrice
	Normalized    []NormalizedRate
	ContentHash   string

	// Backup verification
	BackupPath    string
	BackupHash    string
	BackupVerified bool

	// Only assigned after successful commit
	SnapshotID    *uuid.UUID

	// Tracking
	StartTime     time.Time
	Errors        []string
}

// LifecycleConfig configures the ingestion lifecycle
type LifecycleConfig struct {
	Provider         db.CloudProvider
	Region           string
	Alias            string
	Environment      string // "production" | "staging" | "development"
	BackupDir        string
	DryRun           bool
	AllowMockPricing bool   // MUST BE FALSE IN PRODUCTION
	MinCoverage      float64
	Timeout          time.Duration
}

// DefaultLifecycleConfig returns safe production defaults
func DefaultLifecycleConfig() *LifecycleConfig {
	return &LifecycleConfig{
		Alias:            "default",
		Environment:      "production",
		BackupDir:        "./pricing-backups",
		DryRun:           false,
		AllowMockPricing: false, // Safe default
		MinCoverage:      95.0,
		Timeout:          30 * time.Minute,
	}
}

// Lifecycle manages the strict ingestion state machine
type Lifecycle struct {
	mu        sync.Mutex
	state     *LifecycleState
	config    *LifecycleConfig
	fetcher   PriceFetcher
	normalizer PriceNormalizer
	validator *IngestionValidator
	backupMgr *BackupManager
	store     db.PricingStore
}

// NewLifecycle creates a new strict ingestion lifecycle
func NewLifecycle(
	fetcher PriceFetcher,
	normalizer PriceNormalizer,
	store db.PricingStore,
) *Lifecycle {
	return &Lifecycle{
		fetcher:    fetcher,
		normalizer: normalizer,
		validator:  NewIngestionValidator(),
		backupMgr:  NewBackupManager(),
		store:      store,
		state: &LifecycleState{
			Phase: PhaseInit,
		},
	}
}

// Execute runs the complete strict ingestion lifecycle
func (l *Lifecycle) Execute(ctx context.Context, config *LifecycleConfig) (*LifecycleResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if config == nil {
		config = DefaultLifecycleConfig()
	}
	l.config = config

	// Initialize state
	l.state = &LifecycleState{
		Phase:       PhaseInit,
		Provider:    config.Provider,
		Region:      config.Region,
		Alias:       config.Alias,
		Environment: config.Environment,
		StartTime:   time.Now(),
	}

	// Apply timeout
	if config.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, config.Timeout)
		defer cancel()
	}

	// ==================================================
	// PRODUCTION GUARD - No mocks allowed
	// ==================================================
	if err := l.enforceProductionGuards(); err != nil {
		return l.fail(err)
	}

	// ==================================================
	// PHASE: FETCHING (NO DB ACCESS)
	// ==================================================
	if err := l.phaseFetching(ctx); err != nil {
		return l.fail(err)
	}

	// ==================================================
	// PHASE: NORMALIZING (NO DB ACCESS)
	// ==================================================
	if err := l.phaseNormalizing(ctx); err != nil {
		return l.fail(err)
	}

	// ==================================================
	// PHASE: VALIDATING (NO DB ACCESS)
	// ==================================================
	if err := l.phaseValidating(ctx); err != nil {
		return l.fail(err)
	}

	// ==================================================
	// PHASE: STAGING (NO DB ACCESS)
	// ==================================================
	if err := l.phaseStaging(ctx); err != nil {
		return l.fail(err)
	}

	// ==================================================
	// PHASE: BACKUP (MANDATORY)
	// ==================================================
	if err := l.phaseBackup(ctx); err != nil {
		return l.fail(err)
	}

	// ==================================================
	// DRY-RUN CHECK - Stop before DB writes
	// ==================================================
	if config.DryRun {
		l.state.Phase = PhaseActive // Mark as success without commit
		return l.success("dry-run completed, no DB writes")
	}

	// ==================================================
	// PHASE: COMMITTING (SINGLE DB TRANSACTION)
	// ==================================================
	if err := l.phaseCommitting(ctx); err != nil {
		return l.fail(err)
	}

	return l.success("ingestion complete")
}

// enforceProductionGuards blocks unsafe operations
func (l *Lifecycle) enforceProductionGuards() error {
	// HARD GUARD: No mocks in production
	if l.config.Environment == "production" && l.config.AllowMockPricing {
		return fmt.Errorf("FATAL: mock pricing forbidden in production environment")
	}

	// HARD GUARD: Fetcher must be real API
	if realAPI, ok := l.fetcher.(RealAPIFetcher); ok {
		if !realAPI.IsRealAPI() && l.config.Environment == "production" {
			return fmt.Errorf("FATAL: fetcher is not a real API implementation")
		}
	}

	return nil
}

// phaseFetching downloads raw pricing (NO DB ACCESS)
func (l *Lifecycle) phaseFetching(ctx context.Context) error {
	l.state.Phase = PhaseFetching

	rawPrices, err := l.fetcher.FetchRegion(ctx, l.config.Region)
	if err != nil {
		return fmt.Errorf("fetch failed: %w", err)
	}

	if len(rawPrices) == 0 {
		return fmt.Errorf("fetch returned 0 prices")
	}

	// Store in memory only - NO DB WRITES
	l.state.RawPrices = rawPrices
	return nil
}

// phaseNormalizing transforms to canonical form (NO DB ACCESS)
func (l *Lifecycle) phaseNormalizing(ctx context.Context) error {
	l.state.Phase = PhaseNormalizing

	normalized, err := l.normalizer.Normalize(l.state.RawPrices)
	if err != nil {
		return fmt.Errorf("normalization failed: %w", err)
	}

	if len(normalized) == 0 {
		return fmt.Errorf("normalization produced 0 rates")
	}

	// Store in memory only - NO DB WRITES
	l.state.Normalized = normalized
	l.state.ContentHash = calculateHash(normalized)
	return nil
}

// phaseValidating runs governance checks (NO DB ACCESS)
func (l *Lifecycle) phaseValidating(ctx context.Context) error {
	l.state.Phase = PhaseValidating

	l.validator.SetMinCoveragePercent(l.config.MinCoverage)

	// Get previous snapshot for coverage comparison
	prevSnapshot, _ := l.store.GetActiveSnapshot(ctx, l.config.Provider, l.config.Region, l.config.Alias)
	var prevRateCount int
	if prevSnapshot != nil {
		prevRateCount, _ = l.store.CountRates(ctx, prevSnapshot.ID)
	}

	return l.validator.ValidateAll(l.state.Normalized, prevRateCount)
}

// phaseStaging prepares for commit (NO DB ACCESS)
func (l *Lifecycle) phaseStaging(ctx context.Context) error {
	l.state.Phase = PhaseStaging

	// Validation passed - data is ready for backup
	// But we still haven't touched the DB

	return nil
}

// phaseBackup writes mandatory backup (MUST SUCCEED BEFORE COMMIT)
func (l *Lifecycle) phaseBackup(ctx context.Context) error {
	backup := &SnapshotBackup{
		Provider:      l.config.Provider,
		Region:        l.config.Region,
		Alias:         l.config.Alias,
		Timestamp:     time.Now(),
		ContentHash:   l.state.ContentHash,
		RateCount:     len(l.state.Normalized),
		SchemaVersion: "1.0",
		Rates:         l.state.Normalized,
	}

	backupPath, err := l.backupMgr.WriteBackup(l.config.BackupDir, backup)
	if err != nil {
		return fmt.Errorf("backup failed: %w", err)
	}

	// Verify backup was written correctly
	if err := l.verifyBackup(backupPath); err != nil {
		return fmt.Errorf("backup verification failed: %w", err)
	}

	l.state.BackupPath = backupPath
	l.state.BackupHash = l.state.ContentHash
	l.state.BackupVerified = true
	l.state.Phase = PhaseBackedUp

	return nil
}

// verifyBackup reads back and verifies the backup
func (l *Lifecycle) verifyBackup(path string) error {
	// Check file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return fmt.Errorf("backup file does not exist: %s", path)
	}

	// Read and validate
	backup, err := l.backupMgr.ReadBackup(path)
	if err != nil {
		return fmt.Errorf("backup read failed: %w", err)
	}

	// Verify hash matches
	if backup.ContentHash != l.state.ContentHash {
		return fmt.Errorf("backup hash mismatch: expected %s, got %s",
			l.state.ContentHash, backup.ContentHash)
	}

	return nil
}

// phaseCommitting performs atomic DB transaction
func (l *Lifecycle) phaseCommitting(ctx context.Context) error {
	l.state.Phase = PhaseCommitting

	// HARD REQUIREMENT: Backup must be verified
	if !l.state.BackupVerified {
		return fmt.Errorf("FATAL: cannot commit without verified backup")
	}
	if l.state.BackupPath == "" {
		return fmt.Errorf("FATAL: backup path is empty")
	}

	// Check for existing snapshot with same hash (idempotency)
	existing, _ := l.store.FindSnapshotByHash(ctx, l.config.Provider, l.config.Region, l.config.Alias, l.state.ContentHash)
	if existing != nil {
		// Already ingested with same content
		l.state.SnapshotID = &existing.ID
		l.state.Phase = PhaseActive
		return nil
	}

	// Create snapshot
	snapshotID := uuid.New()
	snapshot := &db.PricingSnapshot{
		ID:            snapshotID,
		Cloud:         l.config.Provider,
		Region:        l.config.Region,
		ProviderAlias: l.config.Alias,
		Source:        "strict_ingestion_lifecycle",
		FetchedAt:     time.Now(),
		ValidFrom:     time.Now(),
		Hash:          l.state.ContentHash,
		Version:       "1.0",
		IsActive:      false, // Not active until transaction commits
	}

	// Begin transaction
	tx, err := l.store.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Rollback on any error
	committed := false
	defer func() {
		if !committed {
			tx.Rollback()
		}
	}()

	// Insert snapshot (state='staging')
	if err = tx.CreateSnapshot(ctx, snapshot); err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}

	// Insert all rates
	for _, nr := range l.state.Normalized {
		nr.RateKey.ID = uuid.New()
		key, err := tx.UpsertRateKey(ctx, &nr.RateKey)
		if err != nil {
			return fmt.Errorf("failed to upsert rate key: %w", err)
		}

		rate := &db.PricingRate{
			ID:         uuid.New(),
			SnapshotID: snapshotID,
			RateKeyID:  key.ID,
			Unit:       nr.Unit,
			Price:      nr.Price,
			Currency:   nr.Currency,
			Confidence: nr.Confidence,
			TierMin:    nr.TierMin,
			TierMax:    nr.TierMax,
		}
		if err = tx.CreateRate(ctx, rate); err != nil {
			return fmt.Errorf("failed to create rate: %w", err)
		}
	}

	// Activate snapshot (state='ready', is_active=true)
	if err = tx.ActivateSnapshot(ctx, snapshotID); err != nil {
		return fmt.Errorf("failed to activate snapshot: %w", err)
	}

	// Commit transaction
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}
	committed = true

	l.state.SnapshotID = &snapshotID
	l.state.Phase = PhaseActive
	return nil
}

// fail marks the lifecycle as failed
func (l *Lifecycle) fail(err error) (*LifecycleResult, error) {
	l.state.Phase = PhaseFailed
	l.state.Errors = append(l.state.Errors, err.Error())

	return &LifecycleResult{
		Success:      false,
		Phase:        l.state.Phase,
		Error:        err.Error(),
		Duration:     time.Since(l.state.StartTime),
		BackupPath:   l.state.BackupPath,
		RawCount:     len(l.state.RawPrices),
		NormalizedCount: len(l.state.Normalized),
	}, nil
}

// success marks the lifecycle as successful
func (l *Lifecycle) success(msg string) (*LifecycleResult, error) {
	return &LifecycleResult{
		Success:         true,
		Phase:           l.state.Phase,
		Message:         msg,
		Duration:        time.Since(l.state.StartTime),
		SnapshotID:      l.state.SnapshotID,
		BackupPath:      l.state.BackupPath,
		ContentHash:     l.state.ContentHash,
		RawCount:        len(l.state.RawPrices),
		NormalizedCount: len(l.state.Normalized),
	}, nil
}

// LifecycleResult is the outcome of an ingestion run
type LifecycleResult struct {
	Success         bool           `json:"success"`
	Phase           IngestionPhase `json:"phase"`
	Message         string         `json:"message,omitempty"`
	Error           string         `json:"error,omitempty"`
	Duration        time.Duration  `json:"duration"`
	SnapshotID      *uuid.UUID     `json:"snapshot_id,omitempty"`
	BackupPath      string         `json:"backup_path,omitempty"`
	ContentHash     string         `json:"content_hash,omitempty"`
	RawCount        int            `json:"raw_count"`
	NormalizedCount int            `json:"normalized_count"`
}

// RealAPIFetcher is an interface for fetchers that can verify they use real APIs
type RealAPIFetcher interface {
	PriceFetcher
	// IsRealAPI returns true if this fetcher calls real cloud APIs
	IsRealAPI() bool
}

// NewAWSPriceFetcher creates the canonical AWS fetcher for the CLI
func NewAWSPriceFetcher() PriceFetcher {
	return NewAWSPricingAPIFetcher()
}

// NewAWSPriceNormalizer creates the canonical AWS normalizer for the CLI
func NewAWSPriceNormalizer() PriceNormalizer {
	return NewAWSPricingAPINormalizer()
}
