// Package ingestion - Streaming pricing pipeline for low-memory environments
// Designed to run on servers with 4-8GB RAM
package ingestion

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"terraform-cost/db"

	"github.com/google/uuid"
)

// StreamingConfig configures the streaming pipeline for low-memory environments
type StreamingConfig struct {
	// BatchSize is the number of prices to process before writing to disk
	// Lower = less memory, slower. Higher = more memory, faster.
	// Default: 10000 (good for 4GB RAM)
	BatchSize int

	// MaxMemoryMB is the soft memory limit in megabytes
	// Pipeline will pause and flush when approaching this limit
	// Default: 2048 (2GB, safe for 4GB server)
	MaxMemoryMB int

	// WorkDir is where temporary files are stored during processing
	// Default: system temp directory
	WorkDir string

	// ConcurrentFetches is the number of parallel service fetches
	// Lower = less memory, slower. Default: 2 for low-memory
	ConcurrentFetches int

	// EnableCheckpointing allows resuming interrupted ingestion
	EnableCheckpointing bool

	// GCInterval is how often to force garbage collection (in batches)
	// Default: 5 (every 5 batches)
	GCInterval int
}

// DefaultStreamingConfig returns configuration safe for 4GB RAM servers
func DefaultStreamingConfig() *StreamingConfig {
	return &StreamingConfig{
		BatchSize:           10000,
		MaxMemoryMB:         2048,
		WorkDir:             os.TempDir(),
		ConcurrentFetches:   2,
		EnableCheckpointing: true,
		GCInterval:          5,
	}
}

// LowMemoryConfig returns configuration for minimal memory usage (4GB)
func LowMemoryConfig() *StreamingConfig {
	return &StreamingConfig{
		BatchSize:           5000,
		MaxMemoryMB:         1024,
		WorkDir:             os.TempDir(),
		ConcurrentFetches:   1,
		EnableCheckpointing: true,
		GCInterval:          3,
	}
}

// HighMemoryConfig returns configuration for 16GB+ servers
func HighMemoryConfig() *StreamingConfig {
	return &StreamingConfig{
		BatchSize:           50000,
		MaxMemoryMB:         8192,
		WorkDir:             os.TempDir(),
		ConcurrentFetches:   4,
		EnableCheckpointing: true,
		GCInterval:          10,
	}
}

// StreamingLifecycle manages memory-efficient ingestion
type StreamingLifecycle struct {
	mu          sync.Mutex
	config      *StreamingConfig
	lcConfig    *LifecycleConfig
	fetcher     PriceFetcher
	normalizer  PriceNormalizer
	store       db.PricingStore
	
	// Progress tracking
	totalFetched    int
	totalNormalized int
	totalWritten    int
	batchCount      int
	
	// Temporary storage
	tempFiles   []string
	checkpoint  *IngestionCheckpoint
}

// IngestionCheckpoint tracks progress for resumable ingestion
type IngestionCheckpoint struct {
	Provider       db.CloudProvider `json:"provider"`
	Region         string           `json:"region"`
	StartedAt      time.Time        `json:"started_at"`
	CompletedServices []string      `json:"completed_services"`
	TotalPrices    int              `json:"total_prices"`
	TempFiles      []string         `json:"temp_files"`
}

// NewStreamingLifecycle creates a memory-efficient lifecycle
func NewStreamingLifecycle(
	fetcher PriceFetcher,
	normalizer PriceNormalizer,
	store db.PricingStore,
	streamConfig *StreamingConfig,
) *StreamingLifecycle {
	if streamConfig == nil {
		streamConfig = DefaultStreamingConfig()
	}

	return &StreamingLifecycle{
		config:     streamConfig,
		fetcher:    fetcher,
		normalizer: normalizer,
		store:      store,
	}
}

// Execute runs the streaming ingestion pipeline
func (s *StreamingLifecycle) Execute(ctx context.Context, config *LifecycleConfig) (*LifecycleResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if config == nil {
		config = DefaultLifecycleConfig()
	}
	s.lcConfig = config

	startTime := time.Now()
	s.logProgress("STARTING", "Initializing streaming ingestion lifecycle...")

	// Check for existing checkpoint
	if s.config.EnableCheckpointing {
		if err := s.loadCheckpoint(); err == nil {
			s.logProgress("CHECKPOINT", "Resuming from previous checkpoint")
		}
	}

	// Apply timeout
	if config.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, config.Timeout)
		defer cancel()
	}

	s.logProgress("CONFIG", fmt.Sprintf("batch=%d, maxMem=%dMB, concurrency=%d",
		s.config.BatchSize, s.config.MaxMemoryMB, s.config.ConcurrentFetches))

	// Phase 1: Stream fetch and normalize to temp files
	s.logPhaseStart(1, 4, "FETCH & NORMALIZE", "Fetching pricing from cloud APIs...")
	if err := s.streamFetchAndNormalize(ctx); err != nil {
		s.cleanup()
		return s.fail(err, startTime)
	}
	s.logPhaseComplete(1, 4, "FETCH & NORMALIZE", fmt.Sprintf("Fetched %d raw prices", s.totalFetched))

	// Phase 2: Merge and validate
	s.logPhaseStart(2, 4, "MERGE & VALIDATE", "Merging temp files and validating...")
	allRates, err := s.mergeAndValidate(ctx)
	if err != nil {
		s.cleanup()
		return s.fail(err, startTime)
	}
	s.logPhaseComplete(2, 4, "MERGE & VALIDATE", fmt.Sprintf("Validated %d normalized rates", len(allRates)))

	// Phase 3: Backup
	s.logPhaseStart(3, 4, "BACKUP", "Writing backup file...")
	backupPath, err := s.writeBackup(allRates)
	if err != nil {
		s.cleanup()
		return s.fail(fmt.Errorf("backup failed: %w", err), startTime)
	}
	s.logPhaseComplete(3, 4, "BACKUP", fmt.Sprintf("Backup saved to %s", backupPath))

	// Phase 4: Commit (if not dry-run)
	var snapshotID *uuid.UUID
	if !config.DryRun {
		s.logPhaseStart(4, 4, "COMMIT", "Committing to database...")
		sid, err := s.streamCommit(ctx, allRates)
		if err != nil {
			s.cleanup()
			return s.fail(fmt.Errorf("commit failed: %w", err), startTime)
		}
		snapshotID = &sid
		s.logPhaseComplete(4, 4, "COMMIT", fmt.Sprintf("Snapshot %s activated", sid))
	} else {
		s.logProgress("DRY-RUN", "Skipping database commit (dry-run mode)")
	}

	// Cleanup
	s.cleanup()
	s.deleteCheckpoint()

	s.logProgress("COMPLETE", fmt.Sprintf("Ingestion finished in %s", time.Since(startTime).Round(time.Second)))

	return &LifecycleResult{
		Success:         true,
		Phase:           PhaseActive,
		Message:         "streaming ingestion complete",
		Duration:        time.Since(startTime),
		SnapshotID:      snapshotID,
		BackupPath:      backupPath,
		ContentHash:     calculateHash(allRates),
		RawCount:        s.totalFetched,
		NormalizedCount: len(allRates),
	}, nil
}

// streamFetchAndNormalize fetches pricing in batches and writes to temp files
func (s *StreamingLifecycle) streamFetchAndNormalize(ctx context.Context) error {

	s.logProgress("FETCHING", "Fetching all pricing data from cloud API...")
	
	// Fetch ALL prices once (not per-service to avoid duplication)
	rawPrices, err := s.fetcher.FetchRegion(ctx, s.lcConfig.Region)
	if err != nil {
		return fmt.Errorf("failed to fetch pricing: %w", err)
	}
	
	totalPrices := len(rawPrices)
	s.logProgress("FETCHED", fmt.Sprintf("Retrieved %d raw prices", totalPrices))
	
	// Create temp file for normalized rates
	tempFile := filepath.Join(s.config.WorkDir, fmt.Sprintf("pricing_%s_%s_%d.jsonl.gz",
		s.lcConfig.Provider, s.lcConfig.Region, time.Now().UnixNano()))

	f, err := os.Create(tempFile)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer f.Close()

	gzw := gzip.NewWriter(f)
	defer gzw.Close()

	writer := bufio.NewWriter(gzw)
	
	s.logProgress("NORMALIZING", fmt.Sprintf("Processing %d prices in batches of %d...", totalPrices, s.config.BatchSize))

	// Process in batches to control memory
	batchNum := 0
	for i := 0; i < len(rawPrices); i += s.config.BatchSize {
		end := i + s.config.BatchSize
		if end > len(rawPrices) {
			end = len(rawPrices)
		}

		batch := rawPrices[i:end]

		// Normalize batch
		normalized, err := s.normalizer.Normalize(batch)
		if err != nil {
			s.logProgress("WARNING", fmt.Sprintf("Batch %d normalization error: %v", batchNum, err))
			continue
		}

		// Write to temp file (JSON Lines format)
		for _, rate := range normalized {
			data, err := json.Marshal(rate)
			if err != nil {
				continue
			}
			writer.Write(data)
			writer.WriteString("\n")
			s.totalNormalized++
		}

		s.totalFetched += len(batch)
		batchNum++

		// Progress update
		progress := float64(i+len(batch)) / float64(totalPrices) * 100
		s.logProgress("PROCESSING", fmt.Sprintf("%s %d/%d prices (%.1f%%)", s.progressBar(progress), i+len(batch), totalPrices, progress))

		// Memory management - flush and GC
		if batchNum%s.config.GCInterval == 0 {
			writer.Flush()
			s.checkMemoryAndGC()
		}
	}

	writer.Flush()
	
	// Clear raw prices from memory
	rawPrices = nil
	runtime.GC()
	
	s.tempFiles = append(s.tempFiles, tempFile)
	s.logProgress("NORMALIZED", fmt.Sprintf("Written %d normalized rates to temp file", s.totalNormalized))

	return nil
}

// fetchServicePricing fetches pricing for a service (uses streaming internally)
func (s *StreamingLifecycle) fetchServicePricing(ctx context.Context, service string) ([]RawPrice, error) {
	// For now, delegate to fetcher
	// In future, implement streaming JSON parsing for very large responses
	return s.fetcher.FetchRegion(ctx, s.lcConfig.Region)
}

// mergeAndValidate reads temp files and validates
func (s *StreamingLifecycle) mergeAndValidate(ctx context.Context) ([]NormalizedRate, error) {
	var allRates []NormalizedRate

	totalFiles := len(s.tempFiles)
	for i, tempFile := range s.tempFiles {
		s.logProgress("READING", fmt.Sprintf("[%d/%d] Processing temp file...", i+1, totalFiles))
		rates, err := s.readTempFile(tempFile)
		if err != nil {
			s.logProgress("WARNING", fmt.Sprintf("Failed to read temp file: %v", err))
			continue
		}
		allRates = append(allRates, rates...)
		s.logProgress("MERGED", fmt.Sprintf("Loaded %d rates (total: %d)", len(rates), len(allRates)))
	}

	s.logProgress("VALIDATING", fmt.Sprintf("Validating %d total rates...", len(allRates)))

	// Validate
	validator := NewIngestionValidator()
	validator.SetMinCoveragePercent(s.lcConfig.MinCoverage)

	if err := validator.ValidateAll(allRates, 0); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	return allRates, nil
}

// readTempFile reads a gzipped JSON Lines temp file
func (s *StreamingLifecycle) readTempFile(path string) ([]NormalizedRate, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	gzr, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gzr.Close()

	var rates []NormalizedRate
	scanner := bufio.NewScanner(gzr)
	
	// Increase buffer size for large lines
	buf := make([]byte, 1024*1024) // 1MB buffer
	scanner.Buffer(buf, len(buf))

	for scanner.Scan() {
		var rate NormalizedRate
		if err := json.Unmarshal(scanner.Bytes(), &rate); err != nil {
			continue
		}
		rates = append(rates, rate)
	}

	return rates, scanner.Err()
}

// writeBackup writes the final backup
func (s *StreamingLifecycle) writeBackup(rates []NormalizedRate) (string, error) {
	fmt.Println("\nPhase 3: Write backup...")

	backup := &SnapshotBackup{
		Provider:      s.lcConfig.Provider,
		Region:        s.lcConfig.Region,
		Alias:         s.lcConfig.Alias,
		Timestamp:     time.Now(),
		ContentHash:   calculateHash(rates),
		RateCount:     len(rates),
		SchemaVersion: "1.0",
		Rates:         rates,
	}

	backupMgr := NewBackupManager()
	path, err := backupMgr.WriteBackup(s.lcConfig.BackupDir, backup)
	if err != nil {
		return "", err
	}

	fmt.Printf("  ✓ Backup written: %s\n", path)
	return path, nil
}

// streamCommit commits rates in batches to reduce memory
func (s *StreamingLifecycle) streamCommit(ctx context.Context, rates []NormalizedRate) (uuid.UUID, error) {
	totalRates := len(rates)
	s.logProgress("COMMIT", fmt.Sprintf("Starting database commit of %d rates...", totalRates))

	snapshotID := uuid.New()
	snapshot := &db.PricingSnapshot{
		ID:            snapshotID,
		Cloud:         s.lcConfig.Provider,
		Region:        s.lcConfig.Region,
		ProviderAlias: s.lcConfig.Alias,
		Source:        "streaming_ingestion",
		FetchedAt:     time.Now(),
		ValidFrom:     time.Now(),
		Hash:          calculateHash(rates),
		Version:       "1.0",
		IsActive:      false,
	}

	tx, err := s.store.BeginTx(ctx)
	if err != nil {
		return uuid.Nil, err
	}

	committed := false
	defer func() {
		if !committed {
			tx.Rollback()
		}
	}()

	if err = tx.CreateSnapshot(ctx, snapshot); err != nil {
		return uuid.Nil, err
	}

	// Commit in batches
	batchSize := s.config.BatchSize
	for i := 0; i < len(rates); i += batchSize {
		end := i + batchSize
		if end > len(rates) {
			end = len(rates)
		}

		for _, nr := range rates[i:end] {
			nr.RateKey.ID = uuid.New()
			key, err := tx.UpsertRateKey(ctx, &nr.RateKey)
			if err != nil {
				return uuid.Nil, err
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
				return uuid.Nil, err
			}
		}

		s.totalWritten += (end - i)
		progress := float64(s.totalWritten) / float64(len(rates)) * 100
		s.logProgress("WRITING", fmt.Sprintf("%s %d/%d rates (%.1f%%)", s.progressBar(progress), s.totalWritten, len(rates), progress))

		// GC between batches
		if (i/batchSize)%s.config.GCInterval == 0 {
			s.checkMemoryAndGC()
		}
	}

	if err = tx.ActivateSnapshot(ctx, snapshotID); err != nil {
		return uuid.Nil, err
	}

	if err = tx.Commit(); err != nil {
		return uuid.Nil, err
	}
	committed = true

	fmt.Printf("  ✓ Snapshot activated: %s\n", snapshotID)
	return snapshotID, nil
}

// checkMemoryAndGC checks memory usage and triggers GC if needed
func (s *StreamingLifecycle) checkMemoryAndGC() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	usedMB := m.Alloc / 1024 / 1024
	if int(usedMB) > s.config.MaxMemoryMB*80/100 { // 80% threshold
		s.logProgress("MEMORY", fmt.Sprintf("Usage: %dMB (threshold: %dMB) - triggering GC", usedMB, s.config.MaxMemoryMB))
		runtime.GC()
	}
}

// logProgress prints a timestamped progress message
func (s *StreamingLifecycle) logProgress(stage, message string) {
	timestamp := time.Now().Format("15:04:05")
	fmt.Printf("[%s] %-12s │ %s\n", timestamp, stage, message)
}

// logPhaseStart prints a phase start banner
func (s *StreamingLifecycle) logPhaseStart(current, total int, name, description string) {
	timestamp := time.Now().Format("15:04:05")
	fmt.Printf("\n[%s] ══════════════════════════════════════════════════════════\n", timestamp)
	fmt.Printf("[%s] PHASE %d/%d: %s\n", timestamp, current, total, name)
	fmt.Printf("[%s] %s\n", timestamp, description)
	fmt.Printf("[%s] ══════════════════════════════════════════════════════════\n", timestamp)
}

// logPhaseComplete prints a phase completion message
func (s *StreamingLifecycle) logPhaseComplete(current, total int, name, result string) {
	timestamp := time.Now().Format("15:04:05")
	fmt.Printf("[%s] ✓ PHASE %d/%d COMPLETE: %s\n", timestamp, current, total, result)
}

// progressBar generates a progress bar string
func (s *StreamingLifecycle) progressBar(percent float64) string {
	const width = 20
	filled := int(percent / 100 * width)
	if filled > width {
		filled = width
	}
	empty := width - filled
	return "[" + repeatChar('█', filled) + repeatChar('░', empty) + "]"
}

func repeatChar(ch rune, count int) string {
	result := make([]rune, count)
	for i := range result {
		result[i] = ch
	}
	return string(result)
}

// cleanup removes temp files
func (s *StreamingLifecycle) cleanup() {
	for _, f := range s.tempFiles {
		os.Remove(f)
	}
	s.tempFiles = nil
}

// Checkpoint management
func (s *StreamingLifecycle) loadCheckpoint() error {
	path := s.checkpointPath()
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &s.checkpoint)
}

func (s *StreamingLifecycle) saveCheckpoint() error {
	if s.checkpoint == nil {
		s.checkpoint = &IngestionCheckpoint{
			Provider:  s.lcConfig.Provider,
			Region:    s.lcConfig.Region,
			StartedAt: time.Now(),
		}
	}
	s.checkpoint.TempFiles = s.tempFiles
	s.checkpoint.TotalPrices = s.totalFetched

	data, err := json.Marshal(s.checkpoint)
	if err != nil {
		return err
	}
	return os.WriteFile(s.checkpointPath(), data, 0644)
}

func (s *StreamingLifecycle) deleteCheckpoint() {
	os.Remove(s.checkpointPath())
}

func (s *StreamingLifecycle) checkpointPath() string {
	return filepath.Join(s.config.WorkDir, fmt.Sprintf("checkpoint_%s_%s.json",
		s.lcConfig.Provider, s.lcConfig.Region))
}

func (s *StreamingLifecycle) isServiceCompleted(service string) bool {
	if s.checkpoint == nil {
		return false
	}
	for _, completed := range s.checkpoint.CompletedServices {
		if completed == service {
			return true
		}
	}
	return false
}

func (s *StreamingLifecycle) markServiceCompleted(service string) {
	if s.checkpoint == nil {
		s.checkpoint = &IngestionCheckpoint{}
	}
	s.checkpoint.CompletedServices = append(s.checkpoint.CompletedServices, service)
	s.saveCheckpoint()
}

func (s *StreamingLifecycle) fail(err error, startTime time.Time) (*LifecycleResult, error) {
	return &LifecycleResult{
		Success:         false,
		Phase:           PhaseFailed,
		Error:           err.Error(),
		Duration:        time.Since(startTime),
		RawCount:        s.totalFetched,
		NormalizedCount: s.totalNormalized,
	}, nil
}
