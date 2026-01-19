// Package ingestion - Pipeline tests
package ingestion

import (
	"testing"

	"terraform-cost/db"

	"github.com/shopspring/decimal"
)

func TestValidatePricesPositive(t *testing.T) {
	validator := NewIngestionValidator()

	// Valid prices
	validRates := []NormalizedRate{
		{Price: decimal.NewFromFloat(0.10)},
		{Price: decimal.NewFromFloat(0.0)},
		{Price: decimal.NewFromFloat(100.00)},
	}
	if err := validator.ValidatePricesPositive(validRates); err != nil {
		t.Errorf("expected valid prices to pass, got: %v", err)
	}

	// Invalid negative price
	invalidRates := []NormalizedRate{
		{Price: decimal.NewFromFloat(-0.10)},
	}
	if err := validator.ValidatePricesPositive(invalidRates); err == nil {
		t.Error("expected negative price to fail validation")
	}
}

func TestValidateNoDuplicates(t *testing.T) {
	validator := NewIngestionValidator()

	// Unique rates
	uniqueRates := []NormalizedRate{
		{RateKey: db.RateKey{Cloud: db.AWS, Service: "EC2", Region: "us-east-1", Attributes: map[string]string{"type": "t3.micro"}}},
		{RateKey: db.RateKey{Cloud: db.AWS, Service: "EC2", Region: "us-east-1", Attributes: map[string]string{"type": "t3.small"}}},
	}
	if err := validator.ValidateNoDuplicates(uniqueRates); err != nil {
		t.Errorf("expected unique rates to pass, got: %v", err)
	}

	// Duplicate rates
	duplicateRates := []NormalizedRate{
		{RateKey: db.RateKey{Cloud: db.AWS, Service: "EC2", Region: "us-east-1", Attributes: map[string]string{"type": "t3.micro"}}},
		{RateKey: db.RateKey{Cloud: db.AWS, Service: "EC2", Region: "us-east-1", Attributes: map[string]string{"type": "t3.micro"}}},
	}
	if err := validator.ValidateNoDuplicates(duplicateRates); err == nil {
		t.Error("expected duplicate rates to fail validation")
	}
}

func TestValidateCoverageNotDecreased(t *testing.T) {
	validator := NewIngestionValidator()
	validator.SetMinCoveragePercent(95.0)

	// Coverage maintained
	if err := validator.ValidateCoverageNotDecreased(100, 100); err != nil {
		t.Errorf("expected same coverage to pass, got: %v", err)
	}

	// Coverage increased
	if err := validator.ValidateCoverageNotDecreased(110, 100); err != nil {
		t.Errorf("expected increased coverage to pass, got: %v", err)
	}

	// Coverage slightly decreased (still above 95%)
	if err := validator.ValidateCoverageNotDecreased(96, 100); err != nil {
		t.Errorf("expected 96%% coverage to pass, got: %v", err)
	}

	// Coverage decreased below threshold
	if err := validator.ValidateCoverageNotDecreased(90, 100); err == nil {
		t.Error("expected 90%% coverage to fail validation")
	}
}

func TestBackupWriteRead(t *testing.T) {
	backup := &SnapshotBackup{
		Provider:      db.AWS,
		Region:        "us-east-1",
		Alias:         "default",
		ContentHash:   "abc123",
		RateCount:     2,
		SchemaVersion: "1.0",
		Rates: []NormalizedRate{
			{
				RateKey: db.RateKey{
					Cloud:   db.AWS,
					Service: "EC2",
					Region:  "us-east-1",
				},
				Price: decimal.NewFromFloat(0.10),
			},
			{
				RateKey: db.RateKey{
					Cloud:   db.AWS,
					Service: "RDS",
					Region:  "us-east-1",
				},
				Price: decimal.NewFromFloat(0.20),
			},
		},
	}

	// Recalculate hash to match
	backup.ContentHash = calculateHash(backup.Rates)

	mgr := NewBackupManager()
	
	// Write to temp directory
	path, err := mgr.WriteBackup(t.TempDir(), backup)
	if err != nil {
		t.Fatalf("failed to write backup: %v", err)
	}

	// Read back
	readBackup, err := mgr.ReadBackup(path)
	if err != nil {
		t.Fatalf("failed to read backup: %v", err)
	}

	// Verify
	if readBackup.Provider != backup.Provider {
		t.Errorf("provider mismatch: got %s, want %s", readBackup.Provider, backup.Provider)
	}
	if readBackup.RateCount != backup.RateCount {
		t.Errorf("rate count mismatch: got %d, want %d", readBackup.RateCount, backup.RateCount)
	}
	if len(readBackup.Rates) != len(backup.Rates) {
		t.Errorf("rates length mismatch: got %d, want %d", len(readBackup.Rates), len(backup.Rates))
	}
}

func TestPipelineResult(t *testing.T) {
	// Verify PipelineResult structure
	result := &PipelineResult{
		Success:         true,
		PhasesCompleted: []Phase{PhaseFetch, PhaseNormalize, PhaseValidate, PhaseBackup, PhaseCommit},
		Stats: PipelineStats{
			RawPricesCount:       100,
			NormalizedRatesCount: 95,
			UniqueServicesCount:  5,
		},
	}

	if !result.Success {
		t.Error("expected success")
	}
	if len(result.PhasesCompleted) != 5 {
		t.Errorf("expected 5 phases, got %d", len(result.PhasesCompleted))
	}
}
