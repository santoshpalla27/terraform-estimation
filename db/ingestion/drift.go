// Package ingestion - Pricing drift detection
package ingestion

import (
	"context"
	"fmt"

	"terraform-cost/db"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// DriftDetector compares snapshots and identifies price changes
type DriftDetector struct {
	store                  db.PricingStore
	significanceThreshold  float64 // Percent change considered significant
}

// NewDriftDetector creates a new drift detector
func NewDriftDetector(store db.PricingStore) *DriftDetector {
	return &DriftDetector{
		store:                 store,
		significanceThreshold: 0.05, // 5% default
	}
}

// WithThreshold sets the significance threshold
func (d *DriftDetector) WithThreshold(pct float64) *DriftDetector {
	d.significanceThreshold = pct
	return d
}

// DriftRecord represents a single price change
type DriftRecord struct {
	Service        string
	ProductFamily  string
	DimensionKey   string
	DimensionValue string
	OldPrice       decimal.Decimal
	NewPrice       decimal.Decimal
	PriceDelta     decimal.Decimal
	PercentChange  float64
	Unit           string
	DriftType      DriftType
	IsSignificant  bool
}

// DriftType categorizes the type of drift
type DriftType string

const (
	DriftIncrease DriftType = "increase"
	DriftDecrease DriftType = "decrease"
	DriftNew      DriftType = "new"
	DriftRemoved  DriftType = "removed"
)

// DriftSummary summarizes drift between two snapshots
type DriftSummary struct {
	OldSnapshotID     uuid.UUID
	NewSnapshotID     uuid.UUID
	Cloud             db.CloudProvider
	TotalChanges      int
	PriceIncreases    int
	PriceDecreases    int
	NewRates          int
	RemovedRates      int
	AvgPercentChange  float64
	MaxPercentChange  float64
	SignificantChanges int
	Records           []DriftRecord
}

// DetectDrift compares two snapshots and returns drift summary
func (d *DriftDetector) DetectDrift(ctx context.Context, oldSnapshotID, newSnapshotID uuid.UUID) (*DriftSummary, error) {
	// Get snapshots
	oldSnapshot, err := d.store.GetSnapshot(ctx, oldSnapshotID)
	if err != nil {
		return nil, fmt.Errorf("failed to get old snapshot: %w", err)
	}
	if oldSnapshot == nil {
		return nil, fmt.Errorf("old snapshot not found: %s", oldSnapshotID)
	}

	newSnapshot, err := d.store.GetSnapshot(ctx, newSnapshotID)
	if err != nil {
		return nil, fmt.Errorf("failed to get new snapshot: %w", err)
	}
	if newSnapshot == nil {
		return nil, fmt.Errorf("new snapshot not found: %s", newSnapshotID)
	}

	// Verify same cloud/region
	if oldSnapshot.Cloud != newSnapshot.Cloud || oldSnapshot.Region != newSnapshot.Region {
		return nil, fmt.Errorf("snapshots must be for same cloud/region")
	}

	summary := &DriftSummary{
		OldSnapshotID: oldSnapshotID,
		NewSnapshotID: newSnapshotID,
		Cloud:         oldSnapshot.Cloud,
		Records:       make([]DriftRecord, 0),
	}

	// This would typically query the database for rate comparisons
	// For now, we'll create the structure for the comparison

	return summary, nil
}

// DetectDriftFromRates compares two sets of rates directly
func (d *DriftDetector) DetectDriftFromRates(oldRates, newRates []NormalizedRate) *DriftSummary {
	summary := &DriftSummary{
		Records: make([]DriftRecord, 0),
	}

	// Index old rates by key
	oldIndex := make(map[string]NormalizedRate)
	for _, r := range oldRates {
		key := rateKeyString(r.RateKey) + "|" + r.Unit
		oldIndex[key] = r
	}

	// Index new rates by key
	newIndex := make(map[string]NormalizedRate)
	for _, r := range newRates {
		key := rateKeyString(r.RateKey) + "|" + r.Unit
		newIndex[key] = r
	}

	// Find changes and additions
	for key, newRate := range newIndex {
		if oldRate, exists := oldIndex[key]; exists {
			// Compare prices
			if !oldRate.Price.Equal(newRate.Price) {
				record := d.createDriftRecord(oldRate, newRate)
				summary.Records = append(summary.Records, record)
				summary.TotalChanges++
				
				if record.IsSignificant {
					summary.SignificantChanges++
				}
				
				switch record.DriftType {
				case DriftIncrease:
					summary.PriceIncreases++
				case DriftDecrease:
					summary.PriceDecreases++
				}
			}
		} else {
			// New rate
			record := DriftRecord{
				Service:       newRate.RateKey.Service,
				ProductFamily: newRate.RateKey.ProductFamily,
				OldPrice:      decimal.Zero,
				NewPrice:      newRate.Price,
				PriceDelta:    newRate.Price,
				PercentChange: 100,
				Unit:          newRate.Unit,
				DriftType:     DriftNew,
				IsSignificant: true,
			}
			summary.Records = append(summary.Records, record)
			summary.TotalChanges++
			summary.NewRates++
			summary.SignificantChanges++
		}
	}

	// Find removals
	for key, oldRate := range oldIndex {
		if _, exists := newIndex[key]; !exists {
			record := DriftRecord{
				Service:       oldRate.RateKey.Service,
				ProductFamily: oldRate.RateKey.ProductFamily,
				OldPrice:      oldRate.Price,
				NewPrice:      decimal.Zero,
				PriceDelta:    oldRate.Price.Neg(),
				PercentChange: -100,
				Unit:          oldRate.Unit,
				DriftType:     DriftRemoved,
				IsSignificant: true,
			}
			summary.Records = append(summary.Records, record)
			summary.TotalChanges++
			summary.RemovedRates++
			summary.SignificantChanges++
		}
	}

	// Calculate averages
	if summary.TotalChanges > 0 {
		var totalPct float64
		var maxPct float64
		for _, r := range summary.Records {
			absPct := r.PercentChange
			if absPct < 0 {
				absPct = -absPct
			}
			totalPct += absPct
			if absPct > maxPct {
				maxPct = absPct
			}
		}
		summary.AvgPercentChange = totalPct / float64(summary.TotalChanges)
		summary.MaxPercentChange = maxPct
	}

	return summary
}

func (d *DriftDetector) createDriftRecord(oldRate, newRate NormalizedRate) DriftRecord {
	delta := newRate.Price.Sub(oldRate.Price)
	
	var pctChange float64
	if !oldRate.Price.IsZero() {
		pctChange = delta.Div(oldRate.Price).Mul(decimal.NewFromInt(100)).InexactFloat64()
	}

	driftType := DriftDecrease
	if delta.IsPositive() {
		driftType = DriftIncrease
	}

	isSignificant := false
	absPct := pctChange
	if absPct < 0 {
		absPct = -absPct
	}
	if absPct >= d.significanceThreshold*100 {
		isSignificant = true
	}

	return DriftRecord{
		Service:       newRate.RateKey.Service,
		ProductFamily: newRate.RateKey.ProductFamily,
		OldPrice:      oldRate.Price,
		NewPrice:      newRate.Price,
		PriceDelta:    delta,
		PercentChange: pctChange,
		Unit:          newRate.Unit,
		DriftType:     driftType,
		IsSignificant: isSignificant,
	}
}

// HasSignificantDrift returns true if there are significant price changes
func (s *DriftSummary) HasSignificantDrift() bool {
	return s.SignificantChanges > 0
}

// String returns a human-readable summary
func (s *DriftSummary) String() string {
	return fmt.Sprintf(
		"Pricing Drift: %d changes (%d significant) - %d increases, %d decreases, %d new, %d removed - avg %.2f%%, max %.2f%%",
		s.TotalChanges, s.SignificantChanges,
		s.PriceIncreases, s.PriceDecreases, s.NewRates, s.RemovedRates,
		s.AvgPercentChange, s.MaxPercentChange,
	)
}

// GetSignificantRecords returns only significant drift records
func (s *DriftSummary) GetSignificantRecords() []DriftRecord {
	var significant []DriftRecord
	for _, r := range s.Records {
		if r.IsSignificant {
			significant = append(significant, r)
		}
	}
	return significant
}

// GroupByService groups drift records by service
func (s *DriftSummary) GroupByService() map[string][]DriftRecord {
	byService := make(map[string][]DriftRecord)
	for _, r := range s.Records {
		byService[r.Service] = append(byService[r.Service], r)
	}
	return byService
}
