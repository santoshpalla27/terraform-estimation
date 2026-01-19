// Package ingestion - Snapshot coverage tracking
package ingestion

import (
	"fmt"

	"terraform-cost/db"

	"github.com/google/uuid"
)

// SnapshotCoverage tracks completeness of a snapshot
type SnapshotCoverage struct {
	SnapshotID      uuid.UUID
	Services        map[string]*ServiceCoverage
	TotalRates      int
	TotalDimensions int
	IsComplete      bool
	MinCoverage     float64
}

// ServiceCoverage tracks coverage for a single service
type ServiceCoverage struct {
	Service           string
	RateCount         int
	RequiredCount     int
	DimensionCount    int
	CoveragePercent   float64
	MissingDimensions []string
	IsComplete        bool
}

// CoverageTracker tracks and enforces snapshot completeness
type CoverageTracker struct {
	contracts  map[string]IngestionContract
	allowlists *DimensionAllowlist
}

// NewCoverageTracker creates a new coverage tracker
func NewCoverageTracker() *CoverageTracker {
	ct := &CoverageTracker{
		contracts:  make(map[string]IngestionContract),
		allowlists: NewDimensionAllowlist(),
	}
	for _, c := range DefaultContracts() {
		key := fmt.Sprintf("%s:%s", c.Cloud, c.Service)
		ct.contracts[key] = c
	}
	return ct
}

// CalculateCoverage calculates coverage for a set of rates
func (ct *CoverageTracker) CalculateCoverage(snapshotID uuid.UUID, cloud db.CloudProvider, rates []NormalizedRate) *SnapshotCoverage {
	coverage := &SnapshotCoverage{
		SnapshotID:  snapshotID,
		Services:    make(map[string]*ServiceCoverage),
		TotalRates:  len(rates),
		IsComplete:  true,
		MinCoverage: 100,
	}

	// Group rates by service
	byService := make(map[string][]NormalizedRate)
	for _, r := range rates {
		byService[r.RateKey.Service] = append(byService[r.RateKey.Service], r)
	}

	// Calculate coverage for each contracted service
	for key, contract := range ct.contracts {
		if contract.Cloud != cloud {
			continue
		}

		serviceRates := byService[contract.Service]
		sc := ct.calculateServiceCoverage(contract, serviceRates)
		coverage.Services[key] = sc

		if !sc.IsComplete {
			coverage.IsComplete = false
		}
		if sc.CoveragePercent < coverage.MinCoverage {
			coverage.MinCoverage = sc.CoveragePercent
		}
	}

	// Track total dimensions
	dims := make(map[string]bool)
	for _, r := range rates {
		for k := range r.RateKey.Attributes {
			dims[k] = true
		}
	}
	coverage.TotalDimensions = len(dims)

	return coverage
}

func (ct *CoverageTracker) calculateServiceCoverage(contract IngestionContract, rates []NormalizedRate) *ServiceCoverage {
	sc := &ServiceCoverage{
		Service:       contract.Service,
		RateCount:     len(rates),
		RequiredCount: contract.MinRateCount,
		IsComplete:    true,
	}

	// Calculate coverage percent
	if contract.MinRateCount > 0 {
		sc.CoveragePercent = float64(len(rates)) / float64(contract.MinRateCount) * 100
		if sc.CoveragePercent > 100 {
			sc.CoveragePercent = 100
		}
	} else {
		sc.CoveragePercent = 100
	}

	// Check rate count
	if len(rates) < contract.MinRateCount {
		sc.IsComplete = false
	}

	// Check required dimensions
	presentDims := make(map[string]bool)
	for _, r := range rates {
		for k := range r.RateKey.Attributes {
			presentDims[k] = true
		}
	}
	sc.DimensionCount = len(presentDims)

	for _, reqDim := range contract.RequiredDimensions {
		if !presentDims[reqDim] {
			sc.MissingDimensions = append(sc.MissingDimensions, reqDim)
			sc.IsComplete = false
		}
	}

	return sc
}

// EnforceCoverage checks if coverage meets minimum thresholds
func (ct *CoverageTracker) EnforceCoverage(coverage *SnapshotCoverage, minPercent float64) error {
	if coverage.MinCoverage < minPercent {
		return fmt.Errorf("snapshot coverage %.1f%% below minimum %.1f%%", 
			coverage.MinCoverage, minPercent)
	}

	var incompleteServices []string
	for key, sc := range coverage.Services {
		if !sc.IsComplete {
			incompleteServices = append(incompleteServices, key)
		}
	}

	if len(incompleteServices) > 0 {
		return fmt.Errorf("incomplete services: %v", incompleteServices)
	}

	return nil
}

// CoverageReport generates a human-readable coverage report
type CoverageReport struct {
	SnapshotID       uuid.UUID
	Cloud            db.CloudProvider
	Region           string
	TotalRates       int
	TotalDimensions  int
	OverallCoverage  float64
	IsComplete       bool
	ServiceReports   []ServiceReport
	MissingServices  []string
}

// ServiceReport is a per-service coverage summary
type ServiceReport struct {
	Service           string
	RateCount         int
	RequiredCount     int
	CoveragePercent   float64
	DimensionCount    int
	MissingDimensions []string
	Status            string // "complete", "partial", "missing"
}

// GenerateReport creates a detailed coverage report
func (ct *CoverageTracker) GenerateReport(snapshot *db.PricingSnapshot, rates []NormalizedRate) *CoverageReport {
	coverage := ct.CalculateCoverage(snapshot.ID, snapshot.Cloud, rates)

	report := &CoverageReport{
		SnapshotID:      snapshot.ID,
		Cloud:           snapshot.Cloud,
		Region:          snapshot.Region,
		TotalRates:      coverage.TotalRates,
		TotalDimensions: coverage.TotalDimensions,
		OverallCoverage: coverage.MinCoverage,
		IsComplete:      coverage.IsComplete,
		ServiceReports:  make([]ServiceReport, 0),
	}

	// Generate service reports
	for _, contract := range ct.contracts {
		if contract.Cloud != snapshot.Cloud {
			continue
		}

		key := fmt.Sprintf("%s:%s", contract.Cloud, contract.Service)
		sc, ok := coverage.Services[key]
		
		sr := ServiceReport{
			Service:       contract.Service,
			RequiredCount: contract.MinRateCount,
		}

		if !ok || sc.RateCount == 0 {
			sr.Status = "missing"
			report.MissingServices = append(report.MissingServices, contract.Service)
		} else {
			sr.RateCount = sc.RateCount
			sr.CoveragePercent = sc.CoveragePercent
			sr.DimensionCount = sc.DimensionCount
			sr.MissingDimensions = sc.MissingDimensions
			
			if sc.IsComplete {
				sr.Status = "complete"
			} else {
				sr.Status = "partial"
			}
		}

		report.ServiceReports = append(report.ServiceReports, sr)
	}

	return report
}

// String returns a summary string
func (r *CoverageReport) String() string {
	status := "INCOMPLETE"
	if r.IsComplete {
		status = "COMPLETE"
	}
	return fmt.Sprintf(
		"[%s] %s/%s: %d rates, %d dimensions, %.1f%% coverage (%s)",
		status, r.Cloud, r.Region,
		r.TotalRates, r.TotalDimensions, r.OverallCoverage,
		r.SnapshotID,
	)
}
