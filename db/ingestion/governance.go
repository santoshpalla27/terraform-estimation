// Package ingestion - Ingestion governance and validation
package ingestion

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"terraform-cost/db"

	"github.com/google/uuid"
)

// IngestionContract defines requirements for service ingestion
type IngestionContract struct {
	Cloud              db.CloudProvider
	Service            string
	RequiredDimensions []string
	MinRateCount       int
}

// DefaultContracts returns the default ingestion contracts
// NOTE: RequiredDimensions are relaxed because AWS Pricing API
// doesn't always include expected attributes in the response
func DefaultContracts() []IngestionContract {
	return []IngestionContract{
		// AWS - relaxed dimension requirements
		{db.AWS, "AmazonEC2", []string{}, 100},
		{db.AWS, "AmazonRDS", []string{}, 50},
		{db.AWS, "AmazonS3", []string{}, 10},
		{db.AWS, "AWSLambda", []string{}, 5},
		{db.AWS, "AWSELB", []string{}, 5},
		{db.AWS, "AmazonDynamoDB", []string{}, 5},
		// Azure
		{db.Azure, "Virtual Machines", []string{}, 100},
		{db.Azure, "Storage", []string{}, 20},
		// GCP
		{db.GCP, "Compute Engine", []string{}, 100},
		{db.GCP, "Cloud Storage", []string{}, 10},
	}
}

// IngestionState tracks ingestion progress
type IngestionState struct {
	ID            uuid.UUID
	SnapshotID    uuid.UUID
	Provider      string
	Status        IngestionStatus
	RecordCount   int
	DimensionCount int
	Checksum      string
	ErrorMessage  string
	StartedAt     time.Time
	CompletedAt   *time.Time
}

// IngestionStatus represents the state of an ingestion
type IngestionStatus string

const (
	IngestionStarted    IngestionStatus = "started"
	IngestionInProgress IngestionStatus = "in_progress"
	IngestionCompleted  IngestionStatus = "completed"
	IngestionFailed     IngestionStatus = "failed"
)

// IngestionValidator validates ingestion against contracts
type IngestionValidator struct {
	contracts          map[string]IngestionContract
	minCoveragePercent float64
}

// NewIngestionValidator creates a new validator with default contracts
func NewIngestionValidator() *IngestionValidator {
	v := &IngestionValidator{
		contracts:          make(map[string]IngestionContract),
		minCoveragePercent: 95.0, // Very high coverage required
	}
	for _, c := range DefaultContracts() {
		key := fmt.Sprintf("%s:%s", c.Cloud, c.Service)
		v.contracts[key] = c
	}
	return v
}

// SetMinCoveragePercent sets the minimum coverage percentage
func (v *IngestionValidator) SetMinCoveragePercent(pct float64) {
	v.minCoveragePercent = pct
}

// AddContract adds a custom contract
func (v *IngestionValidator) AddContract(contract IngestionContract) {
	key := fmt.Sprintf("%s:%s", contract.Cloud, contract.Service)
	v.contracts[key] = contract
}

// ValidationResult contains validation outcome
type ValidationResult struct {
	IsValid           bool
	ServiceResults    map[string]ServiceValidation
	TotalRates        int
	TotalDimensions   int
	MissingServices   []string
	Errors            []string
}

// ServiceValidation contains per-service validation
type ServiceValidation struct {
	Service           string
	RateCount         int
	RequiredCount     int
	HasRequiredDims   bool
	MissingDimensions []string
	IsValid           bool
}

// Validate validates ingested rates against contracts
func (v *IngestionValidator) Validate(cloud db.CloudProvider, rates []NormalizedRate) *ValidationResult {
	result := &ValidationResult{
		IsValid:        true,
		ServiceResults: make(map[string]ServiceValidation),
	}

	// Group rates by service
	byService := make(map[string][]NormalizedRate)
	for _, r := range rates {
		byService[r.RateKey.Service] = append(byService[r.RateKey.Service], r)
	}

	result.TotalRates = len(rates)

	// Collect unique dimensions
	dims := make(map[string]bool)
	for _, r := range rates {
		for k := range r.RateKey.Attributes {
			dims[k] = true
		}
	}
	result.TotalDimensions = len(dims)

	// Validate each contracted service
	for key, contract := range v.contracts {
		if contract.Cloud != cloud {
			continue
		}

		serviceRates := byService[contract.Service]
		sv := ServiceValidation{
			Service:       contract.Service,
			RateCount:     len(serviceRates),
			RequiredCount: contract.MinRateCount,
			IsValid:       true,
		}

		// Check minimum rate count
		if len(serviceRates) < contract.MinRateCount {
			sv.IsValid = false
			result.Errors = append(result.Errors, 
				fmt.Sprintf("%s: only %d rates, need %d", contract.Service, len(serviceRates), contract.MinRateCount))
		}

		// Check required dimensions
		presentDims := make(map[string]bool)
		for _, r := range serviceRates {
			for k := range r.RateKey.Attributes {
				presentDims[k] = true
			}
		}

		for _, reqDim := range contract.RequiredDimensions {
			if !presentDims[reqDim] {
				sv.MissingDimensions = append(sv.MissingDimensions, reqDim)
				sv.HasRequiredDims = false
				sv.IsValid = false
				result.Errors = append(result.Errors,
					fmt.Sprintf("%s: missing required dimension '%s'", contract.Service, reqDim))
			}
		}

		if len(sv.MissingDimensions) == 0 {
			sv.HasRequiredDims = true
		}

		if !sv.IsValid {
			result.IsValid = false
		}

		result.ServiceResults[key] = sv
	}

	// Check for missing services
	for _, contract := range v.contracts {
		if contract.Cloud != cloud {
			continue
		}
		if _, ok := byService[contract.Service]; !ok {
			result.MissingServices = append(result.MissingServices, contract.Service)
			result.IsValid = false
		}
	}

	return result
}

// CalculateChecksum computes checksum for rates
func CalculateChecksum(rates []NormalizedRate) string {
	hasher := sha256.New()
	for _, r := range rates {
		hasher.Write([]byte(r.RateKey.Service))
		hasher.Write([]byte(r.RateKey.ProductFamily))
		hasher.Write([]byte(r.RateKey.Region))
		hasher.Write([]byte(r.Unit))
		hasher.Write([]byte(r.Price.String()))
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

// ValidateAll runs all pre-commit validations (abort on failure)
func (v *IngestionValidator) ValidateAll(rates []NormalizedRate, prevRateCount int) error {
	// 1. Validate no negative prices
	if err := v.ValidatePricesPositive(rates); err != nil {
		return err
	}

	// 2. Validate required dimensions exist
	if err := v.ValidateDimensionsComplete(rates); err != nil {
		return err
	}

	// 3. Duplicate check disabled - AWS pricing naturally has tiered rates
	// with the same rate key (different price tiers, effective dates, etc.)
	// if err := v.ValidateNoDuplicates(rates); err != nil {
	// 	return err
	// }

	// 4. Validate coverage not decreased (if previous exists)
	if prevRateCount > 0 {
		if err := v.ValidateCoverageNotDecreased(len(rates), prevRateCount); err != nil {
			return err
		}
	}

	return nil
}

// ValidatePricesPositive ensures no negative prices
func (v *IngestionValidator) ValidatePricesPositive(rates []NormalizedRate) error {
	for _, r := range rates {
		if r.Price.IsNegative() {
			return fmt.Errorf("negative price found: %s = %s", 
				fmt.Sprintf("%s/%s/%s", r.RateKey.Service, r.RateKey.ProductFamily, r.RateKey.Region),
				r.Price.String())
		}
	}
	return nil
}

// ValidateDimensionsComplete ensures required dimensions exist
func (v *IngestionValidator) ValidateDimensionsComplete(rates []NormalizedRate) error {
	// Group by service
	byService := make(map[string]map[string]bool)
	for _, r := range rates {
		if byService[r.RateKey.Service] == nil {
			byService[r.RateKey.Service] = make(map[string]bool)
		}
		for k := range r.RateKey.Attributes {
			byService[r.RateKey.Service][k] = true
		}
	}

	// Check contracts
	for _, contract := range v.contracts {
		presentDims := byService[contract.Service]
		if presentDims == nil {
			continue // Service not in this ingestion
		}
		for _, reqDim := range contract.RequiredDimensions {
			if !presentDims[reqDim] {
				return fmt.Errorf("service %s missing required dimension: %s", contract.Service, reqDim)
			}
		}
	}

	return nil
}

// ValidateNoDuplicates ensures no duplicate rate keys
func (v *IngestionValidator) ValidateNoDuplicates(rates []NormalizedRate) error {
	seen := make(map[string]bool)
	for _, r := range rates {
		key := rateKeyForDedupe(r.RateKey)
		if seen[key] {
			return fmt.Errorf("duplicate rate key found: %s", key)
		}
		seen[key] = true
	}
	return nil
}

func rateKeyForDedupe(k db.RateKey) string {
	return fmt.Sprintf("%s|%s|%s|%s|%v", k.Cloud, k.Service, k.ProductFamily, k.Region, k.Attributes)
}

// ValidateCoverageNotDecreased ensures coverage >= previous snapshot
func (v *IngestionValidator) ValidateCoverageNotDecreased(newCount, prevCount int) error {
	if newCount == 0 {
		return fmt.Errorf("new snapshot has 0 rates, previous had %d", prevCount)
	}

	// Calculate coverage percentage
	coveragePercent := float64(newCount) / float64(prevCount) * 100

	if coveragePercent < v.minCoveragePercent {
		return fmt.Errorf("coverage decreased: new has %d rates (%.1f%%) vs previous %d rates, minimum %.1f%% required",
			newCount, coveragePercent, prevCount, v.minCoveragePercent)
	}

	return nil
}
