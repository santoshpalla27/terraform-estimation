// Package db - Strict resolver with coverage integration
package db

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// StrictMode defines resolver behavior
type StrictMode int

const (
	// Permissive - missing rates become symbolic
	Permissive StrictMode = iota
	// Strict - missing rates cause errors
	Strict
)

// StrictResolver provides strict-mode pricing resolution
type StrictResolver struct {
	store        PricingStore
	defaultAlias string
	mode         StrictMode
	usedSnapshot map[string]uuid.UUID // Track snapshots used for auditability
}

// NewStrictResolver creates a new strict resolver
func NewStrictResolver(store PricingStore) *StrictResolver {
	return &StrictResolver{
		store:        store,
		defaultAlias: "default",
		mode:         Permissive,
		usedSnapshot: make(map[string]uuid.UUID),
	}
}

// WithMode sets the strict mode
func (r *StrictResolver) WithMode(mode StrictMode) *StrictResolver {
	r.mode = mode
	return r
}

// WithAlias sets the default provider alias
func (r *StrictResolver) WithAlias(alias string) *StrictResolver {
	r.defaultAlias = alias
	return r
}

// ResolutionRequest contains rate resolution parameters
type ResolutionRequest struct {
	Cloud         CloudProvider
	Service       string
	ProductFamily string
	Region        string
	Attributes    map[string]string
	Unit          string
	Alias         string
}

// ResolutionResult contains resolution outcome
type ResolutionResult struct {
	// Rate info (nil if symbolic)
	Price      *decimal.Decimal
	Currency   string
	Confidence float64
	SnapshotID uuid.UUID
	Source     string
	
	// Symbolic info
	IsSymbolic bool
	Reason     string
}

// Resolve resolves a rate with strict mode enforcement
func (r *StrictResolver) Resolve(ctx context.Context, req ResolutionRequest) (*ResolutionResult, error) {
	alias := req.Alias
	if alias == "" {
		alias = r.defaultAlias
	}
	
	// 1. Validate provider alias (MANDATORY)
	if alias == "" {
		return nil, fmt.Errorf("provider alias cannot be empty")
	}
	
	// 2. Get active snapshot (MANDATORY - fails in both modes)
	snapshot, err := r.store.GetActiveSnapshot(ctx, req.Cloud, req.Region, alias)
	if err != nil {
		return nil, fmt.Errorf("snapshot lookup failed: %w", err)
	}
	if snapshot == nil {
		// Alias mismatch or missing snapshot - always fail
		return nil, fmt.Errorf("no active pricing snapshot for %s/%s/%s", req.Cloud, req.Region, alias)
	}
	
	// Track snapshot for audit
	key := fmt.Sprintf("%s:%s:%s", req.Cloud, req.Region, alias)
	r.usedSnapshot[key] = snapshot.ID
	
	// 3. Resolve rate
	rate, err := r.store.ResolveRate(
		ctx, req.Cloud, req.Service, req.ProductFamily,
		req.Region, req.Attributes, req.Unit, alias,
	)
	if err != nil {
		return nil, fmt.Errorf("rate resolution failed: %w", err)
	}
	
	// 4. Handle missing rate
	if rate == nil {
		if r.mode == Strict {
			return nil, fmt.Errorf("strict mode: no rate for %s/%s/%s unit=%s",
				req.Service, req.ProductFamily, req.Region, req.Unit)
		}
		
		// Permissive mode - return symbolic
		return &ResolutionResult{
			IsSymbolic: true,
			Reason:     fmt.Sprintf("rate not found: %s/%s/%s", req.Service, req.ProductFamily, req.Unit),
			SnapshotID: snapshot.ID,
			Source:     snapshot.Source,
		}, nil
	}
	
	// 5. Success
	return &ResolutionResult{
		Price:      &rate.Price,
		Currency:   rate.Currency,
		Confidence: rate.Confidence,
		SnapshotID: rate.SnapshotID,
		Source:     rate.Source,
		IsSymbolic: false,
	}, nil
}

// ResolveTiered resolves tiered pricing
func (r *StrictResolver) ResolveTiered(ctx context.Context, req ResolutionRequest) (*TieredResolutionResult, error) {
	alias := req.Alias
	if alias == "" {
		alias = r.defaultAlias
	}
	
	// Validate snapshot exists
	snapshot, err := r.store.GetActiveSnapshot(ctx, req.Cloud, req.Region, alias)
	if err != nil {
		return nil, fmt.Errorf("snapshot lookup failed: %w", err)
	}
	if snapshot == nil {
		return nil, fmt.Errorf("no active pricing snapshot for %s/%s/%s", req.Cloud, req.Region, alias)
	}
	
	// Get tiers
	tiers, err := r.store.ResolveTieredRates(
		ctx, req.Cloud, req.Service, req.ProductFamily,
		req.Region, req.Attributes, req.Unit, alias,
	)
	if err != nil {
		return nil, fmt.Errorf("tiered rate resolution failed: %w", err)
	}
	
	if len(tiers) == 0 {
		if r.mode == Strict {
			return nil, fmt.Errorf("strict mode: no tiered rates for %s/%s/%s unit=%s",
				req.Service, req.ProductFamily, req.Region, req.Unit)
		}
		
		return &TieredResolutionResult{
			IsSymbolic: true,
			Reason:     fmt.Sprintf("tiered rates not found: %s/%s", req.Service, req.ProductFamily),
			SnapshotID: snapshot.ID,
		}, nil
	}
	
	return &TieredResolutionResult{
		Tiers:      tiers,
		SnapshotID: snapshot.ID,
		IsSymbolic: false,
	}, nil
}

// TieredResolutionResult contains tiered resolution outcome
type TieredResolutionResult struct {
	Tiers      []TieredRate
	SnapshotID uuid.UUID
	IsSymbolic bool
	Reason     string
}

// CalculateCost computes cost from tiered rates and usage
func (r *TieredResolutionResult) CalculateCost(usage decimal.Decimal) (decimal.Decimal, float64) {
	return CalculateTieredCost(usage, r.Tiers)
}

// GetUsedSnapshots returns all snapshots used during resolution
func (r *StrictResolver) GetUsedSnapshots() map[string]uuid.UUID {
	result := make(map[string]uuid.UUID)
	for k, v := range r.usedSnapshot {
		result[k] = v
	}
	return result
}

// ResetSnapshots resets the snapshot tracking
func (r *StrictResolver) ResetSnapshots() {
	r.usedSnapshot = make(map[string]uuid.UUID)
}

// SnapshotAudit returns audit info for reproducibility
type SnapshotAudit struct {
	Cloud      CloudProvider `json:"cloud"`
	Region     string        `json:"region"`
	Alias      string        `json:"alias"`
	SnapshotID uuid.UUID     `json:"snapshot_id"`
}

// GetAuditInfo returns audit information for all used snapshots
func (r *StrictResolver) GetAuditInfo() []SnapshotAudit {
	var audits []SnapshotAudit
	for key, id := range r.usedSnapshot {
		parts := splitKey(key)
		if len(parts) == 3 {
			audits = append(audits, SnapshotAudit{
				Cloud:      CloudProvider(parts[0]),
				Region:     parts[1],
				Alias:      parts[2],
				SnapshotID: id,
			})
		}
	}
	return audits
}

func splitKey(key string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(key); i++ {
		if key[i] == ':' {
			parts = append(parts, key[start:i])
			start = i + 1
		}
	}
	parts = append(parts, key[start:])
	return parts
}
