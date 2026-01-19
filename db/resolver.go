// Package db - Pricing resolver that integrates with mappers
package db

import (
	"context"
	"fmt"

	"github.com/shopspring/decimal"
)

// Resolver provides pricing resolution for the estimation engine
type Resolver struct {
	store        PricingStore
	defaultAlias string
	strictMode   bool
}

// NewResolver creates a new pricing resolver
func NewResolver(store PricingStore) *Resolver {
	return &Resolver{
		store:        store,
		defaultAlias: "default",
		strictMode:   false,
	}
}

// WithStrictMode enables strict mode (fails on missing rates)
func (r *Resolver) WithStrictMode(strict bool) *Resolver {
	r.strictMode = strict
	return r
}

// WithDefaultAlias sets the default provider alias
func (r *Resolver) WithDefaultAlias(alias string) *Resolver {
	r.defaultAlias = alias
	return r
}

// ResolveRequest contains all parameters for rate resolution
type ResolveRequest struct {
	Cloud         CloudProvider
	Service       string
	ProductFamily string
	Region        string
	Attributes    map[string]string
	Unit          string
	Alias         string // Optional, uses default if empty
}

// ResolveResult contains the resolved rate or error info
type ResolveResult struct {
	Rate       *ResolvedRate
	IsSymbolic bool
	Reason     string
}

// Resolve attempts to resolve a pricing rate
func (r *Resolver) Resolve(ctx context.Context, req ResolveRequest) (*ResolveResult, error) {
	alias := req.Alias
	if alias == "" {
		alias = r.defaultAlias
	}

	// Check for active snapshot
	snapshot, err := r.store.GetActiveSnapshot(ctx, req.Cloud, req.Region, alias)
	if err != nil {
		return nil, fmt.Errorf("failed to get active snapshot: %w", err)
	}
	if snapshot == nil {
		if r.strictMode {
			return nil, fmt.Errorf("strict mode: no active snapshot for %s/%s/%s", req.Cloud, req.Region, alias)
		}
		return &ResolveResult{
			IsSymbolic: true,
			Reason:     fmt.Sprintf("no pricing snapshot for %s/%s", req.Cloud, req.Region),
		}, nil
	}

	// Resolve the rate
	rate, err := r.store.ResolveRate(ctx, req.Cloud, req.Service, req.ProductFamily, req.Region, req.Attributes, req.Unit, alias)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve rate: %w", err)
	}
	if rate == nil {
		if r.strictMode {
			return nil, fmt.Errorf("strict mode: no rate found for %s/%s/%s", req.Service, req.ProductFamily, req.Unit)
		}
		return &ResolveResult{
			IsSymbolic: true,
			Reason:     fmt.Sprintf("rate not found: %s/%s/%s", req.Service, req.ProductFamily, req.Unit),
		}, nil
	}

	return &ResolveResult{
		Rate:       rate,
		IsSymbolic: false,
	}, nil
}

// ResolveTiered resolves tiered pricing (S3, data transfer, etc.)
func (r *Resolver) ResolveTiered(ctx context.Context, req ResolveRequest) ([]TieredRate, error) {
	alias := req.Alias
	if alias == "" {
		alias = r.defaultAlias
	}

	return r.store.ResolveTieredRates(ctx, req.Cloud, req.Service, req.ProductFamily, req.Region, req.Attributes, req.Unit, alias)
}

// CalculateTieredCost computes cost for tiered pricing
func CalculateTieredCost(usage decimal.Decimal, tiers []TieredRate) (decimal.Decimal, float64) {
	if len(tiers) == 0 {
		return decimal.Zero, 0
	}

	totalCost := decimal.Zero
	remaining := usage
	minConfidence := 1.0

	for _, tier := range tiers {
		if remaining.LessThanOrEqual(decimal.Zero) {
			break
		}

		var tierUsage decimal.Decimal
		if tier.Max == nil {
			// Unlimited tier
			tierUsage = remaining
		} else {
			tierSize := tier.Max.Sub(tier.Min)
			if remaining.GreaterThan(tierSize) {
				tierUsage = tierSize
			} else {
				tierUsage = remaining
			}
		}

		tierCost := tierUsage.Mul(tier.Price)
		totalCost = totalCost.Add(tierCost)
		remaining = remaining.Sub(tierUsage)

		if tier.Confidence < minConfidence {
			minConfidence = tier.Confidence
		}
	}

	return totalCost, minConfidence
}
