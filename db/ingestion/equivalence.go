// Package ingestion - Region equivalence detection
// Detects regions with identical pricing for storage optimization
package ingestion

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"terraform-cost/db"
)

// RegionGroup represents a group of equivalent regions
type RegionGroup struct {
	CanonicalRegion string   // The primary region (first alphabetically)
	Aliases         []string // Other regions with identical pricing
	EquivalenceHash string   // Hash proving equivalence
	RateCount       int      // Number of rates in this group
}

// EquivalenceDetector detects equivalent pricing between regions
type EquivalenceDetector struct {
	// Map of equivalence hash -> regions with that hash
	hashToRegions map[string][]string
	// Map of region -> equivalence hash
	regionToHash map[string]string
	// Provider being analyzed
	provider db.CloudProvider
}

// NewEquivalenceDetector creates a new detector
func NewEquivalenceDetector(provider db.CloudProvider) *EquivalenceDetector {
	return &EquivalenceDetector{
		hashToRegions: make(map[string][]string),
		regionToHash:  make(map[string]string),
		provider:      provider,
	}
}

// AddRegionRates adds rates for a region to the detector
func (d *EquivalenceDetector) AddRegionRates(region string, rates []NormalizedRate) {
	hash := d.computeEquivalenceHash(rates)
	d.regionToHash[region] = hash
	d.hashToRegions[hash] = append(d.hashToRegions[hash], region)
}

// DetectEquivalence returns detected region groups after all regions are added
func (d *EquivalenceDetector) DetectEquivalence() []RegionGroup {
	var groups []RegionGroup

	// Process each unique hash
	processedHashes := make(map[string]bool)
	for hash, regions := range d.hashToRegions {
		if processedHashes[hash] {
			continue
		}
		processedHashes[hash] = true

		// Sort regions to get deterministic canonical region
		sort.Strings(regions)

		if len(regions) == 1 {
			// No equivalence, region is unique
			groups = append(groups, RegionGroup{
				CanonicalRegion: regions[0],
				Aliases:         []string{},
				EquivalenceHash: hash,
			})
		} else {
			// Multiple regions with same hash = equivalent
			groups = append(groups, RegionGroup{
				CanonicalRegion: regions[0], // First alphabetically is canonical
				Aliases:         regions[1:],
				EquivalenceHash: hash,
			})
		}
	}

	// Sort groups by canonical region for deterministic output
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].CanonicalRegion < groups[j].CanonicalRegion
	})

	return groups
}

// computeEquivalenceHash computes a hash that represents all pricing data
// Two regions are equivalent if and only if their hashes match
func (d *EquivalenceDetector) computeEquivalenceHash(rates []NormalizedRate) string {
	// Create normalized representation for hashing
	type rateForHash struct {
		Service       string            `json:"s"`
		ProductFamily string            `json:"pf"`
		Unit          string            `json:"u"`
		Price         string            `json:"p"`
		Currency      string            `json:"c"`
		Attributes    map[string]string `json:"a"`
	}

	hashRates := make([]rateForHash, 0, len(rates))
	for _, r := range rates {
		// Exclude region from attributes for comparison
		attrs := make(map[string]string)
		for k, v := range r.RateKey.Attributes {
			if k != "region" && k != "regionCode" && k != "location" {
				attrs[k] = v
			}
		}

		hashRates = append(hashRates, rateForHash{
			Service:       r.RateKey.Service,
			ProductFamily: r.RateKey.ProductFamily,
			Unit:          r.Unit,
			Price:         r.Price.String(),
			Currency:      r.Currency,
			Attributes:    attrs,
		})
	}

	// Sort for deterministic ordering
	sort.Slice(hashRates, func(i, j int) bool {
		if hashRates[i].Service != hashRates[j].Service {
			return hashRates[i].Service < hashRates[j].Service
		}
		if hashRates[i].ProductFamily != hashRates[j].ProductFamily {
			return hashRates[i].ProductFamily < hashRates[j].ProductFamily
		}
		return hashRates[i].Price < hashRates[j].Price
	})

	// Hash the sorted data
	data, _ := json.Marshal(hashRates)
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:16]) // Use first 16 bytes for storage efficiency
}

// GetCanonicalRegion returns the canonical region for a given region
func (d *EquivalenceDetector) GetCanonicalRegion(region string) string {
	hash, ok := d.regionToHash[region]
	if !ok {
		return region // Not analyzed, return as-is
	}

	regions := d.hashToRegions[hash]
	if len(regions) == 0 {
		return region
	}

	sort.Strings(regions)
	return regions[0]
}

// PrintEquivalenceReport prints a human-readable report of detected equivalences
func PrintEquivalenceReport(groups []RegionGroup) {
	fmt.Println("\n══════════════════════════════════════════════════════════")
	fmt.Println("REGION EQUIVALENCE REPORT")
	fmt.Println("══════════════════════════════════════════════════════════")

	uniqueCount := 0
	aliasCount := 0

	for _, g := range groups {
		if len(g.Aliases) == 0 {
			uniqueCount++
		} else {
			fmt.Printf("\n✓ Canonical: %s\n", g.CanonicalRegion)
			fmt.Printf("  Aliases:   %v\n", g.Aliases)
			fmt.Printf("  Hash:      %s...\n", g.EquivalenceHash[:16])
			aliasCount += len(g.Aliases)
		}
	}

	fmt.Println("\n──────────────────────────────────────────────────────────")
	fmt.Printf("Summary:\n")
	fmt.Printf("  • Unique regions:     %d\n", uniqueCount)
	fmt.Printf("  • Equivalent aliases: %d\n", aliasCount)
	fmt.Printf("  • Storage savings:    %d fewer region datasets\n", aliasCount)
	fmt.Println("══════════════════════════════════════════════════════════")
}
