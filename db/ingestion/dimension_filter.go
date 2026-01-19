// Package ingestion - Dimension filtering to prevent explosion
package ingestion

import (
	"terraform-cost/db"
)

// DimensionAllowlist defines which dimensions to keep per service
type DimensionAllowlist struct {
	dimensions map[string]map[string]DimensionConfig // cloud:service -> dimension -> config
}

// DimensionConfig holds configuration for a dimension
type DimensionConfig struct {
	Key        string
	IsRequired bool
	Priority   int
}

// NewDimensionAllowlist creates a new allowlist with defaults
func NewDimensionAllowlist() *DimensionAllowlist {
	al := &DimensionAllowlist{
		dimensions: make(map[string]map[string]DimensionConfig),
	}
	al.loadDefaults()
	return al
}

// loadDefaults populates default allowlists for high-impact services
func (al *DimensionAllowlist) loadDefaults() {
	// AWS EC2
	al.Add(db.AWS, "AmazonEC2", "instance_type", true, 100)
	al.Add(db.AWS, "AmazonEC2", "os", true, 90)
	al.Add(db.AWS, "AmazonEC2", "tenancy", false, 80)
	al.Add(db.AWS, "AmazonEC2", "volume_type", false, 70)
	al.Add(db.AWS, "AmazonEC2", "capacity_status", false, 50)
	al.Add(db.AWS, "AmazonEC2", "product_family", false, 60)

	// AWS RDS
	al.Add(db.AWS, "AmazonRDS", "instance_type", true, 100)
	al.Add(db.AWS, "AmazonRDS", "engine", true, 90)
	al.Add(db.AWS, "AmazonRDS", "deployment", false, 70)
	al.Add(db.AWS, "AmazonRDS", "license", false, 60)

	// AWS Lambda
	al.Add(db.AWS, "AWSLambda", "memory_size", false, 80)
	al.Add(db.AWS, "AWSLambda", "architecture", false, 60)
	al.Add(db.AWS, "AWSLambda", "group", false, 70)

	// AWS S3
	al.Add(db.AWS, "AmazonS3", "storage_class", true, 100)
	al.Add(db.AWS, "AmazonS3", "volume_type", false, 80)

	// AWS ELB
	al.Add(db.AWS, "ElasticLoadBalancing", "product_family", true, 100)
	al.Add(db.AWS, "ElasticLoadBalancing", "usage_type", false, 70)

	// AWS DynamoDB
	al.Add(db.AWS, "AmazonDynamoDB", "group", false, 80)
	al.Add(db.AWS, "AmazonDynamoDB", "usage_type", false, 70)

	// AWS NAT Gateway
	al.Add(db.AWS, "AmazonEC2", "usage_type", false, 60)
}

// Add adds a dimension to the allowlist
func (al *DimensionAllowlist) Add(cloud db.CloudProvider, service, dimension string, required bool, priority int) {
	key := string(cloud) + ":" + service
	if _, ok := al.dimensions[key]; !ok {
		al.dimensions[key] = make(map[string]DimensionConfig)
	}
	al.dimensions[key][dimension] = DimensionConfig{
		Key:        dimension,
		IsRequired: required,
		Priority:   priority,
	}
}

// GetAllowed returns allowed dimensions for a service
func (al *DimensionAllowlist) GetAllowed(cloud db.CloudProvider, service string) map[string]DimensionConfig {
	key := string(cloud) + ":" + service
	if dims, ok := al.dimensions[key]; ok {
		return dims
	}
	return nil
}

// GetRequired returns required dimensions for a service
func (al *DimensionAllowlist) GetRequired(cloud db.CloudProvider, service string) []string {
	key := string(cloud) + ":" + service
	dims, ok := al.dimensions[key]
	if !ok {
		return nil
	}

	var required []string
	for k, cfg := range dims {
		if cfg.IsRequired {
			required = append(required, k)
		}
	}
	return required
}

// Filter filters attributes to only allowed dimensions
func (al *DimensionAllowlist) Filter(cloud db.CloudProvider, service string, attrs map[string]string) map[string]string {
	allowed := al.GetAllowed(cloud, service)
	if allowed == nil {
		// No allowlist = accept all (for unconfigured services)
		return attrs
	}

	filtered := make(map[string]string)
	for k, v := range attrs {
		if _, ok := allowed[k]; ok {
			filtered[k] = v
		}
	}
	return filtered
}

// IsAllowed checks if a dimension is allowed
func (al *DimensionAllowlist) IsAllowed(cloud db.CloudProvider, service, dimension string) bool {
	allowed := al.GetAllowed(cloud, service)
	if allowed == nil {
		return true // No list = allow all
	}
	_, ok := allowed[dimension]
	return ok
}

// FilteredNormalizer wraps a normalizer with dimension filtering
type FilteredNormalizer struct {
	inner     PriceNormalizer
	allowlist *DimensionAllowlist
}

// NewFilteredNormalizer creates a normalizer that filters dimensions
func NewFilteredNormalizer(inner PriceNormalizer) *FilteredNormalizer {
	return &FilteredNormalizer{
		inner:     inner,
		allowlist: NewDimensionAllowlist(),
	}
}

func (n *FilteredNormalizer) Cloud() db.CloudProvider {
	return n.inner.Cloud()
}

// Normalize normalizes and filters dimensions
func (n *FilteredNormalizer) Normalize(raw []RawPrice) ([]NormalizedRate, error) {
	// First normalize with inner normalizer
	rates, err := n.inner.Normalize(raw)
	if err != nil {
		return nil, err
	}

	// Then filter dimensions
	for i := range rates {
		rates[i].RateKey.Attributes = n.allowlist.Filter(
			rates[i].RateKey.Cloud,
			rates[i].RateKey.Service,
			rates[i].RateKey.Attributes,
		)
	}

	// Deduplicate after filtering (same rate key might now match)
	return n.deduplicate(rates), nil
}

// deduplicate removes duplicate rates (keeping first)
func (n *FilteredNormalizer) deduplicate(rates []NormalizedRate) []NormalizedRate {
	seen := make(map[string]bool)
	var result []NormalizedRate

	for _, r := range rates {
		key := rateKeyString(r.RateKey) + "|" + r.Unit
		if !seen[key] {
			seen[key] = true
			result = append(result, r)
		}
	}

	return result
}

// Stats returns filtering statistics
type FilteringStats struct {
	TotalRates    int
	FilteredRates int
	DroppedRates  int
	ByService     map[string]int
}

// WithStats normalizes and returns stats
func (n *FilteredNormalizer) WithStats(raw []RawPrice) ([]NormalizedRate, *FilteringStats, error) {
	// Normalize without filtering first
	allRates, err := n.inner.Normalize(raw)
	if err != nil {
		return nil, nil, err
	}

	// Then filter
	filtered, err := n.Normalize(raw)
	if err != nil {
		return nil, nil, err
	}

	stats := &FilteringStats{
		TotalRates:    len(allRates),
		FilteredRates: len(filtered),
		DroppedRates:  len(allRates) - len(filtered),
		ByService:     make(map[string]int),
	}

	for _, r := range filtered {
		stats.ByService[r.RateKey.Service]++
	}

	return filtered, stats, nil
}
