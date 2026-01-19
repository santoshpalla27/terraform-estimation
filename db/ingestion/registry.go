// Package ingestion - Fetcher registry and factory
// Provides centralized access to production pricing fetchers
package ingestion

import (
	"fmt"
	"sync"

	"terraform-cost/db"
)

// FetcherRegistry manages pricing fetchers by cloud provider
type FetcherRegistry struct {
	mu       sync.RWMutex
	fetchers map[db.CloudProvider]PriceFetcher
	normalizers map[db.CloudProvider]PriceNormalizer
}

var (
	defaultRegistry *FetcherRegistry
	registryOnce    sync.Once
)

// GetRegistry returns the global fetcher registry
func GetRegistry() *FetcherRegistry {
	registryOnce.Do(func() {
		defaultRegistry = NewFetcherRegistry()
		defaultRegistry.RegisterDefaults()
	})
	return defaultRegistry
}

// NewFetcherRegistry creates a new registry
func NewFetcherRegistry() *FetcherRegistry {
	return &FetcherRegistry{
		fetchers:    make(map[db.CloudProvider]PriceFetcher),
		normalizers: make(map[db.CloudProvider]PriceNormalizer),
	}
}

// RegisterDefaults registers all default production fetchers
func (r *FetcherRegistry) RegisterDefaults() {
	r.mu.Lock()
	defer r.mu.Unlock()

	// AWS - Production API client
	r.fetchers[db.AWS] = NewAWSPricingAPIFetcher()
	r.normalizers[db.AWS] = NewAWSPricingAPINormalizer()

	// Azure - Production API client
	r.fetchers[db.Azure] = NewAzurePricingAPIClient(nil)
	r.normalizers[db.Azure] = NewAzurePricingNormalizer()

	// GCP - Production API client
	r.fetchers[db.GCP] = NewGCPPricingAPIClient(nil)
	r.normalizers[db.GCP] = NewGCPPricingNormalizer()
}

// GetFetcher returns the fetcher for a cloud provider
func (r *FetcherRegistry) GetFetcher(cloud db.CloudProvider) (PriceFetcher, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	fetcher, ok := r.fetchers[cloud]
	if !ok {
		return nil, fmt.Errorf("no fetcher registered for cloud: %s", cloud)
	}
	return fetcher, nil
}

// GetNormalizer returns the normalizer for a cloud provider
func (r *FetcherRegistry) GetNormalizer(cloud db.CloudProvider) (PriceNormalizer, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	normalizer, ok := r.normalizers[cloud]
	if !ok {
		return nil, fmt.Errorf("no normalizer registered for cloud: %s", cloud)
	}
	return normalizer, nil
}

// RegisterFetcher registers a custom fetcher
func (r *FetcherRegistry) RegisterFetcher(cloud db.CloudProvider, fetcher PriceFetcher) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fetchers[cloud] = fetcher
}

// RegisterNormalizer registers a custom normalizer
func (r *FetcherRegistry) RegisterNormalizer(cloud db.CloudProvider, normalizer PriceNormalizer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.normalizers[cloud] = normalizer
}

// IsRealAPI checks if the fetcher for a cloud uses real APIs
func (r *FetcherRegistry) IsRealAPI(cloud db.CloudProvider) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	fetcher, ok := r.fetchers[cloud]
	if !ok {
		return false
	}

	if realAPI, ok := fetcher.(RealAPIFetcher); ok {
		return realAPI.IsRealAPI()
	}

	return false
}

// GetProductionFetcher returns the production fetcher for a cloud
// Returns error if the fetcher is not a real API
func GetProductionFetcher(cloud db.CloudProvider) (PriceFetcher, error) {
	registry := GetRegistry()
	
	fetcher, err := registry.GetFetcher(cloud)
	if err != nil {
		return nil, err
	}

	// Verify it's a real API
	if realAPI, ok := fetcher.(RealAPIFetcher); ok {
		if !realAPI.IsRealAPI() {
			return nil, fmt.Errorf("fetcher for %s is not a real API implementation", cloud)
		}
	}

	return fetcher, nil
}

// GetProductionNormalizer returns the production normalizer for a cloud
func GetProductionNormalizer(cloud db.CloudProvider) (PriceNormalizer, error) {
	return GetRegistry().GetNormalizer(cloud)
}
