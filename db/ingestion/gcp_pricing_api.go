// Package ingestion - Production GCP Cloud Billing API client
// Fetches COMPLETE pricing catalogs - mapper-agnostic
package ingestion

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"terraform-cost/db"
)

// GCPPricingAPIClient fetches pricing from GCP Cloud Billing Catalog API
// This is the PRODUCTION implementation - no stubs, no mocks
type GCPPricingAPIClient struct {
	httpClient   *http.Client
	baseURL      string
	servicesList []string
}

// GCPPricingConfig configures the GCP pricing client
type GCPPricingConfig struct {
	// HTTPTimeout for API calls
	HTTPTimeout time.Duration

	// Services to fetch (empty = ALL services)
	Services []string
}

// DefaultGCPPricingConfig returns production defaults
func DefaultGCPPricingConfig() *GCPPricingConfig {
	return &GCPPricingConfig{
		HTTPTimeout: 10 * time.Minute,
		Services:    AllGCPServices(),
	}
}

// AllGCPServices returns all GCP services with pricing
func AllGCPServices() []string {
	return []string{
		// Compute
		"Compute Engine",
		"Cloud Functions",
		"Cloud Run",
		"Google Kubernetes Engine",
		"App Engine",

		// Storage
		"Cloud Storage",
		"Persistent Disk",
		"Filestore",
		"Cloud Storage for Firebase",

		// Database
		"Cloud SQL",
		"Cloud Spanner",
		"Firestore",
		"Cloud Bigtable",
		"Memorystore",
		"AlloyDB",

		// Networking
		"Cloud NAT",
		"Cloud Load Balancing",
		"Cloud DNS",
		"Cloud CDN",
		"Cloud Armor",
		"Cloud VPN",
		"Cloud Interconnect",
		"Network Service Tiers",

		// Analytics
		"BigQuery",
		"Dataflow",
		"Dataproc",
		"Pub/Sub",
		"Cloud Composer",

		// AI/ML
		"Vertex AI",
		"Cloud Vision API",
		"Cloud Natural Language API",
		"Cloud Speech-to-Text",
		"Cloud Translation",

		// Security
		"Secret Manager",
		"Cloud KMS",
		"Cloud Identity",

		// Management
		"Cloud Logging",
		"Cloud Monitoring",
		"Cloud Trace",

		// Containers
		"Artifact Registry",
		"Container Registry",
	}
}

// GCPServiceIDs maps service names to their service IDs in the Billing API
var GCPServiceIDs = map[string]string{
	"Compute Engine":           "services/6F81-5844-456A",
	"Cloud Storage":            "services/95FF-2EF5-5EA1",
	"Cloud SQL":                "services/9662-B51E-5089",
	"BigQuery":                 "services/24E6-581D-38E5",
	"Cloud Functions":          "services/29E7-DA93-CA13",
	"Cloud Run":                "services/152E-C115-5142",
	"Google Kubernetes Engine": "services/6F81-5844-456A", // Uses Compute Engine pricing
	"Cloud Spanner":            "services/C3B3-6C49-F1FC",
	"Pub/Sub":                  "services/A1E8-BE35-7EBC",
	"Cloud Logging":            "services/5490-F7B7-8DF6",
	"Cloud Monitoring":         "services/58CD-E7C3-72CA",
	"Memorystore":              "services/B682-B197-4E71",
	"Cloud NAT":                "services/BD7F-AAE7-43B4",
	"Cloud Load Balancing":     "services/BD7F-AAE7-43B4",
	"Cloud DNS":                "services/BD7F-AAE7-43B4",
	"Secret Manager":           "services/F0B8-F53B-95FC",
	"Cloud KMS":                "services/91CD-AFAD-8EBB",
	"Artifact Registry":        "services/178E-D68C-64AB",
	"Firestore":                "services/F17B-65A4-2C49",
	"Cloud Bigtable":           "services/A0F2-D0F6-D52E",
	"Dataflow":                 "services/62B0-0FE2-E220",
	"Dataproc":                 "services/C3D7-A535-F265",
}

// NewGCPPricingAPIClient creates a production GCP pricing client
func NewGCPPricingAPIClient(cfg *GCPPricingConfig) *GCPPricingAPIClient {
	if cfg == nil {
		cfg = DefaultGCPPricingConfig()
	}

	return &GCPPricingAPIClient{
		httpClient: &http.Client{
			Timeout: cfg.HTTPTimeout,
		},
		baseURL:      "https://cloudbilling.googleapis.com/v1",
		servicesList: cfg.Services,
	}
}

// Cloud implements PriceFetcher
func (c *GCPPricingAPIClient) Cloud() db.CloudProvider {
	return db.GCP
}

// IsRealAPI implements RealAPIFetcher - THIS IS A REAL API
func (c *GCPPricingAPIClient) IsRealAPI() bool {
	return true
}

// SupportedRegions returns all GCP regions
func (c *GCPPricingAPIClient) SupportedRegions() []string {
	return []string{
		// Americas
		"us-central1", "us-east1", "us-east4", "us-east5",
		"us-west1", "us-west2", "us-west3", "us-west4",
		"us-south1",
		"northamerica-northeast1", "northamerica-northeast2",
		"southamerica-east1", "southamerica-west1",

		// Europe
		"europe-west1", "europe-west2", "europe-west3",
		"europe-west4", "europe-west6", "europe-west8", "europe-west9",
		"europe-north1", "europe-central2",
		"europe-southwest1",

		// Asia Pacific
		"asia-east1", "asia-east2",
		"asia-northeast1", "asia-northeast2", "asia-northeast3",
		"asia-south1", "asia-south2",
		"asia-southeast1", "asia-southeast2",
		"australia-southeast1", "australia-southeast2",

		// Middle East
		"me-west1", "me-central1",

		// Global (for global services)
		"global",
	}
}

// SupportedServices returns all supported services
func (c *GCPPricingAPIClient) SupportedServices() []string {
	return c.servicesList
}

// FetchRegion fetches ALL pricing for a region from GCP Cloud Billing API
// This is mapper-agnostic - fetches complete catalogs
func (c *GCPPricingAPIClient) FetchRegion(ctx context.Context, region string) ([]RawPrice, error) {
	var allPrices []RawPrice

	// First, get list of all services
	services, err := c.listServices(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list GCP services: %w", err)
	}

	// Fetch SKUs for each service
	for _, service := range services {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		skus, err := c.fetchServiceSKUs(ctx, service.ServiceID, region)
		if err != nil {
			// Log but continue
			fmt.Printf("Warning: failed to fetch SKUs for %s: %v\n", service.DisplayName, err)
			continue
		}

		allPrices = append(allPrices, skus...)
	}

	if len(allPrices) == 0 {
		return nil, fmt.Errorf("failed to fetch any pricing for GCP region %s", region)
	}

	return allPrices, nil
}

// listServices fetches all billable GCP services
func (c *GCPPricingAPIClient) listServices(ctx context.Context) ([]GCPService, error) {
	var allServices []GCPService
	pageToken := ""

	for {
		url := fmt.Sprintf("%s/services", c.baseURL)
		if pageToken != "" {
			url += "?pageToken=" + pageToken
		}

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, err
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("GCP API returned status %d", resp.StatusCode)
		}

		var response GCPServicesResponse
		if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
			return nil, err
		}

		allServices = append(allServices, response.Services...)

		if response.NextPageToken == "" {
			break
		}
		pageToken = response.NextPageToken
	}

	return allServices, nil
}

// fetchServiceSKUs fetches all SKUs for a service
func (c *GCPPricingAPIClient) fetchServiceSKUs(ctx context.Context, serviceID, region string) ([]RawPrice, error) {
	var allPrices []RawPrice
	pageToken := ""

	for {
		url := fmt.Sprintf("%s/%s/skus", c.baseURL, serviceID)
		if pageToken != "" {
			url += "?pageToken=" + pageToken
		}

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, err
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("GCP SKUs API returned status %d", resp.StatusCode)
		}

		var response GCPSKUsResponse
		if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
			return nil, err
		}

		// Convert SKUs to RawPrice
		for _, sku := range response.SKUs {
			// Filter by region if specified
			if region != "global" && !c.skuMatchesRegion(sku, region) {
				continue
			}

			prices := c.skuToPrices(sku, region)
			allPrices = append(allPrices, prices...)
		}

		if response.NextPageToken == "" {
			break
		}
		pageToken = response.NextPageToken
	}

	return allPrices, nil
}

// skuMatchesRegion checks if a SKU applies to a region
func (c *GCPPricingAPIClient) skuMatchesRegion(sku GCPSKU, region string) bool {
	if len(sku.ServiceRegions) == 0 {
		return true // Global SKU
	}
	for _, r := range sku.ServiceRegions {
		if r == region || r == "global" {
			return true
		}
	}
	return false
}

// skuToPrices converts a GCP SKU to RawPrice records
func (c *GCPPricingAPIClient) skuToPrices(sku GCPSKU, region string) []RawPrice {
	var prices []RawPrice

	for _, pricingInfo := range sku.PricingInfo {
		for _, tierRate := range pricingInfo.PricingExpression.TieredRates {
			// Calculate unit price in USD
			unitPrice := float64(tierRate.UnitPrice.Units) +
				float64(tierRate.UnitPrice.Nanos)/1e9

			if unitPrice == 0 {
				continue // Skip free tiers
			}

			price := RawPrice{
				SKU:           sku.SkuId,
				ServiceCode:   sku.Category.ServiceDisplayName,
				ProductFamily: sku.Category.ResourceFamily,
				Region:        region,
				Unit:          pricingInfo.PricingExpression.UsageUnit,
				PricePerUnit:  fmt.Sprintf("%.10f", unitPrice),
				Currency:      tierRate.UnitPrice.CurrencyCode,
				Attributes:    c.buildSKUAttributes(sku),
			}

			// Handle tiered pricing
			if tierRate.StartUsageAmount > 0 {
				start := tierRate.StartUsageAmount
				price.TierStart = &start
			}

			prices = append(prices, price)
		}
	}

	return prices
}

// buildSKUAttributes creates attributes from GCP SKU
func (c *GCPPricingAPIClient) buildSKUAttributes(sku GCPSKU) map[string]string {
	attrs := make(map[string]string)

	if sku.Description != "" {
		attrs["description"] = sku.Description
	}
	if sku.Category.ResourceGroup != "" {
		attrs["resourceGroup"] = sku.Category.ResourceGroup
	}
	if sku.Category.UsageType != "" {
		attrs["usageType"] = sku.Category.UsageType
	}

	for _, region := range sku.ServiceRegions {
		if region != "" {
			attrs["serviceRegion"] = region
			break
		}
	}

	return attrs
}

// GCPServicesResponse represents the Cloud Billing services list response
type GCPServicesResponse struct {
	Services      []GCPService `json:"services"`
	NextPageToken string       `json:"nextPageToken"`
}

// GCPService represents a GCP billable service
type GCPService struct {
	ServiceID   string `json:"name"` // Format: services/{service_id}
	DisplayName string `json:"displayName"`
	BusinessName string `json:"businessEntityName"`
}

// GCPSKUsResponse represents the Cloud Billing SKUs response
type GCPSKUsResponse struct {
	SKUs          []GCPSKU `json:"skus"`
	NextPageToken string   `json:"nextPageToken"`
}

// GCPSKU represents a GCP pricing SKU
type GCPSKU struct {
	Name           string     `json:"name"`
	SkuId          string     `json:"skuId"`
	Description    string     `json:"description"`
	Category       GCPCategory `json:"category"`
	ServiceRegions []string   `json:"serviceRegions"`
	PricingInfo    []GCPPricingInfo `json:"pricingInfo"`
	ServiceProviderName string `json:"serviceProviderName"`
}

// GCPCategory represents SKU category
type GCPCategory struct {
	ServiceDisplayName string `json:"serviceDisplayName"`
	ResourceFamily     string `json:"resourceFamily"`
	ResourceGroup      string `json:"resourceGroup"`
	UsageType          string `json:"usageType"`
}

// GCPPricingInfo represents pricing information
type GCPPricingInfo struct {
	EffectiveTime     string              `json:"effectiveTime"`
	Summary           string              `json:"summary"`
	PricingExpression GCPPricingExpression `json:"pricingExpression"`
	CurrencyConversionRate float64        `json:"currencyConversionRate"`
}

// GCPPricingExpression represents the pricing expression
type GCPPricingExpression struct {
	UsageUnit               string          `json:"usageUnit"`
	UsageUnitDescription    string          `json:"usageUnitDescription"`
	BaseUnit                string          `json:"baseUnit"`
	BaseUnitDescription     string          `json:"baseUnitDescription"`
	BaseUnitConversionFactor float64        `json:"baseUnitConversionFactor"`
	DisplayQuantity         float64         `json:"displayQuantity"`
	TieredRates             []GCPTieredRate `json:"tieredRates"`
}

// GCPTieredRate represents a pricing tier
type GCPTieredRate struct {
	StartUsageAmount float64    `json:"startUsageAmount"`
	UnitPrice        GCPMoney   `json:"unitPrice"`
}

// GCPMoney represents a monetary amount
type GCPMoney struct {
	CurrencyCode string `json:"currencyCode"`
	Units        int64  `json:"units"`
	Nanos        int32  `json:"nanos"`
}

// GCPPricingNormalizer normalizes raw GCP pricing to canonical format
type GCPPricingNormalizer struct{}

// NewGCPPricingNormalizer creates a production normalizer
func NewGCPPricingNormalizer() *GCPPricingNormalizer {
	return &GCPPricingNormalizer{}
}

// Cloud implements PriceNormalizer
func (n *GCPPricingNormalizer) Cloud() db.CloudProvider {
	return db.GCP
}

// Normalize converts raw GCP prices to normalized rates
func (n *GCPPricingNormalizer) Normalize(raw []RawPrice) ([]NormalizedRate, error) {
	var rates []NormalizedRate

	for _, r := range raw {
		price, err := ParsePrice(r.PricePerUnit)
		if err != nil {
			continue
		}

		attrs := n.normalizeAttributes(r.Attributes)

		rateKey := db.RateKey{
			Cloud:         db.GCP,
			Service:       r.ServiceCode,
			ProductFamily: r.ProductFamily,
			Region:        r.Region,
			Attributes:    attrs,
		}

		nr := NormalizedRate{
			RateKey:    rateKey,
			Unit:       n.normalizeUnit(r.Unit),
			Price:      price,
			Currency:   r.Currency,
			Confidence: 1.0,
		}

		rates = append(rates, nr)
	}

	return rates, nil
}

// normalizeAttributes converts GCP attributes to canonical form
func (n *GCPPricingNormalizer) normalizeAttributes(raw map[string]string) map[string]string {
	result := make(map[string]string)

	mapping := map[string]string{
		"resourceGroup":  "resource_group",
		"usageType":      "usage_type",
		"description":    "description",
		"serviceRegion":  "service_region",
	}

	for k, v := range raw {
		if v == "" {
			continue
		}
		if canonical, ok := mapping[k]; ok {
			result[canonical] = strings.ToLower(v)
		} else {
			result[toSnakeCase(k)] = strings.ToLower(v)
		}
	}

	return result
}

// normalizeUnit converts GCP units to canonical form
func (n *GCPPricingNormalizer) normalizeUnit(unit string) string {
	mapping := map[string]string{
		"h":         "hours",
		"mo":        "month",
		"GiBy":      "GB",
		"GiBy.h":    "GB-hours",
		"GiBy.mo":   "GB-month",
		"By":        "bytes",
		"count":     "count",
		"request":   "requests",
		"s":         "seconds",
	}

	if normalized, ok := mapping[unit]; ok {
		return normalized
	}

	return strings.ToLower(unit)
}
