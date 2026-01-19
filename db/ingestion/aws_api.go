// Package ingestion - Real AWS Pricing API integration
package ingestion

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"terraform-cost/db"

	"github.com/shopspring/decimal"
)

// AWSPricingAPIFetcher fetches real pricing data from AWS Pricing API
type AWSPricingAPIFetcher struct {
	httpClient *http.Client
	regions    []string
	services   []string
	baseURL    string
}

// NewAWSPricingAPIFetcher creates a new AWS Pricing API fetcher
func NewAWSPricingAPIFetcher() *AWSPricingAPIFetcher {
	return &AWSPricingAPIFetcher{
		httpClient: &http.Client{Timeout: 60 * time.Second},
		baseURL:    "https://pricing.us-east-1.amazonaws.com",
		regions: []string{
			// US
			"us-east-1", "us-east-2", "us-west-1", "us-west-2",
			// Canada
			"ca-central-1", "ca-west-1",
			// Europe
			"eu-west-1", "eu-west-2", "eu-west-3", "eu-central-1", "eu-central-2",
			"eu-north-1", "eu-south-1", "eu-south-2",
			// Asia Pacific
			"ap-southeast-1", "ap-southeast-2", "ap-southeast-3", "ap-southeast-4",
			"ap-northeast-1", "ap-northeast-2", "ap-northeast-3",
			"ap-east-1", "ap-south-1", "ap-south-2",
			// South America
			"sa-east-1",
			// Middle East
			"me-south-1", "me-central-1", "il-central-1",
			// Africa
			"af-south-1",
		},
		services: []string{
			"AmazonEC2", "AmazonRDS", "AWSLambda", "AmazonS3", "ElasticLoadBalancing",
			"AmazonDynamoDB", "AmazonElastiCache", "AmazonCloudWatch", "AmazonRoute53",
			"AWSSecretsManager", "AWSKMS", "AmazonSNS", "AmazonSQS", "AmazonECS",
			"AmazonEKS", "AWSFargate", "AmazonCloudFront", "AWSCodeBuild",
		},
	}
}

func (f *AWSPricingAPIFetcher) Cloud() db.CloudProvider {
	return db.AWS
}

// IsRealAPI implements RealAPIFetcher - THIS IS A REAL API
func (f *AWSPricingAPIFetcher) IsRealAPI() bool {
	return true
}

// SetAllowedServices configures which services to fetch (useful for dev/testing)
func (f *AWSPricingAPIFetcher) SetAllowedServices(services []string) {
	if len(services) > 0 {
		f.services = services
	}
}

func (f *AWSPricingAPIFetcher) SupportedRegions() []string {
	return f.regions
}

// SupportedServices returns the list of AWS services this fetcher can get pricing for
// SupportedServices returns the list of AWS services this fetcher can get pricing for
func (f *AWSPricingAPIFetcher) SupportedServices() []string {
	return f.services
}

// AWSPriceListIndex represents the top-level price list index
type AWSPriceListIndex struct {
	FormatVersion string `json:"formatVersion"`
	Disclaimer    string `json:"disclaimer"`
	Offers        map[string]struct {
		OfferCode     string `json:"offerCode"`
		CurrentVersion string `json:"currentVersionUrl"`
		RegionIndex   string `json:"currentRegionIndexUrl"`
	} `json:"offers"`
}

// AWSRegionIndex represents regional price index
type AWSRegionIndex struct {
	Regions map[string]struct {
		CurrentVersionURL string `json:"currentVersionUrl"`
	} `json:"regions"`
}

// AWSPriceList represents the full price list for a service
type AWSPriceList struct {
	FormatVersion string `json:"formatVersion"`
	Disclaimer    string `json:"disclaimer"`
	PublicationDate string `json:"publicationDate"`
	Products      map[string]AWSProduct `json:"products"`
	Terms         struct {
		OnDemand map[string]map[string]AWSTerm `json:"OnDemand"`
		Reserved map[string]map[string]AWSTerm `json:"Reserved,omitempty"`
	} `json:"terms"`
}

// AWSProduct represents a product in the price list
type AWSProduct struct {
	SKU           string            `json:"sku"`
	ProductFamily string            `json:"productFamily"`
	Attributes    map[string]string `json:"attributes"`
}

// AWSTerm represents a pricing term
type AWSTerm struct {
	OfferTermCode   string `json:"offerTermCode"`
	SKU             string `json:"sku"`
	EffectiveDate   string `json:"effectiveDate"`
	PriceDimensions map[string]AWSPriceDimension `json:"priceDimensions"`
	TermAttributes  map[string]string `json:"termAttributes,omitempty"`
}

// AWSPriceDimension represents a price dimension
type AWSPriceDimension struct {
	RateCode     string `json:"rateCode"`
	Description  string `json:"description"`
	BeginRange   string `json:"beginRange"`
	EndRange     string `json:"endRange"`
	Unit         string `json:"unit"`
	PricePerUnit struct {
		USD string `json:"USD"`
	} `json:"pricePerUnit"`
	AppliesTo []string `json:"appliesTo"`
}

// FetchRegion fetches all prices for a region from AWS Pricing API
func (f *AWSPricingAPIFetcher) FetchRegion(ctx context.Context, region string) ([]RawPrice, error) {
	var allPrices []RawPrice
	
	// Core services to fetch
	services := f.services
	
	for _, service := range services {
		prices, err := f.fetchServicePricing(ctx, service, region)
		if err != nil {
			// Log but continue with other services
			fmt.Printf("Warning: failed to fetch %s pricing: %v\n", service, err)
			continue
		}
		allPrices = append(allPrices, prices...)
		fmt.Printf("Fetched %d prices for %s\n", len(prices), service)
	}

	return allPrices, nil
}

// fetchServicePricing fetches pricing for a specific service using region_index
func (f *AWSPricingAPIFetcher) fetchServicePricing(ctx context.Context, service, region string) ([]RawPrice, error) {
	// Get the index first
	indexURL := fmt.Sprintf("%s/offers/v1.0/aws/%s/current/region_index.json", f.baseURL, service)
	
	req, err := http.NewRequestWithContext(ctx, "GET", indexURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := f.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("index request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("index not found: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var regionIndex AWSRegionIndex
	if err := json.Unmarshal(body, &regionIndex); err != nil {
		return nil, fmt.Errorf("failed to parse region index: %w", err)
	}

	// Find the region-specific URL
	regionData, ok := regionIndex.Regions[region]
	if !ok {
		return nil, fmt.Errorf("region %s not found in index", region)
	}

	// Fetch region-specific pricing
	regionURL := f.baseURL + regionData.CurrentVersionURL
	req, err = http.NewRequestWithContext(ctx, "GET", regionURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err = f.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("region pricing request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("region pricing not found: %d", resp.StatusCode)
	}

	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return f.parsePriceList(body, service, region)
}

// parsePriceList parses AWS price list JSON
func (f *AWSPricingAPIFetcher) parsePriceList(data []byte, service, region string) ([]RawPrice, error) {
	var priceList AWSPriceList
	if err := json.Unmarshal(data, &priceList); err != nil {
		return nil, fmt.Errorf("failed to parse price list: %w", err)
	}

	var prices []RawPrice

	// Process on-demand terms
	for sku, productTerms := range priceList.Terms.OnDemand {
		product, ok := priceList.Products[sku]
		if !ok {
			continue
		}

		// Filter by region
		if prodRegion := product.Attributes["regionCode"]; prodRegion != "" && prodRegion != region {
			continue
		}
		if prodLocation := product.Attributes["location"]; prodLocation != "" && !matchesRegion(prodLocation, region) {
			continue
		}

		for _, term := range productTerms {
			for _, dim := range term.PriceDimensions {
				price := RawPrice{
					SKU:           sku,
					ServiceCode:   service,
					ProductFamily: product.ProductFamily,
					Region:        region,
					Unit:          dim.Unit,
					PricePerUnit:  dim.PricePerUnit.USD,
					Currency:      "USD",
					Attributes:    product.Attributes,
				}

				// Parse tiers
				if dim.BeginRange != "0" && dim.BeginRange != "" {
					if val, err := parseFloat(dim.BeginRange); err == nil {
						price.TierStart = &val
					}
				}
				if dim.EndRange != "Inf" && dim.EndRange != "" {
					if val, err := parseFloat(dim.EndRange); err == nil {
						price.TierEnd = &val
					}
				}

				// Parse effective date
				if term.EffectiveDate != "" {
					if t, err := time.Parse("2006-01-02T15:04:05Z", term.EffectiveDate); err == nil {
						price.EffectiveDate = &t
					}
				}

				prices = append(prices, price)
			}
		}
	}

	return prices, nil
}

// mapRegionToAWSName maps region codes to AWS naming convention
func mapRegionToAWSName(region string) string {
	// AWS uses different naming in some cases
	mapping := map[string]string{
		"us-east-1": "US East (N. Virginia)",
		"us-east-2": "US East (Ohio)",
		"us-west-1": "US West (N. California)",
		"us-west-2": "US West (Oregon)",
		"eu-west-1": "EU (Ireland)",
		"eu-west-2": "EU (London)",
		"eu-central-1": "EU (Frankfurt)",
		"ap-southeast-1": "Asia Pacific (Singapore)",
		"ap-southeast-2": "Asia Pacific (Sydney)",
		"ap-northeast-1": "Asia Pacific (Tokyo)",
	}
	if name, ok := mapping[region]; ok {
		return url.PathEscape(name)
	}
	return region
}

// matchesRegion checks if a location string matches a region
func matchesRegion(location, region string) bool {
	mapping := map[string][]string{
		// US
		"us-east-1": {"US East (N. Virginia)", "US-East"},
		"us-east-2": {"US East (Ohio)"},
		"us-west-1": {"US West (N. California)"},
		"us-west-2": {"US West (Oregon)"},
		
		// Canada
		"ca-central-1": {"Canada (Central)"},
		"ca-west-1":    {"Canada West (Calgary)"},

		// Europe
		"eu-west-1":    {"EU (Ireland)", "Europe (Ireland)", "EU-West"},
		"eu-west-2":    {"EU (London)", "Europe (London)"},
		"eu-west-3":    {"EU (Paris)", "Europe (Paris)"},
		"eu-central-1": {"EU (Frankfurt)", "Europe (Frankfurt)"},
		"eu-central-2": {"EU (Zurich)", "Europe (Zurich)"},
		"eu-north-1":   {"EU (Stockholm)", "Europe (Stockholm)"},
		"eu-south-1":   {"EU (Milan)", "Europe (Milan)"},
		"eu-south-2":   {"EU (Spain)", "Europe (Spain)"},

		// Asia Pacific
		"ap-southeast-1": {"Asia Pacific (Singapore)"},
		"ap-southeast-2": {"Asia Pacific (Sydney)"},
		"ap-southeast-3": {"Asia Pacific (Jakarta)"},
		"ap-southeast-4": {"Asia Pacific (Melbourne)"},
		"ap-northeast-1": {"Asia Pacific (Tokyo)"},
		"ap-northeast-2": {"Asia Pacific (Seoul)"},
		"ap-northeast-3": {"Asia Pacific (Osaka)"},
		"ap-east-1":      {"Asia Pacific (Hong Kong)"},
		"ap-south-1":     {"Asia Pacific (Mumbai)"},
		"ap-south-2":     {"Asia Pacific (Hyderabad)"},

		// South America
		"sa-east-1": {"South America (São Paulo)", "South America (Sao Paulo)"},

		// Middle East
		"me-south-1":   {"Middle East (Bahrain)"},
		"me-central-1": {"Middle East (UAE)"},
		"il-central-1": {"Israel (Tel Aviv)"},

		// Africa
		"af-south-1": {"Africa (Cape Town)"},
	}
	
	candidates, ok := mapping[region]
	if !ok {
		return false
	}
	
	for _, c := range candidates {
		if strings.Contains(location, c) || c == location {
			return true
		}
	}
	return false
}

func parseFloat(s string) (float64, error) {
	var f float64
	_, err := fmt.Sscanf(s, "%f", &f)
	return f, err
}

// AWSPricingAPINormalizer normalizes real AWS pricing data
type AWSPricingAPINormalizer struct {
	dimensionMapping map[string]string
}

// NewAWSPricingAPINormalizer creates a new normalizer for AWS Pricing API data
func NewAWSPricingAPINormalizer() *AWSPricingAPINormalizer {
	return &AWSPricingAPINormalizer{
		dimensionMapping: map[string]string{
			"instanceType":        "instance_type",
			"instanceFamily":      "instance_family",
			"operatingSystem":     "os",
			"tenancy":             "tenancy",
			"preInstalledSw":      "software",
			"licenseModel":        "license",
			"capacitystatus":      "capacity_status",
			"volumeApiName":       "volume_type",
			"volumeType":          "volume_class",
			"storageClass":        "storage_class",
			"databaseEngine":      "engine",
			"databaseEdition":     "edition",
			"deploymentOption":    "deployment",
			"productFamily":       "product_family",
			"usagetype":           "usage_type",
			"memory":              "memory",
			"vcpu":                "vcpu",
			"physicalProcessor":   "processor",
			"clockSpeed":          "clock_speed",
			"networkPerformance":  "network",
		},
	}
}

func (n *AWSPricingAPINormalizer) Cloud() db.CloudProvider {
	return db.AWS
}

// Normalize converts raw AWS Pricing API data to normalized rates
func (n *AWSPricingAPINormalizer) Normalize(raw []RawPrice) ([]NormalizedRate, error) {
	var rates []NormalizedRate

	for _, r := range raw {
		// Skip zero/empty prices
		if r.PricePerUnit == "" || r.PricePerUnit == "0" || r.PricePerUnit == "0.0000000000" {
			continue
		}

		price, err := decimal.NewFromString(r.PricePerUnit)
		if err != nil {
			continue
		}

		// Skip zero prices
		if price.IsZero() {
			continue
		}

		// Normalize attributes
		attrs := n.normalizeAttributes(r.Attributes)

		// Create rate key
		rateKey := db.RateKey{
			Cloud:         db.AWS,
			Service:       r.ServiceCode,
			ProductFamily: r.ProductFamily,
			Region:        r.Region,
			Attributes:    attrs,
		}

		// Create normalized rate
		nr := NormalizedRate{
			RateKey:    rateKey,
			Unit:       n.normalizeUnit(r.Unit),
			Price:      price,
			Currency:   r.Currency,
			Confidence: 1.0, // Direct from AWS API = full confidence
		}

		// Handle tiers
		if r.TierStart != nil {
			d := decimal.NewFromFloat(*r.TierStart)
			nr.TierMin = &d
		}
		if r.TierEnd != nil {
			d := decimal.NewFromFloat(*r.TierEnd)
			nr.TierMax = &d
		}

		rates = append(rates, nr)
	}

	return rates, nil
}

func (n *AWSPricingAPINormalizer) normalizeAttributes(raw map[string]string) map[string]string {
	result := make(map[string]string)

	for k, v := range raw {
		// Map to canonical key name
		key := k
		if canonical, ok := n.dimensionMapping[k]; ok {
			key = canonical
		} else {
			key = strings.ToLower(strings.ReplaceAll(k, " ", "_"))
		}

		// Normalize value
		val := strings.ToLower(strings.TrimSpace(v))
		
		// Skip empty or NA values
		if val == "" || val == "na" || val == "n/a" {
			continue
		}

		result[key] = val
	}

	return result
}

func (n *AWSPricingAPINormalizer) normalizeUnit(unit string) string {
	mapping := map[string]string{
		"Hrs":           "hours",
		"hrs":           "hours",
		"GB-Mo":         "GB-month",
		"GB-month":      "GB-month",
		"GB":            "GB",
		"Requests":      "requests",
		"requests":      "requests",
		"GB-Second":     "GB-seconds",
		"GB-Seconds":    "GB-seconds",
		"Lambda-GB-Second": "GB-seconds",
		"Quantity":      "units",
		"LCU-Hrs":       "LCU-hours",
		"NLCU-Hrs":      "NLCU-hours",
	}

	if normalized, ok := mapping[unit]; ok {
		return normalized
	}
	return strings.ToLower(unit)
}
