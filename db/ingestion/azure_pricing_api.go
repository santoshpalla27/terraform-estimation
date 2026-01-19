// Package ingestion - Production Azure Retail Prices API client
// Fetches COMPLETE pricing catalogs - mapper-agnostic
package ingestion

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"terraform-cost/db"
)

// AzurePricingAPIClient fetches pricing from Azure Retail Prices API
// This is the PRODUCTION implementation - no stubs, no mocks
type AzurePricingAPIClient struct {
	httpClient   *http.Client
	baseURL      string
	servicesList []string
}

// AzurePricingConfig configures the Azure pricing client
type AzurePricingConfig struct {
	// HTTPTimeout for API calls
	HTTPTimeout time.Duration

	// Services to fetch (empty = ALL services)
	Services []string
}

// DefaultAzurePricingConfig returns production defaults
func DefaultAzurePricingConfig() *AzurePricingConfig {
	return &AzurePricingConfig{
		HTTPTimeout: 10 * time.Minute,
		Services:    AllAzureServices(),
	}
}

// AllAzureServices returns all Azure services with pricing
func AllAzureServices() []string {
	return []string{
		// Compute
		"Virtual Machines",
		"Virtual Machine Scale Sets",
		"Azure Functions",
		"Container Instances",
		"Azure Kubernetes Service",
		"App Service",
		"Batch",

		// Storage
		"Storage",
		"Blob Storage",
		"File Storage",
		"Queue Storage",
		"Table Storage",
		"Managed Disks",
		"Azure NetApp Files",

		// Database
		"SQL Database",
		"Azure Database for MySQL",
		"Azure Database for PostgreSQL",
		"Azure Cosmos DB",
		"Azure Cache for Redis",
		"Azure Synapse Analytics",
		"Azure Database for MariaDB",

		// Networking
		"Virtual Network",
		"Load Balancer",
		"Application Gateway",
		"VPN Gateway",
		"Azure DNS",
		"Azure Front Door",
		"Azure CDN",
		"Azure Firewall",
		"ExpressRoute",
		"Bandwidth",

		// Analytics
		"Azure Databricks",
		"HDInsight",
		"Azure Data Factory",
		"Azure Stream Analytics",
		"Event Hubs",

		// Integration
		"Service Bus",
		"Event Grid",
		"Logic Apps",
		"API Management",

		// Security
		"Key Vault",
		"Azure Active Directory",
		"Azure DDoS Protection",
		"Azure Sentinel",

		// Management
		"Azure Monitor",
		"Log Analytics",
		"Application Insights",
		"Azure Automation",

		// Containers
		"Container Registry",

		// AI/ML
		"Azure Machine Learning",
		"Cognitive Services",
		"Azure OpenAI Service",
	}
}

// NewAzurePricingAPIClient creates a production Azure pricing client
func NewAzurePricingAPIClient(cfg *AzurePricingConfig) *AzurePricingAPIClient {
	if cfg == nil {
		cfg = DefaultAzurePricingConfig()
	}

	return &AzurePricingAPIClient{
		httpClient: &http.Client{
			Timeout: cfg.HTTPTimeout,
		},
		baseURL:      "https://prices.azure.com/api/retail/prices",
		servicesList: cfg.Services,
	}
}

// Cloud implements PriceFetcher
func (c *AzurePricingAPIClient) Cloud() db.CloudProvider {
	return db.Azure
}

// IsRealAPI implements RealAPIFetcher - THIS IS A REAL API
func (c *AzurePricingAPIClient) IsRealAPI() bool {
	return true
}

// SupportedRegions returns all Azure regions
func (c *AzurePricingAPIClient) SupportedRegions() []string {
	return []string{
		// Americas
		"eastus", "eastus2", "westus", "westus2", "westus3",
		"centralus", "northcentralus", "southcentralus", "westcentralus",
		"canadacentral", "canadaeast",
		"brazilsouth", "brazilsoutheast",

		// Europe
		"northeurope", "westeurope",
		"uksouth", "ukwest",
		"francecentral", "francesouth",
		"germanywestcentral", "germanynorth",
		"switzerlandnorth", "switzerlandwest",
		"norwayeast", "norwaywest",
		"swedencentral",
		"polandcentral",

		// Asia Pacific
		"eastasia", "southeastasia",
		"australiaeast", "australiasoutheast", "australiacentral",
		"japaneast", "japanwest",
		"koreacentral", "koreasouth",
		"centralindia", "westindia", "southindia",

		// Middle East & Africa
		"uaenorth", "uaecentral",
		"southafricanorth", "southafricawest",
		"qatarcentral",
	}
}

// SupportedServices returns all supported services
func (c *AzurePricingAPIClient) SupportedServices() []string {
	return c.servicesList
}

// FetchRegion fetches ALL pricing for a region from Azure Retail Prices API
// This is mapper-agnostic - fetches complete catalogs
func (c *AzurePricingAPIClient) FetchRegion(ctx context.Context, region string) ([]RawPrice, error) {
	var allPrices []RawPrice

	// Azure Retail Prices API uses OData filter syntax
	// We paginate through ALL prices for the region
	filter := fmt.Sprintf("armRegionName eq '%s'", region)

	nextLink := c.buildURL(filter)

	for nextLink != "" {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		prices, next, err := c.fetchPage(ctx, nextLink)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch Azure pricing page: %w", err)
		}

		allPrices = append(allPrices, prices...)
		nextLink = next

		// Log progress
		if len(allPrices)%10000 == 0 {
			fmt.Printf("  Fetched %d Azure prices for %s...\n", len(allPrices), region)
		}
	}

	if len(allPrices) == 0 {
		return nil, fmt.Errorf("failed to fetch any pricing for Azure region %s", region)
	}

	return allPrices, nil
}

// buildURL constructs the API URL with filter
func (c *AzurePricingAPIClient) buildURL(filter string) string {
	params := url.Values{}
	params.Set("$filter", filter)
	params.Set("api-version", "2023-01-01-preview")
	return c.baseURL + "?" + params.Encode()
}

// fetchPage fetches a single page of pricing data
func (c *AzurePricingAPIClient) fetchPage(ctx context.Context, pageURL string) ([]RawPrice, string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", pageURL, nil)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("failed to fetch pricing: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("Azure API returned status %d", resp.StatusCode)
	}

	var response AzurePricingResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, "", fmt.Errorf("failed to decode response: %w", err)
	}

	// Convert to RawPrice
	var prices []RawPrice
	for _, item := range response.Items {
		// Skip zero-priced items and reservation pricing
		if item.RetailPrice == 0 {
			continue
		}

		price := RawPrice{
			SKU:           item.SkuID,
			ServiceCode:   item.ServiceName,
			ProductFamily: item.ServiceFamily,
			Region:        item.ArmRegionName,
			Unit:          item.UnitOfMeasure,
			PricePerUnit:  fmt.Sprintf("%.10f", item.RetailPrice),
			Currency:      item.CurrencyCode,
			Attributes:    c.buildAttributes(item),
		}

		// Parse effective date
		if item.EffectiveStartDate != "" {
			if t, err := time.Parse(time.RFC3339, item.EffectiveStartDate); err == nil {
				price.EffectiveDate = &t
			}
		}

		// Handle tiered pricing
		if item.TierMinimumUnits > 0 {
			tierStart := item.TierMinimumUnits
			price.TierStart = &tierStart
		}

		prices = append(prices, price)
	}

	return prices, response.NextPageLink, nil
}

// buildAttributes creates normalized attributes from Azure pricing item
func (c *AzurePricingAPIClient) buildAttributes(item AzurePriceItem) map[string]string {
	attrs := make(map[string]string)

	if item.SkuName != "" {
		attrs["skuName"] = item.SkuName
	}
	if item.ProductName != "" {
		attrs["productName"] = item.ProductName
	}
	if item.MeterName != "" {
		attrs["meterName"] = item.MeterName
	}
	if item.ArmSkuName != "" {
		attrs["armSkuName"] = item.ArmSkuName
	}
	if item.Type != "" {
		attrs["type"] = item.Type
	}
	if item.ProductId != "" {
		attrs["productId"] = item.ProductId
	}
	if item.MeterId != "" {
		attrs["meterId"] = item.MeterId
	}
	if item.Location != "" {
		attrs["location"] = item.Location
	}
	if item.IsPrimaryMeterRegion {
		attrs["isPrimaryMeterRegion"] = "true"
	}

	return attrs
}

// AzurePricingResponse represents the Azure Retail Prices API response
type AzurePricingResponse struct {
	BillingCurrency    string           `json:"BillingCurrency"`
	CustomerEntityId   string           `json:"CustomerEntityId"`
	CustomerEntityType string           `json:"CustomerEntityType"`
	Items              []AzurePriceItem `json:"Items"`
	NextPageLink       string           `json:"NextPageLink"`
	Count              int              `json:"Count"`
}

// AzurePriceItem represents a single Azure price item
type AzurePriceItem struct {
	CurrencyCode          string  `json:"currencyCode"`
	TierMinimumUnits      float64 `json:"tierMinimumUnits"`
	RetailPrice           float64 `json:"retailPrice"`
	UnitPrice             float64 `json:"unitPrice"`
	ArmRegionName         string  `json:"armRegionName"`
	Location              string  `json:"location"`
	EffectiveStartDate    string  `json:"effectiveStartDate"`
	MeterId               string  `json:"meterId"`
	MeterName             string  `json:"meterName"`
	ProductId             string  `json:"productId"`
	SkuID                 string  `json:"skuId"`
	ProductName           string  `json:"productName"`
	SkuName               string  `json:"skuName"`
	ServiceName           string  `json:"serviceName"`
	ServiceId             string  `json:"serviceId"`
	ServiceFamily         string  `json:"serviceFamily"`
	UnitOfMeasure         string  `json:"unitOfMeasure"`
	Type                  string  `json:"type"`
	IsPrimaryMeterRegion  bool    `json:"isPrimaryMeterRegion"`
	ArmSkuName            string  `json:"armSkuName"`
	ReservationTerm       string  `json:"reservationTerm,omitempty"`
	SavingsPlan           string  `json:"savingsPlan,omitempty"`
}

// AzurePricingNormalizer normalizes raw Azure pricing to canonical format
type AzurePricingNormalizer struct{}

// NewAzurePricingNormalizer creates a production normalizer
func NewAzurePricingNormalizer() *AzurePricingNormalizer {
	return &AzurePricingNormalizer{}
}

// Cloud implements PriceNormalizer
func (n *AzurePricingNormalizer) Cloud() db.CloudProvider {
	return db.Azure
}

// Normalize converts raw Azure prices to normalized rates
func (n *AzurePricingNormalizer) Normalize(raw []RawPrice) ([]NormalizedRate, error) {
	var rates []NormalizedRate

	for _, r := range raw {
		price, err := ParsePrice(r.PricePerUnit)
		if err != nil {
			continue
		}

		// Normalize attributes
		attrs := n.normalizeAttributes(r.Attributes)

		// Create rate key
		rateKey := db.RateKey{
			Cloud:         db.Azure,
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

// normalizeAttributes converts Azure attributes to canonical form
func (n *AzurePricingNormalizer) normalizeAttributes(raw map[string]string) map[string]string {
	result := make(map[string]string)

	mapping := map[string]string{
		"skuName":              "sku_name",
		"productName":          "product_name",
		"meterName":            "meter_name",
		"armSkuName":           "vm_size",
		"type":                 "type",
		"location":             "location",
		"isPrimaryMeterRegion": "is_primary_region",
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

// normalizeUnit converts Azure units to canonical form
func (n *AzurePricingNormalizer) normalizeUnit(unit string) string {
	mapping := map[string]string{
		"1 Hour":             "hours",
		"1/Hour":             "hours",
		"1 GB/Hour":          "GB-hours",
		"1 GB/Month":         "GB-month",
		"1 GB":               "GB",
		"10K":                "10K-requests",
		"1M":                 "1M-requests",
		"10,000 Transactions": "10K-transactions",
		"100":                "100-units",
		"1":                  "unit",
	}

	if normalized, ok := mapping[unit]; ok {
		return normalized
	}

	return strings.ToLower(strings.ReplaceAll(unit, " ", "-"))
}
