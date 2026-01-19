// Package regions - Billable region registry for all cloud providers
// This is the source of truth for which regions can be billed.
package regions

import "terraform-cost/db"

// CloudRegion represents a billable region for a cloud provider
type CloudRegion struct {
	Provider      db.CloudProvider
	Region        string
	DisplayName   string
	Billable      bool
	PricingSource string // "api", "manual", "govcloud", "china"
}

// Registry holds all billable regions for all providers
type Registry struct {
	regions map[db.CloudProvider][]CloudRegion
}

// NewRegistry creates a registry with all known billable regions
func NewRegistry() *Registry {
	r := &Registry{
		regions: make(map[db.CloudProvider][]CloudRegion),
	}
	r.regions[db.AWS] = awsRegions()
	r.regions[db.Azure] = azureRegions()
	r.regions[db.GCP] = gcpRegions()
	return r
}

// GetBillableRegions returns all billable regions for a provider
func (r *Registry) GetBillableRegions(provider db.CloudProvider) []CloudRegion {
	var billable []CloudRegion
	for _, region := range r.regions[provider] {
		if region.Billable {
			billable = append(billable, region)
		}
	}
	return billable
}

// GetAllRegions returns all regions for a provider (billable and non-billable)
func (r *Registry) GetAllRegions(provider db.CloudProvider) []CloudRegion {
	return r.regions[provider]
}

// GetRegion returns a specific region by name
func (r *Registry) GetRegion(provider db.CloudProvider, region string) *CloudRegion {
	for _, reg := range r.regions[provider] {
		if reg.Region == region {
			return &reg
		}
	}
	return nil
}

// IsBillable checks if a region is billable
func (r *Registry) IsBillable(provider db.CloudProvider, region string) bool {
	reg := r.GetRegion(provider, region)
	return reg != nil && reg.Billable
}

// awsRegions returns all AWS regions
func awsRegions() []CloudRegion {
	return []CloudRegion{
		// US Regions
		{db.AWS, "us-east-1", "US East (N. Virginia)", true, "api"},
		{db.AWS, "us-east-2", "US East (Ohio)", true, "api"},
		{db.AWS, "us-west-1", "US West (N. California)", true, "api"},
		{db.AWS, "us-west-2", "US West (Oregon)", true, "api"},

		// Canada
		{db.AWS, "ca-central-1", "Canada (Central)", true, "api"},
		{db.AWS, "ca-west-1", "Canada West (Calgary)", true, "api"},

		// Europe
		{db.AWS, "eu-west-1", "Europe (Ireland)", true, "api"},
		{db.AWS, "eu-west-2", "Europe (London)", true, "api"},
		{db.AWS, "eu-west-3", "Europe (Paris)", true, "api"},
		{db.AWS, "eu-central-1", "Europe (Frankfurt)", true, "api"},
		{db.AWS, "eu-central-2", "Europe (Zurich)", true, "api"},
		{db.AWS, "eu-north-1", "Europe (Stockholm)", true, "api"},
		{db.AWS, "eu-south-1", "Europe (Milan)", true, "api"},
		{db.AWS, "eu-south-2", "Europe (Spain)", true, "api"},

		// Asia Pacific
		{db.AWS, "ap-northeast-1", "Asia Pacific (Tokyo)", true, "api"},
		{db.AWS, "ap-northeast-2", "Asia Pacific (Seoul)", true, "api"},
		{db.AWS, "ap-northeast-3", "Asia Pacific (Osaka)", true, "api"},
		{db.AWS, "ap-southeast-1", "Asia Pacific (Singapore)", true, "api"},
		{db.AWS, "ap-southeast-2", "Asia Pacific (Sydney)", true, "api"},
		{db.AWS, "ap-southeast-3", "Asia Pacific (Jakarta)", true, "api"},
		{db.AWS, "ap-southeast-4", "Asia Pacific (Melbourne)", true, "api"},
		{db.AWS, "ap-south-1", "Asia Pacific (Mumbai)", true, "api"},
		{db.AWS, "ap-south-2", "Asia Pacific (Hyderabad)", true, "api"},
		{db.AWS, "ap-east-1", "Asia Pacific (Hong Kong)", true, "api"},

		// South America
		{db.AWS, "sa-east-1", "South America (São Paulo)", true, "api"},

		// Middle East
		{db.AWS, "me-south-1", "Middle East (Bahrain)", true, "api"},
		{db.AWS, "me-central-1", "Middle East (UAE)", true, "api"},
		{db.AWS, "il-central-1", "Israel (Tel Aviv)", true, "api"},

		// Africa
		{db.AWS, "af-south-1", "Africa (Cape Town)", true, "api"},

		// GovCloud (separate pricing universe)
		{db.AWS, "us-gov-west-1", "AWS GovCloud (US-West)", true, "govcloud"},
		{db.AWS, "us-gov-east-1", "AWS GovCloud (US-East)", true, "govcloud"},

		// China (separate pricing universe)
		{db.AWS, "cn-north-1", "China (Beijing)", true, "china"},
		{db.AWS, "cn-northwest-1", "China (Ningxia)", true, "china"},
	}
}

// azureRegions returns all Azure regions
func azureRegions() []CloudRegion {
	return []CloudRegion{
		// US Regions
		{db.Azure, "eastus", "East US", true, "api"},
		{db.Azure, "eastus2", "East US 2", true, "api"},
		{db.Azure, "centralus", "Central US", true, "api"},
		{db.Azure, "northcentralus", "North Central US", true, "api"},
		{db.Azure, "southcentralus", "South Central US", true, "api"},
		{db.Azure, "westcentralus", "West Central US", true, "api"},
		{db.Azure, "westus", "West US", true, "api"},
		{db.Azure, "westus2", "West US 2", true, "api"},
		{db.Azure, "westus3", "West US 3", true, "api"},

		// Canada
		{db.Azure, "canadacentral", "Canada Central", true, "api"},
		{db.Azure, "canadaeast", "Canada East", true, "api"},

		// Europe
		{db.Azure, "northeurope", "North Europe", true, "api"},
		{db.Azure, "westeurope", "West Europe", true, "api"},
		{db.Azure, "uksouth", "UK South", true, "api"},
		{db.Azure, "ukwest", "UK West", true, "api"},
		{db.Azure, "francecentral", "France Central", true, "api"},
		{db.Azure, "francesouth", "France South", true, "api"},
		{db.Azure, "germanywestcentral", "Germany West Central", true, "api"},
		{db.Azure, "germanynorth", "Germany North", true, "api"},
		{db.Azure, "switzerlandnorth", "Switzerland North", true, "api"},
		{db.Azure, "switzerlandwest", "Switzerland West", true, "api"},
		{db.Azure, "norwayeast", "Norway East", true, "api"},
		{db.Azure, "norwaywest", "Norway West", true, "api"},
		{db.Azure, "swedencentral", "Sweden Central", true, "api"},
		{db.Azure, "polandcentral", "Poland Central", true, "api"},
		{db.Azure, "italynorth", "Italy North", true, "api"},

		// Asia Pacific
		{db.Azure, "eastasia", "East Asia", true, "api"},
		{db.Azure, "southeastasia", "Southeast Asia", true, "api"},
		{db.Azure, "japaneast", "Japan East", true, "api"},
		{db.Azure, "japanwest", "Japan West", true, "api"},
		{db.Azure, "australiaeast", "Australia East", true, "api"},
		{db.Azure, "australiasoutheast", "Australia Southeast", true, "api"},
		{db.Azure, "australiacentral", "Australia Central", true, "api"},
		{db.Azure, "centralindia", "Central India", true, "api"},
		{db.Azure, "southindia", "South India", true, "api"},
		{db.Azure, "westindia", "West India", true, "api"},
		{db.Azure, "koreacentral", "Korea Central", true, "api"},
		{db.Azure, "koreasouth", "Korea South", true, "api"},

		// South America
		{db.Azure, "brazilsouth", "Brazil South", true, "api"},
		{db.Azure, "brazilsoutheast", "Brazil Southeast", true, "api"},

		// Middle East & Africa
		{db.Azure, "uaenorth", "UAE North", true, "api"},
		{db.Azure, "uaecentral", "UAE Central", true, "api"},
		{db.Azure, "southafricanorth", "South Africa North", true, "api"},
		{db.Azure, "southafricawest", "South Africa West", true, "api"},
		{db.Azure, "qatarcentral", "Qatar Central", true, "api"},
		{db.Azure, "israelcentral", "Israel Central", true, "api"},

		// Government (separate)
		{db.Azure, "usgovvirginia", "US Gov Virginia", true, "govcloud"},
		{db.Azure, "usgovarizona", "US Gov Arizona", true, "govcloud"},
		{db.Azure, "usgovtexas", "US Gov Texas", true, "govcloud"},

		// China (separate)
		{db.Azure, "chinaeast", "China East", true, "china"},
		{db.Azure, "chinanorth", "China North", true, "china"},
		{db.Azure, "chinaeast2", "China East 2", true, "china"},
		{db.Azure, "chinanorth2", "China North 2", true, "china"},
	}
}

// gcpRegions returns all GCP regions
func gcpRegions() []CloudRegion {
	return []CloudRegion{
		// US
		{db.GCP, "us-central1", "Iowa", true, "api"},
		{db.GCP, "us-east1", "South Carolina", true, "api"},
		{db.GCP, "us-east4", "Northern Virginia", true, "api"},
		{db.GCP, "us-east5", "Columbus", true, "api"},
		{db.GCP, "us-south1", "Dallas", true, "api"},
		{db.GCP, "us-west1", "Oregon", true, "api"},
		{db.GCP, "us-west2", "Los Angeles", true, "api"},
		{db.GCP, "us-west3", "Salt Lake City", true, "api"},
		{db.GCP, "us-west4", "Las Vegas", true, "api"},

		// Canada
		{db.GCP, "northamerica-northeast1", "Montréal", true, "api"},
		{db.GCP, "northamerica-northeast2", "Toronto", true, "api"},

		// South America
		{db.GCP, "southamerica-east1", "São Paulo", true, "api"},
		{db.GCP, "southamerica-west1", "Santiago", true, "api"},

		// Europe
		{db.GCP, "europe-north1", "Finland", true, "api"},
		{db.GCP, "europe-west1", "Belgium", true, "api"},
		{db.GCP, "europe-west2", "London", true, "api"},
		{db.GCP, "europe-west3", "Frankfurt", true, "api"},
		{db.GCP, "europe-west4", "Netherlands", true, "api"},
		{db.GCP, "europe-west6", "Zürich", true, "api"},
		{db.GCP, "europe-west8", "Milan", true, "api"},
		{db.GCP, "europe-west9", "Paris", true, "api"},
		{db.GCP, "europe-west10", "Berlin", true, "api"},
		{db.GCP, "europe-west12", "Turin", true, "api"},
		{db.GCP, "europe-central2", "Warsaw", true, "api"},
		{db.GCP, "europe-southwest1", "Madrid", true, "api"},

		// Asia Pacific
		{db.GCP, "asia-east1", "Taiwan", true, "api"},
		{db.GCP, "asia-east2", "Hong Kong", true, "api"},
		{db.GCP, "asia-northeast1", "Tokyo", true, "api"},
		{db.GCP, "asia-northeast2", "Osaka", true, "api"},
		{db.GCP, "asia-northeast3", "Seoul", true, "api"},
		{db.GCP, "asia-south1", "Mumbai", true, "api"},
		{db.GCP, "asia-south2", "Delhi", true, "api"},
		{db.GCP, "asia-southeast1", "Singapore", true, "api"},
		{db.GCP, "asia-southeast2", "Jakarta", true, "api"},

		// Australia
		{db.GCP, "australia-southeast1", "Sydney", true, "api"},
		{db.GCP, "australia-southeast2", "Melbourne", true, "api"},

		// Middle East
		{db.GCP, "me-central1", "Doha", true, "api"},
		{db.GCP, "me-central2", "Dammam", true, "api"},
		{db.GCP, "me-west1", "Tel Aviv", true, "api"},

		// Africa
		{db.GCP, "africa-south1", "Johannesburg", true, "api"},
	}
}
