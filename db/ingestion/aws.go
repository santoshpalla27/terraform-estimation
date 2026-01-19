// Package ingestion - AWS pricing fetcher and normalizer
package ingestion

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"terraform-cost/db"

	"github.com/shopspring/decimal"
)

// AWSFetcher fetches pricing from AWS
type AWSFetcher struct {
	httpClient *http.Client
	regions    []string
}

// NewAWSFetcher creates a new AWS pricing fetcher
func NewAWSFetcher() *AWSFetcher {
	return &AWSFetcher{
		httpClient: &http.Client{},
		regions: []string{
			"us-east-1", "us-east-2", "us-west-1", "us-west-2",
			"eu-west-1", "eu-west-2", "eu-central-1",
			"ap-southeast-1", "ap-southeast-2", "ap-northeast-1",
		},
	}
}

func (f *AWSFetcher) Cloud() db.CloudProvider {
	return db.AWS
}

func (f *AWSFetcher) SupportedRegions() []string {
	return f.regions
}

// IsRealAPI returns whether this fetcher uses real cloud APIs
// Currently returns false as this is stub data - set to true when using AWS Pricing API
func (f *AWSFetcher) IsRealAPI() bool {
	// TODO: Return true when using real AWS Pricing API
	// For now, this is stub data for development
	return false
}

// SupportedServices returns services this fetcher covers
func (f *AWSFetcher) SupportedServices() []string {
	return []string{
		"AmazonEC2",
		"AmazonRDS",
		"AmazonS3",
		"AWSLambda",
		"ElasticLoadBalancing",
		"AmazonDynamoDB",
		"AmazonECS",
		"AmazonElastiCache",
		"AWSSecretsManager",
		"AmazonCloudWatch",
		"AmazonVPC",
		"AWSDataTransfer",
		"AWSQueueService",
		"AmazonSNS",
		"awskms",
		"AWSCertificateManager",
		"AmazonRoute53",
	}
}

// FetchRegion fetches AWS pricing for a region
// Note: In production, use AWS Pricing API or bulk price list files
func (f *AWSFetcher) FetchRegion(ctx context.Context, region string) ([]RawPrice, error) {
	// AWS Pricing API endpoint (simplified)
	// In production, use: https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.json
	
	// For now, return stub data for development
	return f.getStubPrices(region), nil
}

// getStubPrices returns development stub prices for ALL supported AWS services
func (f *AWSFetcher) getStubPrices(region string) []RawPrice {
	return []RawPrice{
		// ============================================================
		// EC2 INSTANCES - aws_instance
		// ============================================================
		// T3 Series (Burstable)
		{SKU: "ec2-t3-nano", ServiceCode: "AmazonEC2", ProductFamily: "Compute Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.0052", Currency: "USD",
			Attributes: map[string]string{"instanceType": "t3.nano", "operatingSystem": "Linux", "tenancy": "Shared"}},
		{SKU: "ec2-t3-micro", ServiceCode: "AmazonEC2", ProductFamily: "Compute Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.0104", Currency: "USD",
			Attributes: map[string]string{"instanceType": "t3.micro", "operatingSystem": "Linux", "tenancy": "Shared"}},
		{SKU: "ec2-t3-small", ServiceCode: "AmazonEC2", ProductFamily: "Compute Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.0208", Currency: "USD",
			Attributes: map[string]string{"instanceType": "t3.small", "operatingSystem": "Linux", "tenancy": "Shared"}},
		{SKU: "ec2-t3-medium", ServiceCode: "AmazonEC2", ProductFamily: "Compute Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.0416", Currency: "USD",
			Attributes: map[string]string{"instanceType": "t3.medium", "operatingSystem": "Linux", "tenancy": "Shared"}},
		{SKU: "ec2-t3-large", ServiceCode: "AmazonEC2", ProductFamily: "Compute Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.0832", Currency: "USD",
			Attributes: map[string]string{"instanceType": "t3.large", "operatingSystem": "Linux", "tenancy": "Shared"}},
		{SKU: "ec2-t3-xlarge", ServiceCode: "AmazonEC2", ProductFamily: "Compute Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.1664", Currency: "USD",
			Attributes: map[string]string{"instanceType": "t3.xlarge", "operatingSystem": "Linux", "tenancy": "Shared"}},
		{SKU: "ec2-t3-2xlarge", ServiceCode: "AmazonEC2", ProductFamily: "Compute Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.3328", Currency: "USD",
			Attributes: map[string]string{"instanceType": "t3.2xlarge", "operatingSystem": "Linux", "tenancy": "Shared"}},

		// M5 Series (General Purpose)
		{SKU: "ec2-m5-large", ServiceCode: "AmazonEC2", ProductFamily: "Compute Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.096", Currency: "USD",
			Attributes: map[string]string{"instanceType": "m5.large", "operatingSystem": "Linux", "tenancy": "Shared"}},
		{SKU: "ec2-m5-xlarge", ServiceCode: "AmazonEC2", ProductFamily: "Compute Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.192", Currency: "USD",
			Attributes: map[string]string{"instanceType": "m5.xlarge", "operatingSystem": "Linux", "tenancy": "Shared"}},
		{SKU: "ec2-m5-2xlarge", ServiceCode: "AmazonEC2", ProductFamily: "Compute Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.384", Currency: "USD",
			Attributes: map[string]string{"instanceType": "m5.2xlarge", "operatingSystem": "Linux", "tenancy": "Shared"}},
		{SKU: "ec2-m5-4xlarge", ServiceCode: "AmazonEC2", ProductFamily: "Compute Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.768", Currency: "USD",
			Attributes: map[string]string{"instanceType": "m5.4xlarge", "operatingSystem": "Linux", "tenancy": "Shared"}},

		// C5 Series (Compute Optimized)
		{SKU: "ec2-c5-large", ServiceCode: "AmazonEC2", ProductFamily: "Compute Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.085", Currency: "USD",
			Attributes: map[string]string{"instanceType": "c5.large", "operatingSystem": "Linux", "tenancy": "Shared"}},
		{SKU: "ec2-c5-xlarge", ServiceCode: "AmazonEC2", ProductFamily: "Compute Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.17", Currency: "USD",
			Attributes: map[string]string{"instanceType": "c5.xlarge", "operatingSystem": "Linux", "tenancy": "Shared"}},
		{SKU: "ec2-c5-2xlarge", ServiceCode: "AmazonEC2", ProductFamily: "Compute Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.34", Currency: "USD",
			Attributes: map[string]string{"instanceType": "c5.2xlarge", "operatingSystem": "Linux", "tenancy": "Shared"}},

		// R5 Series (Memory Optimized)
		{SKU: "ec2-r5-large", ServiceCode: "AmazonEC2", ProductFamily: "Compute Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.126", Currency: "USD",
			Attributes: map[string]string{"instanceType": "r5.large", "operatingSystem": "Linux", "tenancy": "Shared"}},
		{SKU: "ec2-r5-xlarge", ServiceCode: "AmazonEC2", ProductFamily: "Compute Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.252", Currency: "USD",
			Attributes: map[string]string{"instanceType": "r5.xlarge", "operatingSystem": "Linux", "tenancy": "Shared"}},

		// Windows Instances
		{SKU: "ec2-t3-micro-win", ServiceCode: "AmazonEC2", ProductFamily: "Compute Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.0208", Currency: "USD",
			Attributes: map[string]string{"instanceType": "t3.micro", "operatingSystem": "Windows", "tenancy": "Shared"}},
		{SKU: "ec2-m5-large-win", ServiceCode: "AmazonEC2", ProductFamily: "Compute Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.188", Currency: "USD",
			Attributes: map[string]string{"instanceType": "m5.large", "operatingSystem": "Windows", "tenancy": "Shared"}},

		// ============================================================
		// EBS VOLUMES - aws_ebs_volume
		// ============================================================
		{SKU: "ebs-gp3", ServiceCode: "AmazonEC2", ProductFamily: "Storage", Region: region,
			Unit: "GB-Mo", PricePerUnit: "0.08", Currency: "USD",
			Attributes: map[string]string{"volumeApiName": "gp3", "volumeType": "General Purpose"}},
		{SKU: "ebs-gp3-iops", ServiceCode: "AmazonEC2", ProductFamily: "Storage", Region: region,
			Unit: "IOPS-Mo", PricePerUnit: "0.005", Currency: "USD",
			Attributes: map[string]string{"volumeApiName": "gp3", "usagetype": "IOPS"}},
		{SKU: "ebs-gp3-throughput", ServiceCode: "AmazonEC2", ProductFamily: "Storage", Region: region,
			Unit: "MiBps-Mo", PricePerUnit: "0.04", Currency: "USD",
			Attributes: map[string]string{"volumeApiName": "gp3", "usagetype": "Throughput"}},
		{SKU: "ebs-gp2", ServiceCode: "AmazonEC2", ProductFamily: "Storage", Region: region,
			Unit: "GB-Mo", PricePerUnit: "0.10", Currency: "USD",
			Attributes: map[string]string{"volumeApiName": "gp2", "volumeType": "General Purpose"}},
		{SKU: "ebs-io1", ServiceCode: "AmazonEC2", ProductFamily: "Storage", Region: region,
			Unit: "GB-Mo", PricePerUnit: "0.125", Currency: "USD",
			Attributes: map[string]string{"volumeApiName": "io1", "volumeType": "Provisioned IOPS"}},
		{SKU: "ebs-io1-iops", ServiceCode: "AmazonEC2", ProductFamily: "Storage", Region: region,
			Unit: "IOPS-Mo", PricePerUnit: "0.065", Currency: "USD",
			Attributes: map[string]string{"volumeApiName": "io1", "usagetype": "IOPS"}},
		{SKU: "ebs-io2", ServiceCode: "AmazonEC2", ProductFamily: "Storage", Region: region,
			Unit: "GB-Mo", PricePerUnit: "0.125", Currency: "USD",
			Attributes: map[string]string{"volumeApiName": "io2", "volumeType": "Provisioned IOPS"}},
		{SKU: "ebs-st1", ServiceCode: "AmazonEC2", ProductFamily: "Storage", Region: region,
			Unit: "GB-Mo", PricePerUnit: "0.045", Currency: "USD",
			Attributes: map[string]string{"volumeApiName": "st1", "volumeType": "Throughput Optimized"}},
		{SKU: "ebs-sc1", ServiceCode: "AmazonEC2", ProductFamily: "Storage", Region: region,
			Unit: "GB-Mo", PricePerUnit: "0.025", Currency: "USD",
			Attributes: map[string]string{"volumeApiName": "sc1", "volumeType": "Cold HDD"}},
		{SKU: "ebs-standard", ServiceCode: "AmazonEC2", ProductFamily: "Storage", Region: region,
			Unit: "GB-Mo", PricePerUnit: "0.05", Currency: "USD",
			Attributes: map[string]string{"volumeApiName": "standard", "volumeType": "Magnetic"}},

		// EBS Snapshots
		{SKU: "ebs-snapshot", ServiceCode: "AmazonEC2", ProductFamily: "Storage Snapshot", Region: region,
			Unit: "GB-Mo", PricePerUnit: "0.05", Currency: "USD",
			Attributes: map[string]string{"usagetype": "EBS:SnapshotUsage"}},

		// ============================================================
		// RDS - aws_db_instance
		// ============================================================
		// MySQL
		{SKU: "rds-mysql-db.t3.micro", ServiceCode: "AmazonRDS", ProductFamily: "Database Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.017", Currency: "USD",
			Attributes: map[string]string{"instanceType": "db.t3.micro", "databaseEngine": "MySQL"}},
		{SKU: "rds-mysql-db.t3.small", ServiceCode: "AmazonRDS", ProductFamily: "Database Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.034", Currency: "USD",
			Attributes: map[string]string{"instanceType": "db.t3.small", "databaseEngine": "MySQL"}},
		{SKU: "rds-mysql-db.t3.medium", ServiceCode: "AmazonRDS", ProductFamily: "Database Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.068", Currency: "USD",
			Attributes: map[string]string{"instanceType": "db.t3.medium", "databaseEngine": "MySQL"}},
		{SKU: "rds-mysql-db.m5.large", ServiceCode: "AmazonRDS", ProductFamily: "Database Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.171", Currency: "USD",
			Attributes: map[string]string{"instanceType": "db.m5.large", "databaseEngine": "MySQL"}},
		{SKU: "rds-mysql-db.r5.large", ServiceCode: "AmazonRDS", ProductFamily: "Database Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.24", Currency: "USD",
			Attributes: map[string]string{"instanceType": "db.r5.large", "databaseEngine": "MySQL"}},

		// PostgreSQL
		{SKU: "rds-postgres-db.t3.micro", ServiceCode: "AmazonRDS", ProductFamily: "Database Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.018", Currency: "USD",
			Attributes: map[string]string{"instanceType": "db.t3.micro", "databaseEngine": "PostgreSQL"}},
		{SKU: "rds-postgres-db.t3.small", ServiceCode: "AmazonRDS", ProductFamily: "Database Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.036", Currency: "USD",
			Attributes: map[string]string{"instanceType": "db.t3.small", "databaseEngine": "PostgreSQL"}},
		{SKU: "rds-postgres-db.m5.large", ServiceCode: "AmazonRDS", ProductFamily: "Database Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.178", Currency: "USD",
			Attributes: map[string]string{"instanceType": "db.m5.large", "databaseEngine": "PostgreSQL"}},

		// Aurora MySQL
		{SKU: "rds-aurora-mysql-db.t3.small", ServiceCode: "AmazonRDS", ProductFamily: "Database Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.041", Currency: "USD",
			Attributes: map[string]string{"instanceType": "db.t3.small", "databaseEngine": "Aurora MySQL"}},
		{SKU: "rds-aurora-mysql-db.r5.large", ServiceCode: "AmazonRDS", ProductFamily: "Database Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.29", Currency: "USD",
			Attributes: map[string]string{"instanceType": "db.r5.large", "databaseEngine": "Aurora MySQL"}},

		// RDS Storage
		{SKU: "rds-storage-gp2", ServiceCode: "AmazonRDS", ProductFamily: "Database Storage", Region: region,
			Unit: "GB-Mo", PricePerUnit: "0.115", Currency: "USD",
			Attributes: map[string]string{"volumeType": "General Purpose (SSD)"}},
		{SKU: "rds-storage-io1", ServiceCode: "AmazonRDS", ProductFamily: "Database Storage", Region: region,
			Unit: "GB-Mo", PricePerUnit: "0.125", Currency: "USD",
			Attributes: map[string]string{"volumeType": "Provisioned IOPS (SSD)"}},
		{SKU: "rds-iops", ServiceCode: "AmazonRDS", ProductFamily: "Provisioned IOPS", Region: region,
			Unit: "IOPS-Mo", PricePerUnit: "0.10", Currency: "USD",
			Attributes: map[string]string{"usagetype": "RDS:PIOPS"}},

		// ============================================================
		// S3 - aws_s3_bucket
		// ============================================================
		// Storage Classes
		{SKU: "s3-standard", ServiceCode: "AmazonS3", ProductFamily: "Storage", Region: region,
			Unit: "GB-Mo", PricePerUnit: "0.023", Currency: "USD",
			Attributes: map[string]string{"storageClass": "General Purpose", "volumeType": "Standard"}},
		{SKU: "s3-intelligent-tiering", ServiceCode: "AmazonS3", ProductFamily: "Storage", Region: region,
			Unit: "GB-Mo", PricePerUnit: "0.023", Currency: "USD",
			Attributes: map[string]string{"storageClass": "Intelligent-Tiering"}},
		{SKU: "s3-standard-ia", ServiceCode: "AmazonS3", ProductFamily: "Storage", Region: region,
			Unit: "GB-Mo", PricePerUnit: "0.0125", Currency: "USD",
			Attributes: map[string]string{"storageClass": "Infrequent Access"}},
		{SKU: "s3-onezone-ia", ServiceCode: "AmazonS3", ProductFamily: "Storage", Region: region,
			Unit: "GB-Mo", PricePerUnit: "0.01", Currency: "USD",
			Attributes: map[string]string{"storageClass": "One Zone - Infrequent Access"}},
		{SKU: "s3-glacier-instant", ServiceCode: "AmazonS3", ProductFamily: "Storage", Region: region,
			Unit: "GB-Mo", PricePerUnit: "0.004", Currency: "USD",
			Attributes: map[string]string{"storageClass": "Glacier Instant Retrieval"}},
		{SKU: "s3-glacier-flexible", ServiceCode: "AmazonS3", ProductFamily: "Storage", Region: region,
			Unit: "GB-Mo", PricePerUnit: "0.0036", Currency: "USD",
			Attributes: map[string]string{"storageClass": "Glacier Flexible Retrieval"}},
		{SKU: "s3-glacier-deep", ServiceCode: "AmazonS3", ProductFamily: "Storage", Region: region,
			Unit: "GB-Mo", PricePerUnit: "0.00099", Currency: "USD",
			Attributes: map[string]string{"storageClass": "Glacier Deep Archive"}},

		// S3 Requests
		{SKU: "s3-put-requests", ServiceCode: "AmazonS3", ProductFamily: "API Requests", Region: region,
			Unit: "Requests", PricePerUnit: "0.000005", Currency: "USD",
			Attributes: map[string]string{"operation": "PUT", "storageClass": "Standard"}},
		{SKU: "s3-get-requests", ServiceCode: "AmazonS3", ProductFamily: "API Requests", Region: region,
			Unit: "Requests", PricePerUnit: "0.0000004", Currency: "USD",
			Attributes: map[string]string{"operation": "GET", "storageClass": "Standard"}},
		{SKU: "s3-list-requests", ServiceCode: "AmazonS3", ProductFamily: "API Requests", Region: region,
			Unit: "Requests", PricePerUnit: "0.000005", Currency: "USD",
			Attributes: map[string]string{"operation": "LIST", "storageClass": "Standard"}},

		// S3 Data Transfer
		{SKU: "s3-data-transfer-out", ServiceCode: "AmazonS3", ProductFamily: "Data Transfer", Region: region,
			Unit: "GB", PricePerUnit: "0.09", Currency: "USD",
			Attributes: map[string]string{"transferType": "AWS Outbound", "fromLocation": region}},

		// ============================================================
		// NAT GATEWAY - aws_nat_gateway
		// ============================================================
		{SKU: "nat-gateway-hour", ServiceCode: "AmazonEC2", ProductFamily: "NAT Gateway", Region: region,
			Unit: "Hrs", PricePerUnit: "0.045", Currency: "USD",
			Attributes: map[string]string{"usagetype": "NatGateway-Hours"}},
		{SKU: "nat-gateway-data", ServiceCode: "AmazonEC2", ProductFamily: "NAT Gateway", Region: region,
			Unit: "GB", PricePerUnit: "0.045", Currency: "USD",
			Attributes: map[string]string{"usagetype": "NatGateway-Bytes"}},

		// ============================================================
		// LAMBDA - aws_lambda_function
		// ============================================================
		{SKU: "lambda-requests", ServiceCode: "AWSLambda", ProductFamily: "Serverless", Region: region,
			Unit: "Requests", PricePerUnit: "0.0000002", Currency: "USD",
			Attributes: map[string]string{"group": "AWS-Lambda-Requests"}},
		{SKU: "lambda-duration-128", ServiceCode: "AWSLambda", ProductFamily: "Serverless", Region: region,
			Unit: "GB-Second", PricePerUnit: "0.0000166667", Currency: "USD",
			Attributes: map[string]string{"group": "AWS-Lambda-Duration", "memorySize": "128"}},
		{SKU: "lambda-duration-512", ServiceCode: "AWSLambda", ProductFamily: "Serverless", Region: region,
			Unit: "GB-Second", PricePerUnit: "0.0000166667", Currency: "USD",
			Attributes: map[string]string{"group": "AWS-Lambda-Duration", "memorySize": "512"}},
		{SKU: "lambda-duration-1024", ServiceCode: "AWSLambda", ProductFamily: "Serverless", Region: region,
			Unit: "GB-Second", PricePerUnit: "0.0000166667", Currency: "USD",
			Attributes: map[string]string{"group": "AWS-Lambda-Duration", "memorySize": "1024"}},
		{SKU: "lambda-provisioned-concurrency", ServiceCode: "AWSLambda", ProductFamily: "Serverless", Region: region,
			Unit: "GB-Second", PricePerUnit: "0.000004", Currency: "USD",
			Attributes: map[string]string{"group": "AWS-Lambda-Provisioned-Concurrency"}},

		// ============================================================
		// LOAD BALANCERS - aws_lb, aws_alb, aws_nlb
		// ============================================================
		{SKU: "alb-hour", ServiceCode: "ElasticLoadBalancing", ProductFamily: "Load Balancer", Region: region,
			Unit: "Hrs", PricePerUnit: "0.0225", Currency: "USD",
			Attributes: map[string]string{"productFamily": "Load Balancer-Application", "usagetype": "LoadBalancerUsage"}},
		{SKU: "alb-lcu", ServiceCode: "ElasticLoadBalancing", ProductFamily: "Load Balancer", Region: region,
			Unit: "LCU-Hrs", PricePerUnit: "0.008", Currency: "USD",
			Attributes: map[string]string{"productFamily": "Load Balancer-Application", "usagetype": "LCUUsage"}},
		{SKU: "nlb-hour", ServiceCode: "ElasticLoadBalancing", ProductFamily: "Load Balancer", Region: region,
			Unit: "Hrs", PricePerUnit: "0.0225", Currency: "USD",
			Attributes: map[string]string{"productFamily": "Load Balancer-Network"}},
		{SKU: "nlb-lcu", ServiceCode: "ElasticLoadBalancing", ProductFamily: "Load Balancer", Region: region,
			Unit: "NLCU-Hrs", PricePerUnit: "0.006", Currency: "USD",
			Attributes: map[string]string{"productFamily": "Load Balancer-Network", "usagetype": "NLCUUsage"}},
		{SKU: "clb-hour", ServiceCode: "ElasticLoadBalancing", ProductFamily: "Load Balancer", Region: region,
			Unit: "Hrs", PricePerUnit: "0.025", Currency: "USD",
			Attributes: map[string]string{"productFamily": "Load Balancer-Classic"}},
		{SKU: "clb-data", ServiceCode: "ElasticLoadBalancing", ProductFamily: "Load Balancer", Region: region,
			Unit: "GB", PricePerUnit: "0.008", Currency: "USD",
			Attributes: map[string]string{"productFamily": "Load Balancer-Classic", "usagetype": "DataProcessing"}},

		// ============================================================
		// ECS / FARGATE - aws_ecs_service, aws_ecs_task_definition
		// ============================================================
		{SKU: "fargate-vcpu", ServiceCode: "AmazonECS", ProductFamily: "Compute", Region: region,
			Unit: "vCPU-Hours", PricePerUnit: "0.04048", Currency: "USD",
			Attributes: map[string]string{"cputype": "ARM", "usagetype": "Fargate-vCPU-Hours"}},
		{SKU: "fargate-vcpu-x86", ServiceCode: "AmazonECS", ProductFamily: "Compute", Region: region,
			Unit: "vCPU-Hours", PricePerUnit: "0.04048", Currency: "USD",
			Attributes: map[string]string{"cputype": "x86", "usagetype": "Fargate-vCPU-Hours"}},
		{SKU: "fargate-memory", ServiceCode: "AmazonECS", ProductFamily: "Compute", Region: region,
			Unit: "GB-Hours", PricePerUnit: "0.004445", Currency: "USD",
			Attributes: map[string]string{"usagetype": "Fargate-GB-Hours"}},
		{SKU: "fargate-spot-vcpu", ServiceCode: "AmazonECS", ProductFamily: "Compute", Region: region,
			Unit: "vCPU-Hours", PricePerUnit: "0.01334384", Currency: "USD",
			Attributes: map[string]string{"usagetype": "Fargate-SpotUsage-vCPU-Hours"}},
		{SKU: "fargate-spot-memory", ServiceCode: "AmazonECS", ProductFamily: "Compute", Region: region,
			Unit: "GB-Hours", PricePerUnit: "0.00146489", Currency: "USD",
			Attributes: map[string]string{"usagetype": "Fargate-SpotUsage-GB-Hours"}},

		// ============================================================
		// DYNAMODB - aws_dynamodb_table
		// ============================================================
		{SKU: "dynamodb-wcu", ServiceCode: "AmazonDynamoDB", ProductFamily: "Provisioned Throughput", Region: region,
			Unit: "WriteCapacityUnit-Hrs", PricePerUnit: "0.00065", Currency: "USD",
			Attributes: map[string]string{"group": "DDB-WriteUnits"}},
		{SKU: "dynamodb-rcu", ServiceCode: "AmazonDynamoDB", ProductFamily: "Provisioned Throughput", Region: region,
			Unit: "ReadCapacityUnit-Hrs", PricePerUnit: "0.00013", Currency: "USD",
			Attributes: map[string]string{"group": "DDB-ReadUnits"}},
		{SKU: "dynamodb-ondemand-write", ServiceCode: "AmazonDynamoDB", ProductFamily: "Amazon DynamoDB PayPerRequest Throughput", Region: region,
			Unit: "WriteRequestUnits", PricePerUnit: "0.00000125", Currency: "USD",
			Attributes: map[string]string{"group": "DDB-WriteUnits", "usagetype": "PayPerRequest"}},
		{SKU: "dynamodb-ondemand-read", ServiceCode: "AmazonDynamoDB", ProductFamily: "Amazon DynamoDB PayPerRequest Throughput", Region: region,
			Unit: "ReadRequestUnits", PricePerUnit: "0.00000025", Currency: "USD",
			Attributes: map[string]string{"group": "DDB-ReadUnits", "usagetype": "PayPerRequest"}},
		{SKU: "dynamodb-storage", ServiceCode: "AmazonDynamoDB", ProductFamily: "Database Storage", Region: region,
			Unit: "GB-Mo", PricePerUnit: "0.25", Currency: "USD",
			Attributes: map[string]string{"usagetype": "TimedStorage-ByteHrs"}},
		{SKU: "dynamodb-gsi-wcu", ServiceCode: "AmazonDynamoDB", ProductFamily: "Provisioned Throughput", Region: region,
			Unit: "WriteCapacityUnit-Hrs", PricePerUnit: "0.00065", Currency: "USD",
			Attributes: map[string]string{"group": "DDB-WriteUnits", "indexType": "GSI"}},
		{SKU: "dynamodb-gsi-rcu", ServiceCode: "AmazonDynamoDB", ProductFamily: "Provisioned Throughput", Region: region,
			Unit: "ReadCapacityUnit-Hrs", PricePerUnit: "0.00013", Currency: "USD",
			Attributes: map[string]string{"group": "DDB-ReadUnits", "indexType": "GSI"}},

		// ============================================================
		// ELASTICACHE - aws_elasticache_cluster
		// ============================================================
		{SKU: "elasticache-redis-t3.micro", ServiceCode: "AmazonElastiCache", ProductFamily: "Cache Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.017", Currency: "USD",
			Attributes: map[string]string{"instanceType": "cache.t3.micro", "cacheEngine": "Redis"}},
		{SKU: "elasticache-redis-t3.small", ServiceCode: "AmazonElastiCache", ProductFamily: "Cache Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.034", Currency: "USD",
			Attributes: map[string]string{"instanceType": "cache.t3.small", "cacheEngine": "Redis"}},
		{SKU: "elasticache-redis-m5.large", ServiceCode: "AmazonElastiCache", ProductFamily: "Cache Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.156", Currency: "USD",
			Attributes: map[string]string{"instanceType": "cache.m5.large", "cacheEngine": "Redis"}},
		{SKU: "elasticache-redis-r5.large", ServiceCode: "AmazonElastiCache", ProductFamily: "Cache Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.224", Currency: "USD",
			Attributes: map[string]string{"instanceType": "cache.r5.large", "cacheEngine": "Redis"}},
		{SKU: "elasticache-memcached-t3.micro", ServiceCode: "AmazonElastiCache", ProductFamily: "Cache Instance", Region: region,
			Unit: "Hrs", PricePerUnit: "0.017", Currency: "USD",
			Attributes: map[string]string{"instanceType": "cache.t3.micro", "cacheEngine": "Memcached"}},

		// ============================================================
		// SECRETS MANAGER - aws_secretsmanager_secret
		// ============================================================
		{SKU: "secrets-manager-secret", ServiceCode: "AWSSecretsManager", ProductFamily: "Secret", Region: region,
			Unit: "Secrets", PricePerUnit: "0.40", Currency: "USD",
			Attributes: map[string]string{"usagetype": "SecretMonth"}},
		{SKU: "secrets-manager-api", ServiceCode: "AWSSecretsManager", ProductFamily: "API Request", Region: region,
			Unit: "Requests", PricePerUnit: "0.05", Currency: "USD",
			Attributes: map[string]string{"usagetype": "API-Requests-Per10K"}},

		// ============================================================
		// CLOUDWATCH - aws_cloudwatch_metric_alarm, aws_cloudwatch_log_group
		// ============================================================
		{SKU: "cloudwatch-metric", ServiceCode: "AmazonCloudWatch", ProductFamily: "Metric", Region: region,
			Unit: "Metrics", PricePerUnit: "0.30", Currency: "USD",
			Attributes: map[string]string{"usagetype": "CW:MetricMonitorUsage"}},
		{SKU: "cloudwatch-alarm", ServiceCode: "AmazonCloudWatch", ProductFamily: "Alarm", Region: region,
			Unit: "Alarms", PricePerUnit: "0.10", Currency: "USD",
			Attributes: map[string]string{"usagetype": "CW:AlarmMonitorUsage", "alarmType": "Standard"}},
		{SKU: "cloudwatch-alarm-highres", ServiceCode: "AmazonCloudWatch", ProductFamily: "Alarm", Region: region,
			Unit: "Alarms", PricePerUnit: "0.30", Currency: "USD",
			Attributes: map[string]string{"usagetype": "CW:AlarmMonitorUsage", "alarmType": "High Resolution"}},
		{SKU: "cloudwatch-logs-ingest", ServiceCode: "AmazonCloudWatch", ProductFamily: "Logs", Region: region,
			Unit: "GB", PricePerUnit: "0.50", Currency: "USD",
			Attributes: map[string]string{"usagetype": "DataProcessing-Bytes"}},
		{SKU: "cloudwatch-logs-storage", ServiceCode: "AmazonCloudWatch", ProductFamily: "Logs", Region: region,
			Unit: "GB-Mo", PricePerUnit: "0.03", Currency: "USD",
			Attributes: map[string]string{"usagetype": "TimedStorage-ByteHrs"}},

		// ============================================================
		// VPC - aws_vpc, aws_subnet, aws_internet_gateway
		// ============================================================
		{SKU: "vpc-endpoint-hour", ServiceCode: "AmazonVPC", ProductFamily: "VpcEndpoint", Region: region,
			Unit: "Hrs", PricePerUnit: "0.01", Currency: "USD",
			Attributes: map[string]string{"usagetype": "VpcEndpoint-Hours"}},
		{SKU: "vpc-endpoint-data", ServiceCode: "AmazonVPC", ProductFamily: "VpcEndpoint", Region: region,
			Unit: "GB", PricePerUnit: "0.01", Currency: "USD",
			Attributes: map[string]string{"usagetype": "VpcEndpoint-Bytes"}},
		{SKU: "vpn-connection-hour", ServiceCode: "AmazonVPC", ProductFamily: "VPN Connection", Region: region,
			Unit: "Hrs", PricePerUnit: "0.05", Currency: "USD",
			Attributes: map[string]string{"usagetype": "VPN-Hours"}},

		// ============================================================
		// DATA TRANSFER (Cross-region, Internet)
		// ============================================================
		{SKU: "data-transfer-out-internet", ServiceCode: "AWSDataTransfer", ProductFamily: "Data Transfer", Region: region,
			Unit: "GB", PricePerUnit: "0.09", Currency: "USD",
			Attributes: map[string]string{"transferType": "AWS Outbound", "toLocation": "External"}},
		{SKU: "data-transfer-in-internet", ServiceCode: "AWSDataTransfer", ProductFamily: "Data Transfer", Region: region,
			Unit: "GB", PricePerUnit: "0.00", Currency: "USD",
			Attributes: map[string]string{"transferType": "AWS Inbound", "fromLocation": "External"}},
		{SKU: "data-transfer-inter-region", ServiceCode: "AWSDataTransfer", ProductFamily: "Data Transfer", Region: region,
			Unit: "GB", PricePerUnit: "0.02", Currency: "USD",
			Attributes: map[string]string{"transferType": "InterRegion Outbound"}},
		{SKU: "data-transfer-inter-az", ServiceCode: "AWSDataTransfer", ProductFamily: "Data Transfer", Region: region,
			Unit: "GB", PricePerUnit: "0.01", Currency: "USD",
			Attributes: map[string]string{"transferType": "IntraRegion"}},

		// ============================================================
		// SQS - aws_sqs_queue
		// ============================================================
		{SKU: "sqs-standard", ServiceCode: "AWSQueueService", ProductFamily: "Queue", Region: region,
			Unit: "Requests", PricePerUnit: "0.0000004", Currency: "USD",
			Attributes: map[string]string{"queueType": "Standard", "usagetype": "Requests"}},
		{SKU: "sqs-fifo", ServiceCode: "AWSQueueService", ProductFamily: "Queue", Region: region,
			Unit: "Requests", PricePerUnit: "0.00000050", Currency: "USD",
			Attributes: map[string]string{"queueType": "FIFO", "usagetype": "Requests"}},

		// ============================================================
		// SNS - aws_sns_topic
		// ============================================================
		{SKU: "sns-publish", ServiceCode: "AmazonSNS", ProductFamily: "Notification", Region: region,
			Unit: "Requests", PricePerUnit: "0.50", Currency: "USD",
			Attributes: map[string]string{"usagetype": "PublishAPI-Requests-Per1M"}},
		{SKU: "sns-delivery-http", ServiceCode: "AmazonSNS", ProductFamily: "Notification", Region: region,
			Unit: "Notifications", PricePerUnit: "0.06", Currency: "USD",
			Attributes: map[string]string{"usagetype": "DeliveryAttempts-HTTP-Per100K"}},

		// ============================================================
		// KMS - aws_kms_key
		// ============================================================
		{SKU: "kms-key", ServiceCode: "awskms", ProductFamily: "Key Management", Region: region,
			Unit: "Keys", PricePerUnit: "1.00", Currency: "USD",
			Attributes: map[string]string{"usagetype": "KMS-Keys"}},
		{SKU: "kms-requests", ServiceCode: "awskms", ProductFamily: "Key Management", Region: region,
			Unit: "Requests", PricePerUnit: "0.03", Currency: "USD",
			Attributes: map[string]string{"usagetype": "KMS-Requests-Per10K"}},

		// ============================================================
		// ACM - aws_acm_certificate (Free for public certs)
		// ============================================================
		{SKU: "acm-private-ca", ServiceCode: "AWSCertificateManager", ProductFamily: "Certificate Authority", Region: region,
			Unit: "CA", PricePerUnit: "400.00", Currency: "USD",
			Attributes: map[string]string{"usagetype": "PrivateCA"}},
		{SKU: "acm-private-cert", ServiceCode: "AWSCertificateManager", ProductFamily: "Certificate", Region: region,
			Unit: "Certificates", PricePerUnit: "0.75", Currency: "USD",
			Attributes: map[string]string{"usagetype": "PrivateCertificate"}},

		// ============================================================
		// ROUTE53 - aws_route53_zone, aws_route53_record
		// ============================================================
		{SKU: "route53-hosted-zone", ServiceCode: "AmazonRoute53", ProductFamily: "Hosted Zone", Region: region,
			Unit: "HostedZone", PricePerUnit: "0.50", Currency: "USD",
			Attributes: map[string]string{"usagetype": "HostedZone"}},
		{SKU: "route53-queries", ServiceCode: "AmazonRoute53", ProductFamily: "DNS Query", Region: region,
			Unit: "Queries", PricePerUnit: "0.40", Currency: "USD",
			Attributes: map[string]string{"usagetype": "DNS-Queries-Per1M"}},
		{SKU: "route53-health-check", ServiceCode: "AmazonRoute53", ProductFamily: "Health Check", Region: region,
			Unit: "HealthCheck", PricePerUnit: "0.50", Currency: "USD",
			Attributes: map[string]string{"usagetype": "HealthCheck-Basic"}},
	}
}

// AWSNormalizer normalizes AWS pricing data
type AWSNormalizer struct{}

func NewAWSNormalizer() *AWSNormalizer {
	return &AWSNormalizer{}
}

func (n *AWSNormalizer) Cloud() db.CloudProvider {
	return db.AWS
}

// Normalize converts raw AWS prices to normalized rates
func (n *AWSNormalizer) Normalize(raw []RawPrice) ([]NormalizedRate, error) {
	var rates []NormalizedRate
	
	for _, r := range raw {
		price, err := ParsePrice(r.PricePerUnit)
		if err != nil {
			continue // Skip unparseable prices
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
			Confidence: 1.0, // Direct from AWS API
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

func (n *AWSNormalizer) normalizeAttributes(raw map[string]string) map[string]string {
	result := make(map[string]string)
	
	for k, v := range raw {
		// Normalize key names
		key := n.normalizeKey(k)
		// Normalize values
		val := strings.ToLower(strings.TrimSpace(v))
		result[key] = val
	}
	
	return result
}

func (n *AWSNormalizer) normalizeKey(k string) string {
	// Map AWS attribute names to canonical names
	mapping := map[string]string{
		"instanceType":     "instance_type",
		"operatingSystem":  "os",
		"tenancy":          "tenancy",
		"volumeApiName":    "volume_type",
		"databaseEngine":   "engine",
		"usagetype":        "usage_type",
		"productFamily":    "product_family",
		"group":            "group",
	}
	
	if canonical, ok := mapping[k]; ok {
		return canonical
	}
	return strings.ToLower(strings.ReplaceAll(k, " ", "_"))
}

func (n *AWSNormalizer) normalizeUnit(unit string) string {
	// Normalize unit names
	mapping := map[string]string{
		"Hrs":        "hours",
		"GB-Mo":      "GB-month",
		"GB":         "GB",
		"Requests":   "requests",
		"GB-Second":  "GB-seconds",
	}
	
	if normalized, ok := mapping[unit]; ok {
		return normalized
	}
	return strings.ToLower(unit)
}

// fetchFromAWSAPI is the production implementation (placeholder)
func fetchFromAWSAPI(ctx context.Context, region, service string) ([]byte, error) {
	// In production:
	// 1. Use AWS Pricing API: pricing.GetProducts()
	// 2. Or download bulk price list: https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/{service}/current/index.json
	return nil, fmt.Errorf("not implemented: use AWS SDK in production")
}

// parseAWSPriceList parses AWS bulk price list JSON
func parseAWSPriceList(data []byte) ([]RawPrice, error) {
	var result struct {
		Products map[string]struct {
			SKU           string            `json:"sku"`
			ProductFamily string            `json:"productFamily"`
			Attributes    map[string]string `json:"attributes"`
		} `json:"products"`
		Terms struct {
			OnDemand map[string]map[string]struct {
				PriceDimensions map[string]struct {
					Unit         string `json:"unit"`
					PricePerUnit struct {
						USD string `json:"USD"`
					} `json:"pricePerUnit"`
				} `json:"priceDimensions"`
			} `json:"OnDemand"`
		} `json:"terms"`
	}
	
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	
	// Parse would continue here...
	return nil, nil
}
