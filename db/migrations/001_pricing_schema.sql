-- Terraform Cost Estimation - Pricing Database Schema
-- PostgreSQL 16+

-- =============================================================================
-- PRICING SNAPSHOTS (IMMUTABLE)
-- =============================================================================
-- Each snapshot represents a point-in-time capture of pricing data.
-- Snapshots are immutable once created - no updates allowed.

CREATE TABLE pricing_snapshots (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cloud           TEXT NOT NULL CHECK (cloud IN ('aws', 'azure', 'gcp')),
    region          TEXT NOT NULL,
    provider_alias  TEXT NOT NULL DEFAULT 'default',
    source          TEXT NOT NULL,  -- aws_pricing_api, azure_retail, gcp_catalog
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valid_from      TIMESTAMPTZ NOT NULL,
    valid_to        TIMESTAMPTZ,
    hash            TEXT NOT NULL,  -- SHA256 content hash for deduplication
    version         TEXT NOT NULL DEFAULT '1.0',
    is_active       BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    CONSTRAINT unique_snapshot UNIQUE (cloud, region, provider_alias, hash)
);

-- Ensure only one active snapshot per cloud/region/alias combination
CREATE UNIQUE INDEX idx_active_snapshot 
ON pricing_snapshots (cloud, region, provider_alias) 
WHERE is_active = TRUE;

CREATE INDEX idx_snapshots_lookup ON pricing_snapshots (cloud, region, provider_alias, is_active);

-- =============================================================================
-- RATE KEYS (NORMALIZED)
-- =============================================================================
-- RateKeys are the unique identifiers for pricing lookups.
-- Matches the RateKey struct in Go exactly.

CREATE TABLE pricing_rate_keys (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cloud           TEXT NOT NULL CHECK (cloud IN ('aws', 'azure', 'gcp')),
    service         TEXT NOT NULL,        -- AmazonEC2, AmazonRDS, etc.
    product_family  TEXT NOT NULL,        -- Compute Instance, Storage, etc.
    region          TEXT NOT NULL,
    attributes      JSONB NOT NULL,       -- {instanceType, os, tenancy, tier, etc.}
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    CONSTRAINT unique_rate_key UNIQUE (cloud, service, product_family, region, attributes)
);

CREATE INDEX idx_rate_keys_lookup 
ON pricing_rate_keys (cloud, service, product_family, region);

CREATE INDEX idx_rate_keys_attributes 
ON pricing_rate_keys USING GIN (attributes);

-- =============================================================================
-- PRICING RATES (THE MONEY)
-- =============================================================================
-- Actual prices tied to snapshots and rate keys.
-- Supports tiered pricing (S3, data transfer, free tiers).

CREATE TABLE pricing_rates (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_id     UUID NOT NULL REFERENCES pricing_snapshots(id) ON DELETE CASCADE,
    rate_key_id     UUID NOT NULL REFERENCES pricing_rate_keys(id) ON DELETE CASCADE,
    unit            TEXT NOT NULL,        -- hours, GB-month, requests, GB
    price           NUMERIC(20, 10) NOT NULL,
    currency        TEXT NOT NULL DEFAULT 'USD',
    confidence      FLOAT NOT NULL DEFAULT 1.0 CHECK (confidence > 0 AND confidence <= 1),
    tier_min        NUMERIC,              -- NULL for non-tiered pricing
    tier_max        NUMERIC,              -- NULL for unlimited tier
    effective_date  DATE,                 -- When this price became effective
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    CONSTRAINT unique_rate UNIQUE (snapshot_id, rate_key_id, unit, tier_min, tier_max)
);

CREATE INDEX idx_rates_snapshot ON pricing_rates (snapshot_id);
CREATE INDEX idx_rates_rate_key ON pricing_rates (rate_key_id);
CREATE INDEX idx_rates_lookup ON pricing_rates (snapshot_id, rate_key_id);

-- =============================================================================
-- PRICING METADATA
-- =============================================================================
-- Optional key-value metadata for snapshots.

CREATE TABLE pricing_metadata (
    snapshot_id     UUID NOT NULL REFERENCES pricing_snapshots(id) ON DELETE CASCADE,
    key             TEXT NOT NULL,
    value           TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    PRIMARY KEY (snapshot_id, key)
);

-- =============================================================================
-- SERVICE CATALOG (Reference Data)
-- =============================================================================
-- Maps service names to product families for validation.

CREATE TABLE service_catalog (
    cloud           TEXT NOT NULL CHECK (cloud IN ('aws', 'azure', 'gcp')),
    service         TEXT NOT NULL,
    product_family  TEXT NOT NULL,
    description     TEXT,
    is_billable     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    PRIMARY KEY (cloud, service, product_family)
);

-- =============================================================================
-- FUNCTIONS
-- =============================================================================

-- Activate a snapshot (deactivates all others for same cloud/region/alias)
CREATE OR REPLACE FUNCTION activate_snapshot(p_snapshot_id UUID)
RETURNS VOID AS $$
DECLARE
    v_cloud TEXT;
    v_region TEXT;
    v_alias TEXT;
BEGIN
    -- Get snapshot details
    SELECT cloud, region, provider_alias 
    INTO v_cloud, v_region, v_alias
    FROM pricing_snapshots 
    WHERE id = p_snapshot_id;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Snapshot not found: %', p_snapshot_id;
    END IF;
    
    -- Deactivate existing
    UPDATE pricing_snapshots
    SET is_active = FALSE
    WHERE cloud = v_cloud 
      AND region = v_region 
      AND provider_alias = v_alias
      AND is_active = TRUE;
    
    -- Activate new
    UPDATE pricing_snapshots
    SET is_active = TRUE
    WHERE id = p_snapshot_id;
END;
$$ LANGUAGE plpgsql;

-- Get rate for a rate key from active snapshot
CREATE OR REPLACE FUNCTION get_active_rate(
    p_cloud TEXT,
    p_service TEXT,
    p_product_family TEXT,
    p_region TEXT,
    p_attributes JSONB,
    p_unit TEXT,
    p_provider_alias TEXT DEFAULT 'default'
)
RETURNS TABLE (
    price NUMERIC,
    currency TEXT,
    confidence FLOAT,
    tier_min NUMERIC,
    tier_max NUMERIC,
    snapshot_id UUID
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        pr.price,
        pr.currency,
        pr.confidence,
        pr.tier_min,
        pr.tier_max,
        ps.id as snapshot_id
    FROM pricing_snapshots ps
    JOIN pricing_rate_keys rk ON rk.cloud = ps.cloud AND rk.region = ps.region
    JOIN pricing_rates pr ON pr.snapshot_id = ps.id AND pr.rate_key_id = rk.id
    WHERE ps.cloud = p_cloud
      AND ps.region = p_region
      AND ps.provider_alias = p_provider_alias
      AND ps.is_active = TRUE
      AND rk.service = p_service
      AND rk.product_family = p_product_family
      AND rk.attributes @> p_attributes
      AND pr.unit = p_unit
    ORDER BY pr.tier_min NULLS FIRST;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- SEED DATA - Common Services
-- =============================================================================

INSERT INTO service_catalog (cloud, service, product_family, description, is_billable) VALUES
-- AWS
('aws', 'AmazonEC2', 'Compute Instance', 'EC2 On-Demand Instances', true),
('aws', 'AmazonEC2', 'Storage', 'EBS Volumes', true),
('aws', 'AmazonEC2', 'Data Transfer', 'EC2 Data Transfer', true),
('aws', 'AmazonRDS', 'Database Instance', 'RDS Instances', true),
('aws', 'AmazonRDS', 'Database Storage', 'RDS Storage', true),
('aws', 'AmazonS3', 'Storage', 'S3 Standard Storage', true),
('aws', 'AmazonS3', 'Data Transfer', 'S3 Data Transfer', true),
('aws', 'AWSLambda', 'Serverless', 'Lambda Functions', true),
('aws', 'AmazonDynamoDB', 'Database', 'DynamoDB Tables', true),
('aws', 'AmazonVPC', 'Networking', 'NAT Gateway', true),
('aws', 'ElasticLoadBalancing', 'Networking', 'Load Balancers', true),
-- Azure
('azure', 'Virtual Machines', 'Compute', 'Azure VMs', true),
('azure', 'Storage', 'Storage', 'Managed Disks', true),
('azure', 'SQL Database', 'Database', 'Azure SQL', true),
('azure', 'Cosmos DB', 'Database', 'CosmosDB', true),
-- GCP
('gcp', 'Compute Engine', 'Compute', 'GCE Instances', true),
('gcp', 'Cloud Storage', 'Storage', 'GCS Buckets', true),
('gcp', 'Cloud SQL', 'Database', 'Cloud SQL Instances', true),
('gcp', 'BigQuery', 'Analytics', 'BigQuery', true)
ON CONFLICT DO NOTHING;
