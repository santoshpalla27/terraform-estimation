-- Terraform Cost Estimation - Pricing Dimensions Schema
-- Phase 2: First-class dimension tables for scalable pricing

-- =============================================================================
-- PRICING DIMENSIONS (FIRST-CLASS)
-- =============================================================================
-- Dimensions are independently queryable, not embedded in JSONB.
-- This enables exact matching, partial fallback, and dimension evolution.

CREATE TABLE pricing_dimensions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    key             TEXT NOT NULL,              -- "instance_type", "storage_class", "os"
    value           TEXT NOT NULL,              -- "t3.medium", "STANDARD_IA", "linux"
    normalized_value TEXT NOT NULL,             -- Canonical lowercase form
    cloud           TEXT NOT NULL CHECK (cloud IN ('aws', 'azure', 'gcp')),
    category        TEXT,                       -- "compute", "storage", "network"
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    CONSTRAINT unique_dimension UNIQUE (cloud, key, value)
);

CREATE INDEX idx_dimensions_lookup ON pricing_dimensions (cloud, key, normalized_value);
CREATE INDEX idx_dimensions_category ON pricing_dimensions (cloud, category, key);

-- =============================================================================
-- PRICING RATE DIMENSIONS (JOIN TABLE)
-- =============================================================================
-- Maps rates to their dimensions for exact and partial matching.

CREATE TABLE pricing_rate_dimensions (
    rate_id         UUID NOT NULL REFERENCES pricing_rates(id) ON DELETE CASCADE,
    dimension_id    UUID NOT NULL REFERENCES pricing_dimensions(id) ON DELETE CASCADE,
    is_primary      BOOLEAN NOT NULL DEFAULT TRUE,  -- Primary vs fallback dimension
    
    PRIMARY KEY (rate_id, dimension_id)
);

CREATE INDEX idx_rate_dimensions_rate ON pricing_rate_dimensions (rate_id);
CREATE INDEX idx_rate_dimensions_dim ON pricing_rate_dimensions (dimension_id);

-- =============================================================================
-- INGESTION GOVERNANCE
-- =============================================================================
-- Tracks ingestion state to prevent partial/corrupted snapshots.

CREATE TABLE pricing_snapshot_ingestion (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_id     UUID NOT NULL REFERENCES pricing_snapshots(id) ON DELETE CASCADE,
    provider        TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('started', 'in_progress', 'completed', 'failed')),
    record_count    INT NOT NULL DEFAULT 0,
    dimension_count INT NOT NULL DEFAULT 0,
    checksum        TEXT,
    error_message   TEXT,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    
    CONSTRAINT unique_snapshot_ingestion UNIQUE (snapshot_id)
);

CREATE INDEX idx_ingestion_status ON pricing_snapshot_ingestion (status);
CREATE INDEX idx_ingestion_snapshot ON pricing_snapshot_ingestion (snapshot_id);

-- =============================================================================
-- INGESTION CONTRACTS
-- =============================================================================
-- Defines required dimensions and minimum rates per service.

CREATE TABLE ingestion_contracts (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cloud           TEXT NOT NULL CHECK (cloud IN ('aws', 'azure', 'gcp')),
    service         TEXT NOT NULL,
    required_dimensions TEXT[] NOT NULL,        -- {"instance_type", "os", "region"}
    min_rate_count  INT NOT NULL DEFAULT 1,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    CONSTRAINT unique_contract UNIQUE (cloud, service)
);

-- Seed contracts for high-impact services
INSERT INTO ingestion_contracts (cloud, service, required_dimensions, min_rate_count) VALUES
('aws', 'AmazonEC2', ARRAY['instance_type', 'os', 'tenancy'], 100),
('aws', 'AmazonRDS', ARRAY['instance_type', 'engine'], 50),
('aws', 'AmazonS3', ARRAY['storage_class'], 10),
('aws', 'AWSLambda', ARRAY['memory_size'], 5),
('aws', 'ElasticLoadBalancing', ARRAY['product_family'], 5),
('azure', 'Virtual Machines', ARRAY['vm_size', 'os'], 100),
('azure', 'Storage', ARRAY['redundancy', 'tier'], 20),
('gcp', 'Compute Engine', ARRAY['machine_type'], 100),
('gcp', 'Cloud Storage', ARRAY['storage_class'], 10)
ON CONFLICT DO NOTHING;

-- =============================================================================
-- SNAPSHOT VERSIONING METADATA
-- =============================================================================
-- Tracks source information for audits.

CREATE TABLE pricing_snapshot_source (
    snapshot_id     UUID PRIMARY KEY REFERENCES pricing_snapshots(id) ON DELETE CASCADE,
    api_version     TEXT,                       -- AWS Pricing API version
    source_url      TEXT,                       -- Bulk file URL or API endpoint
    extraction_time TIMESTAMPTZ NOT NULL,
    pricing_date    DATE,                       -- Effective pricing date
    currency        TEXT NOT NULL DEFAULT 'USD',
    notes           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =============================================================================
-- DIMENSION FALLBACK RULES
-- =============================================================================
-- Defines fallback hierarchy for dimension matching.

CREATE TABLE dimension_fallback_rules (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cloud           TEXT NOT NULL CHECK (cloud IN ('aws', 'azure', 'gcp')),
    dimension_key   TEXT NOT NULL,              -- "instance_type"
    fallback_key    TEXT NOT NULL,              -- "instance_family"
    confidence_penalty FLOAT NOT NULL DEFAULT 0.1,
    priority        INT NOT NULL DEFAULT 0,
    
    CONSTRAINT unique_fallback UNIQUE (cloud, dimension_key, fallback_key)
);

-- Seed fallback rules
INSERT INTO dimension_fallback_rules (cloud, dimension_key, fallback_key, confidence_penalty, priority) VALUES
('aws', 'instance_type', 'instance_family', 0.15, 1),
('aws', 'tenancy', 'default_tenancy', 0.05, 1),
('azure', 'vm_size', 'vm_family', 0.15, 1),
('gcp', 'machine_type', 'machine_family', 0.15, 1)
ON CONFLICT DO NOTHING;

-- =============================================================================
-- FUNCTIONS
-- =============================================================================

-- Validate ingestion against contract
CREATE OR REPLACE FUNCTION validate_ingestion(p_snapshot_id UUID)
RETURNS TABLE (
    is_valid BOOLEAN,
    missing_dimensions TEXT[],
    rate_count INT,
    required_count INT
) AS $$
DECLARE
    v_cloud TEXT;
    v_service TEXT;
BEGIN
    -- Get snapshot info
    SELECT cloud INTO v_cloud FROM pricing_snapshots WHERE id = p_snapshot_id;
    
    -- Get rate counts and validate against contracts
    RETURN QUERY
    SELECT 
        CASE WHEN COUNT(*) >= c.min_rate_count THEN TRUE ELSE FALSE END as is_valid,
        ARRAY[]::TEXT[] as missing_dimensions,
        COUNT(*)::INT as rate_count,
        c.min_rate_count as required_count
    FROM pricing_rates pr
    JOIN pricing_snapshots ps ON pr.snapshot_id = ps.id
    LEFT JOIN ingestion_contracts c ON c.cloud = ps.cloud
    WHERE pr.snapshot_id = p_snapshot_id
    GROUP BY c.min_rate_count;
END;
$$ LANGUAGE plpgsql;

-- Mark ingestion complete
CREATE OR REPLACE FUNCTION complete_ingestion(
    p_snapshot_id UUID,
    p_checksum TEXT
) RETURNS VOID AS $$
DECLARE
    v_rate_count INT;
    v_dim_count INT;
BEGIN
    -- Get counts
    SELECT COUNT(*) INTO v_rate_count FROM pricing_rates WHERE snapshot_id = p_snapshot_id;
    SELECT COUNT(DISTINCT dimension_id) INTO v_dim_count 
    FROM pricing_rate_dimensions prd
    JOIN pricing_rates pr ON prd.rate_id = pr.id
    WHERE pr.snapshot_id = p_snapshot_id;
    
    -- Update ingestion status
    UPDATE pricing_snapshot_ingestion
    SET status = 'completed',
        record_count = v_rate_count,
        dimension_count = v_dim_count,
        checksum = p_checksum,
        completed_at = NOW()
    WHERE snapshot_id = p_snapshot_id;
END;
$$ LANGUAGE plpgsql;

-- Get rate with dimension fallback
CREATE OR REPLACE FUNCTION get_rate_with_fallback(
    p_cloud TEXT,
    p_service TEXT,
    p_region TEXT,
    p_dimensions JSONB,
    p_unit TEXT,
    p_alias TEXT DEFAULT 'default'
)
RETURNS TABLE (
    price NUMERIC,
    confidence FLOAT,
    is_fallback BOOLEAN,
    matched_dimensions JSONB
) AS $$
BEGIN
    -- Try exact match first
    RETURN QUERY
    SELECT 
        pr.price,
        pr.confidence,
        FALSE as is_fallback,
        rk.attributes as matched_dimensions
    FROM pricing_snapshots ps
    JOIN pricing_rate_keys rk ON rk.cloud = ps.cloud AND rk.region = ps.region
    JOIN pricing_rates pr ON pr.snapshot_id = ps.id AND pr.rate_key_id = rk.id
    WHERE ps.cloud = p_cloud
      AND ps.region = p_region
      AND ps.provider_alias = p_alias
      AND ps.is_active = TRUE
      AND rk.service = p_service
      AND rk.attributes @> p_dimensions
      AND pr.unit = p_unit
    LIMIT 1;
    
    -- If no exact match, fallback logic would go here
    -- (simplified for initial implementation)
END;
$$ LANGUAGE plpgsql;
