-- Terraform Cost Estimation - Scale Hardening Schema
-- Phase 3: Coverage tracking, dimension filtering, drift detection

-- =============================================================================
-- SNAPSHOT COVERAGE TRACKING
-- =============================================================================
-- Tracks completeness per service to prevent partial ingestion.

CREATE TABLE pricing_snapshot_coverage (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_id     UUID NOT NULL REFERENCES pricing_snapshots(id) ON DELETE CASCADE,
    service         TEXT NOT NULL,
    rate_count      INT NOT NULL DEFAULT 0,
    dimension_count INT NOT NULL DEFAULT 0,
    coverage_percent FLOAT NOT NULL DEFAULT 0,
    missing_dimensions TEXT[] DEFAULT '{}',
    is_complete     BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    CONSTRAINT unique_snapshot_service_coverage UNIQUE (snapshot_id, service)
);

CREATE INDEX idx_coverage_snapshot ON pricing_snapshot_coverage (snapshot_id);
CREATE INDEX idx_coverage_complete ON pricing_snapshot_coverage (snapshot_id, is_complete);

-- =============================================================================
-- DIMENSION ALLOW-LISTS
-- =============================================================================
-- Prevents dimension explosion by filtering at ingestion time.

CREATE TABLE dimension_allowlists (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cloud           TEXT NOT NULL CHECK (cloud IN ('aws', 'azure', 'gcp')),
    service         TEXT NOT NULL,
    dimension_key   TEXT NOT NULL,
    is_required     BOOLEAN NOT NULL DEFAULT FALSE,
    priority        INT NOT NULL DEFAULT 0,  -- Higher = more important
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    CONSTRAINT unique_dimension_allowlist UNIQUE (cloud, service, dimension_key)
);

CREATE INDEX idx_allowlist_service ON dimension_allowlists (cloud, service);

-- Seed dimension allow-lists for AWS high-impact services
INSERT INTO dimension_allowlists (cloud, service, dimension_key, is_required, priority) VALUES
-- EC2
('aws', 'AmazonEC2', 'instance_type', TRUE, 100),
('aws', 'AmazonEC2', 'os', TRUE, 90),
('aws', 'AmazonEC2', 'tenancy', FALSE, 80),
('aws', 'AmazonEC2', 'region', TRUE, 100),
('aws', 'AmazonEC2', 'volume_type', FALSE, 70),
('aws', 'AmazonEC2', 'capacity_status', FALSE, 50),
-- RDS
('aws', 'AmazonRDS', 'instance_type', TRUE, 100),
('aws', 'AmazonRDS', 'engine', TRUE, 90),
('aws', 'AmazonRDS', 'deployment', FALSE, 70),
('aws', 'AmazonRDS', 'region', TRUE, 100),
-- Lambda
('aws', 'AWSLambda', 'memory_size', FALSE, 80),
('aws', 'AWSLambda', 'architecture', FALSE, 60),
('aws', 'AWSLambda', 'region', TRUE, 100),
-- S3
('aws', 'AmazonS3', 'storage_class', TRUE, 100),
('aws', 'AmazonS3', 'region', TRUE, 100),
-- ELB
('aws', 'ElasticLoadBalancing', 'product_family', TRUE, 100),
('aws', 'ElasticLoadBalancing', 'region', TRUE, 100),
-- DynamoDB
('aws', 'AmazonDynamoDB', 'read_capacity', FALSE, 80),
('aws', 'AmazonDynamoDB', 'write_capacity', FALSE, 80),
('aws', 'AmazonDynamoDB', 'region', TRUE, 100)
ON CONFLICT DO NOTHING;

-- =============================================================================
-- PRICING DRIFT DETECTION
-- =============================================================================
-- Stores differences between snapshots for alerting.

CREATE TABLE pricing_drift_records (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    old_snapshot_id UUID NOT NULL REFERENCES pricing_snapshots(id) ON DELETE CASCADE,
    new_snapshot_id UUID NOT NULL REFERENCES pricing_snapshots(id) ON DELETE CASCADE,
    cloud           TEXT NOT NULL,
    service         TEXT NOT NULL,
    product_family  TEXT NOT NULL,
    dimension_key   TEXT,
    dimension_value TEXT,
    old_price       NUMERIC(20,10) NOT NULL,
    new_price       NUMERIC(20,10) NOT NULL,
    price_delta     NUMERIC(20,10) NOT NULL,
    percent_change  FLOAT NOT NULL,
    unit            TEXT NOT NULL,
    drift_type      TEXT NOT NULL CHECK (drift_type IN ('increase', 'decrease', 'new', 'removed')),
    is_significant  BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    CONSTRAINT unique_drift_record UNIQUE (old_snapshot_id, new_snapshot_id, service, dimension_key, dimension_value, unit)
);

CREATE INDEX idx_drift_snapshots ON pricing_drift_records (old_snapshot_id, new_snapshot_id);
CREATE INDEX idx_drift_significant ON pricing_drift_records (is_significant) WHERE is_significant = TRUE;
CREATE INDEX idx_drift_service ON pricing_drift_records (service, drift_type);

-- Drift summary per snapshot comparison
CREATE TABLE pricing_drift_summary (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    old_snapshot_id     UUID NOT NULL REFERENCES pricing_snapshots(id) ON DELETE CASCADE,
    new_snapshot_id     UUID NOT NULL REFERENCES pricing_snapshots(id) ON DELETE CASCADE,
    cloud               TEXT NOT NULL,
    total_changes       INT NOT NULL DEFAULT 0,
    price_increases     INT NOT NULL DEFAULT 0,
    price_decreases     INT NOT NULL DEFAULT 0,
    new_rates           INT NOT NULL DEFAULT 0,
    removed_rates       INT NOT NULL DEFAULT 0,
    avg_percent_change  FLOAT,
    max_percent_change  FLOAT,
    significant_changes INT NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    CONSTRAINT unique_drift_summary UNIQUE (old_snapshot_id, new_snapshot_id)
);

-- =============================================================================
-- FUNCTIONS
-- =============================================================================

-- Calculate snapshot coverage for a service
CREATE OR REPLACE FUNCTION calculate_service_coverage(
    p_snapshot_id UUID,
    p_service TEXT
) RETURNS TABLE (
    rate_count INT,
    dimension_count INT, 
    coverage_percent FLOAT,
    missing_dimensions TEXT[],
    is_complete BOOLEAN
) AS $$
DECLARE
    v_cloud TEXT;
    v_required_dims TEXT[];
    v_present_dims TEXT[];
    v_missing TEXT[];
    v_rate_count INT;
    v_min_rates INT;
BEGIN
    -- Get cloud from snapshot
    SELECT cloud INTO v_cloud FROM pricing_snapshots WHERE id = p_snapshot_id;
    
    -- Get required dimensions
    SELECT ARRAY_AGG(dimension_key) INTO v_required_dims
    FROM dimension_allowlists
    WHERE cloud = v_cloud AND service = p_service AND is_required = TRUE;
    
    -- Get present dimensions from rates
    SELECT ARRAY_AGG(DISTINCT key) INTO v_present_dims
    FROM pricing_rates pr
    JOIN pricing_rate_keys rk ON pr.rate_key_id = rk.id
    CROSS JOIN LATERAL jsonb_object_keys(rk.attributes) AS key
    WHERE pr.snapshot_id = p_snapshot_id AND rk.service = p_service;
    
    -- Count rates
    SELECT COUNT(*) INTO v_rate_count
    FROM pricing_rates pr
    JOIN pricing_rate_keys rk ON pr.rate_key_id = rk.id
    WHERE pr.snapshot_id = p_snapshot_id AND rk.service = p_service;
    
    -- Get minimum rate requirement
    SELECT min_rate_count INTO v_min_rates
    FROM ingestion_contracts
    WHERE cloud = v_cloud AND service = p_service;
    
    v_min_rates := COALESCE(v_min_rates, 1);
    
    -- Calculate missing dimensions
    IF v_required_dims IS NOT NULL AND v_present_dims IS NOT NULL THEN
        SELECT ARRAY_AGG(dim) INTO v_missing
        FROM unnest(v_required_dims) AS dim
        WHERE dim != ALL(v_present_dims);
    ELSE
        v_missing := v_required_dims;
    END IF;
    
    v_missing := COALESCE(v_missing, '{}');
    
    RETURN QUERY SELECT
        v_rate_count,
        COALESCE(array_length(v_present_dims, 1), 0),
        CASE WHEN v_min_rates > 0 THEN LEAST(v_rate_count::FLOAT / v_min_rates * 100, 100) ELSE 100.0 END,
        v_missing,
        v_rate_count >= v_min_rates AND array_length(v_missing, 1) IS NULL;
END;
$$ LANGUAGE plpgsql;

-- Detect drift between two snapshots
CREATE OR REPLACE FUNCTION detect_pricing_drift(
    p_old_snapshot_id UUID,
    p_new_snapshot_id UUID,
    p_significance_threshold FLOAT DEFAULT 0.05  -- 5% change is significant
) RETURNS INT AS $$
DECLARE
    v_changes_count INT := 0;
    v_cloud TEXT;
BEGIN
    -- Get cloud
    SELECT cloud INTO v_cloud FROM pricing_snapshots WHERE id = p_old_snapshot_id;
    
    -- Insert price changes
    INSERT INTO pricing_drift_records (
        old_snapshot_id, new_snapshot_id, cloud, service, product_family,
        dimension_key, dimension_value, old_price, new_price, price_delta,
        percent_change, unit, drift_type, is_significant
    )
    SELECT 
        p_old_snapshot_id,
        p_new_snapshot_id,
        v_cloud,
        COALESCE(old_rk.service, new_rk.service),
        COALESCE(old_rk.product_family, new_rk.product_family),
        NULL, NULL,
        COALESCE(old_pr.price, 0),
        COALESCE(new_pr.price, 0),
        COALESCE(new_pr.price, 0) - COALESCE(old_pr.price, 0),
        CASE 
            WHEN old_pr.price IS NULL OR old_pr.price = 0 THEN 100
            WHEN new_pr.price IS NULL THEN -100
            ELSE ((new_pr.price - old_pr.price) / old_pr.price * 100)::FLOAT
        END,
        COALESCE(old_pr.unit, new_pr.unit),
        CASE 
            WHEN old_pr.price IS NULL THEN 'new'
            WHEN new_pr.price IS NULL THEN 'removed'
            WHEN new_pr.price > old_pr.price THEN 'increase'
            ELSE 'decrease'
        END,
        ABS(CASE 
            WHEN old_pr.price IS NULL OR old_pr.price = 0 THEN 1
            WHEN new_pr.price IS NULL THEN 1
            ELSE ((new_pr.price - old_pr.price) / old_pr.price)::FLOAT
        END) >= p_significance_threshold
    FROM (
        SELECT pr.*, rk.service, rk.product_family, rk.attributes
        FROM pricing_rates pr
        JOIN pricing_rate_keys rk ON pr.rate_key_id = rk.id
        WHERE pr.snapshot_id = p_old_snapshot_id
    ) old_pr
    FULL OUTER JOIN (
        SELECT pr.*, rk.service, rk.product_family, rk.attributes
        FROM pricing_rates pr
        JOIN pricing_rate_keys rk ON pr.rate_key_id = rk.id
        WHERE pr.snapshot_id = p_new_snapshot_id
    ) new_pr ON old_pr.service = new_pr.service 
        AND old_pr.product_family = new_pr.product_family
        AND old_pr.attributes = new_pr.attributes
        AND old_pr.unit = new_pr.unit
    CROSS JOIN LATERAL (
        SELECT old_pr.service, old_pr.product_family AS old_rk_service
    ) AS old_rk
    CROSS JOIN LATERAL (
        SELECT new_pr.service, new_pr.product_family AS new_rk_service
    ) AS new_rk
    WHERE old_pr.price IS DISTINCT FROM new_pr.price
    ON CONFLICT DO NOTHING;
    
    GET DIAGNOSTICS v_changes_count = ROW_COUNT;
    
    -- Create summary
    INSERT INTO pricing_drift_summary (
        old_snapshot_id, new_snapshot_id, cloud,
        total_changes, price_increases, price_decreases,
        new_rates, removed_rates, avg_percent_change,
        max_percent_change, significant_changes
    )
    SELECT
        p_old_snapshot_id,
        p_new_snapshot_id,
        v_cloud,
        COUNT(*),
        COUNT(*) FILTER (WHERE drift_type = 'increase'),
        COUNT(*) FILTER (WHERE drift_type = 'decrease'),
        COUNT(*) FILTER (WHERE drift_type = 'new'),
        COUNT(*) FILTER (WHERE drift_type = 'removed'),
        AVG(percent_change),
        MAX(ABS(percent_change)),
        COUNT(*) FILTER (WHERE is_significant)
    FROM pricing_drift_records
    WHERE old_snapshot_id = p_old_snapshot_id AND new_snapshot_id = p_new_snapshot_id
    ON CONFLICT (old_snapshot_id, new_snapshot_id) DO UPDATE SET
        total_changes = EXCLUDED.total_changes,
        price_increases = EXCLUDED.price_increases,
        price_decreases = EXCLUDED.price_decreases,
        new_rates = EXCLUDED.new_rates,
        removed_rates = EXCLUDED.removed_rates,
        avg_percent_change = EXCLUDED.avg_percent_change,
        max_percent_change = EXCLUDED.max_percent_change,
        significant_changes = EXCLUDED.significant_changes;
    
    RETURN v_changes_count;
END;
$$ LANGUAGE plpgsql;

-- Check if snapshot is complete for strict mode
CREATE OR REPLACE FUNCTION is_snapshot_complete(
    p_snapshot_id UUID,
    p_min_coverage FLOAT DEFAULT 80.0
) RETURNS BOOLEAN AS $$
DECLARE
    v_all_complete BOOLEAN;
BEGIN
    SELECT bool_and(is_complete AND coverage_percent >= p_min_coverage)
    INTO v_all_complete
    FROM pricing_snapshot_coverage
    WHERE snapshot_id = p_snapshot_id;
    
    RETURN COALESCE(v_all_complete, FALSE);
END;
$$ LANGUAGE plpgsql;
