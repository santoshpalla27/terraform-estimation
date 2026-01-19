-- Migration: Add region aliases table for storage deduplication
-- This enables storing pricing once per canonical region while mapping aliases

-- Region aliases table
CREATE TABLE IF NOT EXISTS pricing_region_aliases (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    provider TEXT NOT NULL,
    region TEXT NOT NULL,
    canonical_region TEXT NOT NULL,
    equivalence_hash TEXT NOT NULL,
    rate_count INT NOT NULL DEFAULT 0,
    detected_at TIMESTAMP NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(provider, region)
);

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_region_aliases_lookup 
ON pricing_region_aliases(provider, region);

CREATE INDEX IF NOT EXISTS idx_region_aliases_canonical 
ON pricing_region_aliases(provider, canonical_region);

-- Function to get canonical region
CREATE OR REPLACE FUNCTION get_canonical_region(p_provider TEXT, p_region TEXT)
RETURNS TEXT AS $$
DECLARE
    v_canonical TEXT;
BEGIN
    SELECT canonical_region INTO v_canonical
    FROM pricing_region_aliases
    WHERE provider = p_provider AND region = p_region;
    
    -- If no alias found, region is its own canonical
    IF v_canonical IS NULL THEN
        RETURN p_region;
    END IF;
    
    RETURN v_canonical;
END;
$$ LANGUAGE plpgsql;

-- View to show region groups
CREATE OR REPLACE VIEW region_equivalence_groups AS
SELECT 
    provider,
    canonical_region,
    COUNT(*) as alias_count,
    ARRAY_AGG(region ORDER BY region) as regions,
    MIN(detected_at) as first_detected
FROM pricing_region_aliases
GROUP BY provider, canonical_region;

-- Comments
COMMENT ON TABLE pricing_region_aliases IS 'Maps regions to canonical regions for storage optimization';
COMMENT ON COLUMN pricing_region_aliases.equivalence_hash IS 'Hash of all pricing data used to determine equivalence';
