-- Migration: Fix and Enforce Snapshot Lifecycle State
-- 1. Ensure activate_snapshot sets state='ready'
-- 2. Ensure resolver checks for state='ready'
-- 3. Fix any inconsistent data

-- 1. Redefine activate_snapshot (Force update)
CREATE OR REPLACE FUNCTION activate_snapshot(p_snapshot_id UUID)
RETURNS VOID AS $$
BEGIN
    -- Archive previous active snapshots
    UPDATE pricing_snapshots 
    SET is_active = FALSE, state = 'archived'
    WHERE is_active = TRUE 
    AND cloud = (SELECT cloud FROM pricing_snapshots WHERE id = p_snapshot_id)
    AND region = (SELECT region FROM pricing_snapshots WHERE id = p_snapshot_id)
    AND provider_alias = (SELECT provider_alias FROM pricing_snapshots WHERE id = p_snapshot_id)
    AND id != p_snapshot_id;
    
    -- Activate new snapshot with state='ready'
    UPDATE pricing_snapshots 
    SET is_active = TRUE, state = 'ready'
    WHERE id = p_snapshot_id;
END;
$$ LANGUAGE plpgsql;

-- 2. Update get_active_rate to respect 'ready' state
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
      AND ps.state = 'ready'  -- Enforce ready state
      AND rk.service = p_service
      AND rk.product_family = p_product_family
      AND rk.attributes @> p_attributes
      AND pr.unit = p_unit
    ORDER BY pr.tier_min NULLS FIRST;
END;
$$ LANGUAGE plpgsql;

-- 3. Fix inconsistent data one last time
UPDATE pricing_snapshots
SET state = 'ready'
WHERE is_active = TRUE AND state != 'ready';
