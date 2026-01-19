-- Migration: Add snapshot lifecycle state
-- Ensures resolver only sees ready snapshots

-- Add state column with default 'pending'
ALTER TABLE pricing_snapshots 
ADD COLUMN IF NOT EXISTS state TEXT NOT NULL DEFAULT 'pending';

-- Valid states:
-- 'pending'  - Created, not yet validated
-- 'staging'  - Validated, backup written, not yet committed
-- 'ready'    - Committed, resolver can use
-- 'failed'   - Validation or commit failed
-- 'archived' - Superseded by newer snapshot

COMMENT ON COLUMN pricing_snapshots.state IS 
'Lifecycle state: pending|staging|ready|failed|archived. Resolver ONLY queries ready snapshots.';

-- Index for fast resolver queries
CREATE INDEX IF NOT EXISTS idx_snapshots_state 
ON pricing_snapshots(cloud, region, provider_alias, state) 
WHERE state = 'ready';

-- Update activate_snapshot function to set state='ready'
CREATE OR REPLACE FUNCTION activate_snapshot(snapshot_id UUID)
RETURNS VOID AS $$
BEGIN
    -- Archive previous active snapshots
    UPDATE pricing_snapshots 
    SET is_active = FALSE, state = 'archived'
    WHERE is_active = TRUE 
    AND cloud = (SELECT cloud FROM pricing_snapshots WHERE id = snapshot_id)
    AND region = (SELECT region FROM pricing_snapshots WHERE id = snapshot_id)
    AND provider_alias = (SELECT provider_alias FROM pricing_snapshots WHERE id = snapshot_id)
    AND id != snapshot_id;
    
    -- Activate new snapshot with state='ready'
    UPDATE pricing_snapshots 
    SET is_active = TRUE, state = 'ready'
    WHERE id = snapshot_id;
END;
$$ LANGUAGE plpgsql;

-- Function to mark snapshot as failed
CREATE OR REPLACE FUNCTION fail_snapshot(snapshot_id UUID, error_msg TEXT)
RETURNS VOID AS $$
BEGIN
    UPDATE pricing_snapshots 
    SET state = 'failed', is_active = FALSE
    WHERE id = snapshot_id;
END;
$$ LANGUAGE plpgsql;
