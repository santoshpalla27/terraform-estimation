-- Migration: Ensure active snapshots are in 'ready' state
-- Fixes issue where pre-existing snapshots got default 'pending' state
UPDATE pricing_snapshots
SET state = 'ready'
WHERE is_active = TRUE AND state = 'pending';
