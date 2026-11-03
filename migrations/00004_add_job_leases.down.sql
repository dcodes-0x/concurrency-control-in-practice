ALTER TABLE queue_jobs
DROP CONSTRAINT IF EXISTS chk_queue_jobs_lease_ownership;

DROP INDEX IF EXISTS idx_queue_jobs_expired_leases;

ALTER TABLE queue_jobs
DROP COLUMN IF EXISTS claim_token,
    DROP COLUMN IF EXISTS lease_expires_at;