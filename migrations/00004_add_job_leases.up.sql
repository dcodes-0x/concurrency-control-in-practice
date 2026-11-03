ALTER TABLE queue_jobs
    ADD COLUMN claim_token UUID,
    ADD COLUMN lease_expires_at TIMESTAMPTZ;

-- Existing in-progress claims were made without leases.
-- For this local demonstration, reset them so they can
-- be claimed through the new protocol.
--
-- In production, drain or coordinate old workers before
-- changing the ownership protocol.
UPDATE queue_jobs
SET status = 'pending',
    locked_by = NULL,
    claimed_at = NULL,
    claim_token = NULL,
    lease_expires_at = NULL,
    updated_at = NOW()
WHERE status = 'processing';

CREATE INDEX idx_queue_jobs_expired_leases
    ON queue_jobs (queue_name, lease_expires_at, id)
    WHERE status = 'processing';

ALTER TABLE queue_jobs
    ADD CONSTRAINT chk_queue_jobs_lease_ownership
        CHECK (
            status <> 'processing'
                OR (
                locked_by IS NOT NULL
                    AND claim_token IS NOT NULL
                    AND lease_expires_at IS NOT NULL
                )
            );