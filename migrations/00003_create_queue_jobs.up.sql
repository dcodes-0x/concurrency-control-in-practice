CREATE TABLE queue_jobs (
                            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

                            queue_name TEXT NOT NULL,
                            payload TEXT NOT NULL,

                            status TEXT NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending', 'processing', 'completed', 'failed')),

                            available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

                            locked_by TEXT,
                            claimed_at TIMESTAMPTZ,

                            attempt_count INT NOT NULL DEFAULT 0
                                CHECK (attempt_count >= 0),

                            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_queue_jobs_pending
    ON queue_jobs (queue_name, available_at, created_at, id)
    WHERE status = 'pending';

CREATE INDEX idx_queue_jobs_processing
    ON queue_jobs (claimed_at)
    WHERE status = 'processing';