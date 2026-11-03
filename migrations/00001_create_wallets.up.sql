CREATE TABLE wallets (
                         id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

                         balance BIGINT NOT NULL
                             CHECK (balance >= 0),

                         version BIGINT NOT NULL DEFAULT 0,

                         created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

                         updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);