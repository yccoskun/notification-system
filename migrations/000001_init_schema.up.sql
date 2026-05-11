-- 1. Create Enums
CREATE TYPE notification_status AS ENUM ('PENDING', 'PROCESSING', 'SENT', 'FAILED', 'CANCELLED');
CREATE TYPE channel_type AS ENUM ('SMS', 'EMAIL', 'PUSH');

-- 2. Templates Table
CREATE TABLE templates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) UNIQUE NOT NULL,
    channel VARCHAR(50) NOT NULL,
    subject VARCHAR(255), -- Primarily for Email
    body TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 3. Core Notifications Table
CREATE TABLE notifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id UUID,
    recipient VARCHAR(255) NOT NULL,
    channel channel_type NOT NULL,
    template_id UUID REFERENCES templates(id),
    payload JSONB,
    priority SMALLINT DEFAULT 1 CHECK (priority >= 1 AND priority <= 10),
    status notification_status DEFAULT 'PENDING',
    idempotency_key VARCHAR(255) UNIQUE,
    retry_count SMALLINT DEFAULT 0,
    last_error TEXT,
    send_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- The Sweeper Index: Only indexes unresolved messages.
CREATE INDEX idx_notifications_sweeper ON notifications(priority DESC, send_at ASC) WHERE status = 'PENDING';

-- Batch Index: For client queries fetching batch status.
CREATE INDEX idx_notifications_batch ON notifications(batch_id) WHERE batch_id IS NOT NULL;