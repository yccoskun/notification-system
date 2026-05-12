-- Supports admin list queries: newest first, optional filters on status/channel/created_at.
CREATE INDEX idx_notifications_list_created ON notifications (created_at DESC, id DESC);
CREATE INDEX idx_notifications_status_created ON notifications (status, created_at DESC);
