ALTER TABLE clusters ADD COLUMN managed_by TEXT;
ALTER TABLE clusters ADD COLUMN maintenance_override_generation INTEGER NOT NULL DEFAULT 0;
-- Operations are JSON documents in records; ignoreMaintenanceWindow is stored
-- in their data alongside the other operation admission fields.
