DROP INDEX IF EXISTS uq_drip_job_unique_key;
--;;
ALTER TABLE drip_job DROP COLUMN unique_key_active;
--;;
CREATE UNIQUE INDEX uq_drip_job_unique_key ON drip_job (unique_key);
