-- unique_key_active holds unique_key only while the job's state is in its
-- unique_states bitmask (bit positions as in s-exp.drip.job/state->bit:
-- available=7 cancelled=6 completed=5 discarded=4 pending=3 retryable=2
-- running=1 scheduled=0). Indexing it gives the same semantics as the
-- PostgreSQL partial unique index: a job holds its slot exactly while it is
-- in a covered state, whatever transition got it there.
ALTER TABLE drip_job ADD COLUMN unique_key_active BLOB AS (
  CASE WHEN (unique_states & (CASE state
                                WHEN 'available' THEN 128
                                WHEN 'cancelled' THEN 64
                                WHEN 'completed' THEN 32
                                WHEN 'discarded' THEN 16
                                WHEN 'pending'   THEN 8
                                WHEN 'retryable' THEN 4
                                WHEN 'running'   THEN 2
                                WHEN 'scheduled' THEN 1
                                ELSE 0 END)) <> 0
       THEN unique_key END) VIRTUAL;
--;;
DROP INDEX IF EXISTS uq_drip_job_unique_key;
--;;
CREATE UNIQUE INDEX uq_drip_job_unique_key ON drip_job (unique_key_active);
