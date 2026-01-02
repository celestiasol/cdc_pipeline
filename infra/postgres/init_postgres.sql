-- Enabling logical replication
ALTER SYSTEM SET wal_level = logical;
SELECT pg_reload_conf();