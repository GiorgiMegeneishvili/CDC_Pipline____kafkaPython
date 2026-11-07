ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET max_wal_senders = 10;
ALTER SYSTEM SET listen_addresses = '*';

-- Safety net: a stopped connector can't make the slot retain more than 10 GB of WAL
ALTER SYSTEM SET max_slot_wal_keep_size = '10GB';

SHOW wal_level;               -- logical
SHOW max_slot_wal_keep_size;  -- 10GB


-- User with the REPLICATION attribute
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium') THEN
    CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium';
  ELSE
    ALTER ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium';
  END IF;
END $$;

-- SELECT is needed for the initial snapshot
GRANT CONNECT ON DATABASE "CDC" TO debezium;
GRANT USAGE ON SCHEMA bronze TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA bronze TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT SELECT ON TABLES TO debezium;

-- Publication: which tables are streamed
DROP PUBLICATION IF EXISTS dbz_publication;

CREATE PUBLICATION dbz_publication FOR TABLE
  bronze.nw1_customers, bronze.nw1_employees, bronze.nw1_products,
  bronze.nw1_categories, bronze.nw1_suppliers, bronze.nw1_shippers,
  bronze.nw1_orders, bronze.nw1_order_details
WITH (publish_via_partition_root = true);


DO $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT n.nspname, c.relname
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'bronze' AND c.relname LIKE 'nw1\_%' AND c.relkind IN ('r', 'p')
  LOOP
    EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY FULL', r.nspname, r.relname);
  END LOOP;
END $$;




SELECT rolname, rolreplication FROM pg_roles WHERE rolname = 'debezium';
SELECT * FROM pg_publication_tables WHERE pubname = 'dbz_publication';  -- parent tables, not partitions
