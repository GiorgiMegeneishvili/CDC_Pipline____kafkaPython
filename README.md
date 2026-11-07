# CDC_Pipline____kafkaPython

Creating Wal Logincs on PostgreSQL

ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET max_wal_senders = 10;
ALTER SYSTEM SET listen_addresses = '*';


ALTER SYSTEM SET max_slot_wal_keep_size = '10GB';


DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium') THEN
    CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium';
  ELSE
    ALTER ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium';
  END IF;
END $$;

GRANT CONNECT ON DATABASE "CDC" TO debezium;
GRANT USAGE ON SCHEMA bronze TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA bronze TO debezium;

-- მომავალში შექმნილ ცხრილებზეც (მაგ. CNK1, AW22) ავტომატურად
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT SELECT ON TABLES TO debezium;

DROP PUBLICATION IF EXISTS dbz_publication;

CREATE PUBLICATION dbz_publication FOR TABLE
  bronze.nw1_customers, bronze.nw1_employees, bronze.nw1_products,
  bronze.nw1_categories, bronze.nw1_suppliers, bronze.nw1_shippers,
  bronze.nw1_orders, bronze.nw1_order_details
WITH (publish_via_partition_root = true);

SELECT * FROM pg_publication_tables WHERE pubname = 'dbz_publication';
