
-- =====================================================
-- BRONZE layer, source: Northwind (NW1)
-- Columns = same as MSSQL source (lowercased) + load_date
-- Table names prefixed with source (nw1_) because the single
-- bronze schema will also hold CNK1 / AW22 tables
-- (e.g. customer / employee exist in all three sources)
-- Snapshot design: every table is RANGE-partitioned by load_date (yearly),
-- PRIMARY KEY (business key, load_date): one row per key per snapshot day;
-- the same key may repeat on different load_dates (history).
-- PK index also serves as the (key, load_date) lookup index.
-- =====================================================
CREATE SCHEMA IF NOT EXISTS bronze;

-- re-runnable: drops old versions
DROP TABLE IF EXISTS bronze.nw1_customers CASCADE;
DROP TABLE IF EXISTS bronze.nw1_employees CASCADE;
DROP TABLE IF EXISTS bronze.nw1_products CASCADE;
DROP TABLE IF EXISTS bronze.nw1_categories CASCADE;
DROP TABLE IF EXISTS bronze.nw1_suppliers CASCADE;
DROP TABLE IF EXISTS bronze.nw1_shippers CASCADE;
DROP TABLE IF EXISTS bronze.nw1_orders CASCADE;
DROP TABLE IF EXISTS bronze.nw1_order_details CASCADE;

-- source: Northwind.dbo.Customers
CREATE TABLE bronze.nw1_customers (
    customerid    CHAR(5),
    companyname   VARCHAR(40),
    contactname   VARCHAR(30),
    contacttitle  VARCHAR(30),
    address       VARCHAR(60),
    city          VARCHAR(15),
    region        VARCHAR(15),
    postalcode    VARCHAR(10),
    country       VARCHAR(15),
    phone         VARCHAR(24),
    fax           VARCHAR(24),
    load_date     DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (customerid, load_date)
) PARTITION BY RANGE (load_date);

-- source: Northwind.dbo.Employees
CREATE TABLE bronze.nw1_employees (
    employeeid      INTEGER,
    lastname        VARCHAR(20),
    firstname       VARCHAR(10),
    title           VARCHAR(30),
    titleofcourtesy VARCHAR(25),
    birthdate       TIMESTAMP,
    hiredate        TIMESTAMP,
    address         VARCHAR(60),
    city            VARCHAR(15),
    region          VARCHAR(15),
    postalcode      VARCHAR(10),
    country         VARCHAR(15),
    homephone       VARCHAR(24),
    extension       VARCHAR(4),
    photo           BYTEA,
    notes           TEXT,
    reportsto       INTEGER,
    photopath       VARCHAR(255),
    load_date       DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (employeeid, load_date)
) PARTITION BY RANGE (load_date);

-- source: Northwind.dbo.Products
CREATE TABLE bronze.nw1_products (
    productid       INTEGER,
    productname     VARCHAR(40),
    supplierid      INTEGER,
    categoryid      INTEGER,
    quantityperunit VARCHAR(20),
    unitprice       NUMERIC(19,4),
    unitsinstock    SMALLINT,
    unitsonorder    SMALLINT,
    reorderlevel    SMALLINT,
    discontinued    BOOLEAN,
    load_date       DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (productid, load_date)
) PARTITION BY RANGE (load_date);

-- source: Northwind.dbo.Categories
CREATE TABLE bronze.nw1_categories (
    categoryid    INTEGER,
    categoryname  VARCHAR(15),
    description   TEXT,
    picture       BYTEA,
    load_date     DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (categoryid, load_date)
) PARTITION BY RANGE (load_date);

-- source: Northwind.dbo.Suppliers
CREATE TABLE bronze.nw1_suppliers (
    supplierid    INTEGER,
    companyname   VARCHAR(40),
    contactname   VARCHAR(30),
    contacttitle  VARCHAR(30),
    address       VARCHAR(60),
    city          VARCHAR(15),
    region        VARCHAR(15),
    postalcode    VARCHAR(10),
    country       VARCHAR(15),
    phone         VARCHAR(24),
    fax           VARCHAR(24),
    homepage      TEXT,
    load_date     DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (supplierid, load_date)
) PARTITION BY RANGE (load_date);

-- source: Northwind.dbo.Shippers
CREATE TABLE bronze.nw1_shippers (
    shipperid     INTEGER,
    companyname   VARCHAR(40),
    phone         VARCHAR(24),
    load_date     DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (shipperid, load_date)
) PARTITION BY RANGE (load_date);

-- source: Northwind.dbo.Orders
CREATE TABLE bronze.nw1_orders (
    orderid        INTEGER,
    customerid     CHAR(5),
    employeeid     INTEGER,
    orderdate      TIMESTAMP,
    requireddate   TIMESTAMP,
    shippeddate    TIMESTAMP,
    shipvia        INTEGER,
    freight        NUMERIC(19,4),
    shipname       VARCHAR(40),
    shipaddress    VARCHAR(60),
    shipcity       VARCHAR(15),
    shipregion     VARCHAR(15),
    shippostalcode VARCHAR(10),
    shipcountry    VARCHAR(15),
    load_date      DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (orderid, load_date)
) PARTITION BY RANGE (load_date);

-- source: Northwind.dbo.[Order Details]
CREATE TABLE bronze.nw1_order_details (
    orderid       INTEGER,
    productid     INTEGER,
    unitprice     NUMERIC(19,4),
    quantity      SMALLINT,
    discount      REAL,
    load_date     DATE NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (orderid, productid, load_date)
) PARTITION BY RANGE (load_date);

-- =====================================================
-- Partitions (explicit, one block per table): 2026, 2027 + DEFAULT
-- =====================================================

-- nw1_customers
CREATE TABLE bronze.nw1_customers_2026 PARTITION OF bronze.nw1_customers
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.nw1_customers_2027 PARTITION OF bronze.nw1_customers
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.nw1_customers_default PARTITION OF bronze.nw1_customers DEFAULT;

-- nw1_employees
CREATE TABLE bronze.nw1_employees_2026 PARTITION OF bronze.nw1_employees
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.nw1_employees_2027 PARTITION OF bronze.nw1_employees
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.nw1_employees_default PARTITION OF bronze.nw1_employees DEFAULT;

-- nw1_products
CREATE TABLE bronze.nw1_products_2026 PARTITION OF bronze.nw1_products
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.nw1_products_2027 PARTITION OF bronze.nw1_products
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.nw1_products_default PARTITION OF bronze.nw1_products DEFAULT;

-- nw1_categories
CREATE TABLE bronze.nw1_categories_2026 PARTITION OF bronze.nw1_categories
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.nw1_categories_2027 PARTITION OF bronze.nw1_categories
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.nw1_categories_default PARTITION OF bronze.nw1_categories DEFAULT;

-- nw1_suppliers
CREATE TABLE bronze.nw1_suppliers_2026 PARTITION OF bronze.nw1_suppliers
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.nw1_suppliers_2027 PARTITION OF bronze.nw1_suppliers
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.nw1_suppliers_default PARTITION OF bronze.nw1_suppliers DEFAULT;

-- nw1_shippers
CREATE TABLE bronze.nw1_shippers_2026 PARTITION OF bronze.nw1_shippers
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.nw1_shippers_2027 PARTITION OF bronze.nw1_shippers
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.nw1_shippers_default PARTITION OF bronze.nw1_shippers DEFAULT;

-- nw1_orders
CREATE TABLE bronze.nw1_orders_2026 PARTITION OF bronze.nw1_orders
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.nw1_orders_2027 PARTITION OF bronze.nw1_orders
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.nw1_orders_default PARTITION OF bronze.nw1_orders DEFAULT;

-- nw1_order_details
CREATE TABLE bronze.nw1_order_details_2026 PARTITION OF bronze.nw1_order_details
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE TABLE bronze.nw1_order_details_2027 PARTITION OF bronze.nw1_order_details
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');

CREATE TABLE bronze.nw1_order_details_default PARTITION OF bronze.nw1_order_details DEFAULT;
