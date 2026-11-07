
INSERT INTO bronze.nw1_shippers (shipperid, companyname, phone)
VALUES (999, 'Test Shipper', '555-0000');

INSERT INTO bronze.nw1_shippers (shipperid, companyname, phone, load_date)
VALUES (998, 'Test Shipper 2', '555-0000', now());
COMMIT;

UPDATE bronze.nw1_shippers SET phone = '555-1111' WHERE shipperid = 999;
UPDATE bronze.nw1_shippers SET phone = '555-' WHERE shipperid = 998;

DELETE FROM bronze.nw1_shippers WHERE shipperid = 999;
DELETE FROM bronze.nw1_shippers WHERE shipperid = 998;


INSERT INTO bronze.nw1_shippers (shipperid, companyname, phone, load_date)
VALUES (999, 'Test Shipper', '555-0000', now());
COMMIT;

INSERT INTO bronze.nw1_orders (orderid, customerid, employeeid, orderdate, freight, shipname, load_date)
VALUES (90001, 'TEST1', 1, now(), 12.5, 'Test Ship', now());
COMMIT;
UPDATE bronze.nw1_orders SET shipname = 'Test' WHERE orderid = 90001;
BEGIN;

-- 1. categories
INSERT INTO bronze.nw1_categories (categoryid, categoryname, description, picture, load_date)
VALUES (9001, 'Test Category', 'Category for CDC test', NULL, current_date);

-- 2. suppliers
INSERT INTO bronze.nw1_suppliers
  (supplierid, companyname, contactname, contacttitle, address, city, region,
   postalcode, country, phone, fax, homepage, load_date)
VALUES
  (9001, 'Test Supplier LLC', 'Nino K.', 'Sales Manager', 'Chavchavadze Ave 10', 'Tbilisi', NULL,
   '0179', 'Georgia', '+995 555 111 111', NULL, 'https://test-supplier.ge', current_date);

-- 3. shippers
INSERT INTO bronze.nw1_shippers (shipperid, companyname, phone, load_date)
VALUES (9001, 'Test Express', '+995 555 222 222', current_date);

-- 4. products
INSERT INTO bronze.nw1_products
  (productid, productname, supplierid, categoryid, quantityperunit, unitprice,
   unitsinstock, unitsonorder, reorderlevel, discontinued, load_date)
VALUES
  (9001, 'Test Product', 9001, 9001, '10 boxes x 20 bags', 25.50,
   100, 0, 10, '0', current_date);

-- 5. customers
INSERT INTO bronze.nw1_customers
  (customerid, companyname, contactname, contacttitle, address, city, region,
   postalcode, country, phone, fax, load_date)
VALUES
  ('TST01', 'Test Company', 'Giorgi M.', 'Data Engineer', 'Rustaveli Ave 1', 'Tbilisi', NULL,
   '0108', 'Georgia', '+995 555 000 000', NULL, current_date);

-- 6. employees
INSERT INTO bronze.nw1_employees
  (employeeid, lastname, firstname, title, titleofcourtesy, birthdate, hiredate,
   address, city, region, postalcode, country, homephone, extension, photo,
   notes, reportsto, photopath, load_date)
VALUES
  (9001, 'Beridze', 'Levan', 'Sales Representative', 'Mr.', '1990-05-15', '2024-01-10',
   'Pekini Ave 5', 'Tbilisi', NULL, '0160', 'Georgia', '+995 555 333 333', '101', NULL,
   'CDC test employee', NULL, NULL, current_date);

-- 7. orders
INSERT INTO bronze.nw1_orders
  (orderid, customerid, employeeid, orderdate, requireddate, shippeddate, shipvia, freight,
   shipname, shipaddress, shipcity, shipregion, shippostalcode, shipcountry, load_date)
VALUES
  (9001, 'TST01', 9001, now(), now() + interval '7 days', NULL, 9001, 12.75,
   'Test Company', 'Rustaveli Ave 1', 'Tbilisi', NULL, '0108', 'Georgia', current_date);

-- 8. order_details
INSERT INTO bronze.nw1_order_details (orderid, productid, unitprice, quantity, discount, load_date)
VALUES (9001, 9001, 25.50, 3, 0.05, current_date);

COMMIT;





BEGIN;
UPDATE bronze.nw1_orders        SET shippeddate = now(), freight = 15.00 WHERE orderid = 9001;
UPDATE bronze.nw1_products      SET unitsinstock = 97                  WHERE productid = 9001;
UPDATE bronze.nw1_customers     SET city = 'Batumi'                    WHERE customerid = 'TST01';
COMMIT;

BEGIN;
DELETE FROM bronze.nw1_order_details WHERE orderid = 9001 AND productid = 9001;
COMMIT;
