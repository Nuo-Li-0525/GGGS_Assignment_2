DROP TABLE gggs_order_detail;
DROP TABLE gggs_orders;
DROP TABLE gggs_customer;
DROP TABLE gggs_province;
DROP TABLE gggs_status;
DROP TABLE gggs_stock;
DROP TABLE gggs_vendor;
DROP TABLE gggs_category;
DROP TABLE gggs_data_upload;
DROP TABLE gggs_error_log_table;

DROP SEQUENCE gggs_customer_seq;
DROP SEQUENCE gggs_order_seq;
DROP SEQUENCE gggs_vendor_seq;
DROP SEQUENCE gggs_stock_seq;
DROP SEQUENCE gggs_category_seq;
DROP SEQUENCE gggs_data_load_seq;

CREATE SEQUENCE gggs_customer_seq;
CREATE SEQUENCE gggs_order_seq
  INCREMENT BY 2;
CREATE SEQUENCE gggs_vendor_seq;
CREATE SEQUENCE gggs_stock_seq;
CREATE SEQUENCE gggs_category_seq;
CREATE SEQUENCE gggs_data_load_seq;

CREATE TABLE gggs_data_upload
(loadID         NUMBER,
 data_type		CHAR(2),
 process_type		CHAR(1),
 column1		VARCHAR2(100),
 column2		VARCHAR2(100),
 column3		VARCHAR2(100),
 column4		VARCHAR2(100),
 column5		VARCHAR2(100),
 column6		NUMBER(10),
 column7		NUMBER,
 column8		NUMBER,
 data_processed CHAR(1)
);

CREATE TABLE gggs_category
(categoryID		NUMBER,
 name			VARCHAR2(10),
 description		VARCHAR2(100),
 status			CHAR(1)
);

CREATE TABLE gggs_vendor
(vendorID		NUMBER,
 name			VARCHAR2(10),
 description		VARCHAR2(100),
 contact_first_name	VARCHAR2(50),
 contact_last_name	VARCHAR2(50),
 contact_phone_number	NUMBER(10),
 status			CHAR(1)
);

CREATE TABLE gggs_stock
(stockID		NUMBER,
 categoryID		NUMBER,
 supplierID		NUMBER,
 name			VARCHAR2(10),
 description		VARCHAR2(100),
 price			NUMBER,
 no_in_stock		NUMBER,
 status			CHAR(1)
);

CREATE TABLE gggs_status
(status_code		VARCHAR2(2),
 description		VARCHAR2(20)
);

CREATE TABLE gggs_province
(province_code		VARCHAR2(2),
 name			VARCHAR2(50)
);


CREATE TABLE gggs_customer
(customerID		NUMBER,
 name			VARCHAR2(10),
 province		VARCHAR2(2),
 first_name		VARCHAR2(50),
 last_name		VARCHAR2(50),
 city			VARCHAR2(75),
 phone_number		NUMBER(10),
 status			CHAR(1)
);

CREATE TABLE gggs_orders
(orderID		NUMBER,
 customerID		NUMBER,
 order_date		DATE,
 total_cost		NUMBER
);

CREATE TABLE gggs_order_detail
(orderID		NUMBER,
 stockID		NUMBER,
 status			VARCHAR2(2),
 current_item_price	NUMBER,
 quantity		NUMBER,
 item_subtotal		NUMBER
);

CREATE TABLE gggs_error_log_table 
(data_type      CHAR(2),
 process_type   CHAR(1),
 error_message  VARCHAR2(100)
 );
 