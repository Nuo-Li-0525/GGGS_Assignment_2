ALTER TABLE gggs_category
  ADD CONSTRAINT pk_gggs_category PRIMARY KEY(categoryID)
  ADD CONSTRAINT uk_gggs_category_name UNIQUE (name)
  ADD CONSTRAINT ck_gggs_category_status CHECK (status IN ('A', 'I'));

ALTER TABLE gggs_vendor
  ADD CONSTRAINT pk_gggs_vendor PRIMARY KEY(vendorID)
  ADD CONSTRAINT uk_gggs_vendor_name UNIQUE (name)
  ADD CONSTRAINT ck_gggs_vendor_status CHECK (status IN ('A', 'I'));

ALTER TABLE gggs_stock
  ADD CONSTRAINT pk_gggs_stock PRIMARY KEY(stockID)
  ADD CONSTRAINT uk_gggs_stock_name UNIQUE (name)
  ADD CONSTRAINT ck_gggs_stock_status CHECK (status IN ('A', 'I'))
  ADD CONSTRAINT fk_gggs_stock_to_category FOREIGN KEY (categoryID) REFERENCES gggs_category (categoryID)
  ADD CONSTRAINT fk_gggs_stock_to_vendor FOREIGN KEY (supplierID) REFERENCES gggs_vendor (vendorID);

ALTER TABLE gggs_status
  ADD CONSTRAINT pk_gggs_status PRIMARY KEY(status_code);

ALTER TABLE gggs_province
  ADD CONSTRAINT pk_gggs_province PRIMARY KEY(province_code);

ALTER TABLE gggs_customer
  ADD CONSTRAINT pk_gggs_customer PRIMARY KEY(customerID)
  ADD CONSTRAINT uk_gggs_customer_name UNIQUE (name)
  ADD CONSTRAINT ck_gggs_customer_status CHECK (status IN ('A', 'I'))
  ADD CONSTRAINT fk_gggs_customer_to_province FOREIGN KEY (province) REFERENCES gggs_province (province_code);

ALTER TABLE gggs_orders
  ADD CONSTRAINT pk_gggs_orders PRIMARY KEY(orderID)
  ADD CONSTRAINT fk_gggs_orders_to_customer FOREIGN KEY (customerID) REFERENCES gggs_customer (customerID);

ALTER TABLE gggs_order_detail
  ADD CONSTRAINT pk_gggs_order_detail PRIMARY KEY(orderID, stockID)
  ADD CONSTRAINT fk_gggs_od_to_orders FOREIGN KEY (orderID) REFERENCES gggs_orders (orderID)
  ADD CONSTRAINT fk_gggs_od_to_stock FOREIGN KEY (stockID) REFERENCES gggs_stock (stockID)
  ADD CONSTRAINT fk_gggs_od_to_status FOREIGN KEY (status) REFERENCES gggs_status (status_code);


