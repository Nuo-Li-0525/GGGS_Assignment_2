-- Inserts into GGGS_DATA_UPLOAD

INSERT INTO gggs_data_upload
VALUES
(gggs_data_load_seq.NEXTVAL, 'ST', 'N', 'GBULBS', 'ROCTEFARM', 'GBRGFF', 'Gladiolus Funny Fiction', null, null, 6.99, 23, 'Y');

INSERT INTO gggs_data_upload
VALUES
(gggs_data_load_seq.NEXTVAL, 'CU', 'N', 'JOSM', 'AB', 'John', 'Smith', 'Calgary', 4035554987, null, null, 'N');

INSERT INTO gggs_data_upload
VALUES
(gggs_data_load_seq.NEXTVAL, 'CA', 'Q', 'GBULBS', 'I', null, null, null, null, null, null, 'N');

INSERT INTO gggs_data_upload
VALUES
(gggs_data_load_seq.NEXTVAL, 'VE', 'C', 'DOULAFARM', 'NC', 'NC', 'NC', 'NC', 4035555692, null, null, 'N');

INSERT INTO gggs_data_upload
VALUES
(gggs_data_load_seq.NEXTVAL, 'CU', 'C', 'SOME', 'BC', 'NC', 'NC', 'Victoria', 2505552963, null, null, 'N');

INSERT INTO gggs_data_upload
VALUES
(gggs_data_load_seq.NEXTVAL, 'ST', 'R', 'GBULBS', 'I', null, null, null, null, null, null, 'N');

INSERT INTO gggs_data_upload
VALUES
(gggs_data_load_seq.NEXTVAL, 'CA', 'N', 'FLBULBS', 'Flower Bulbs', null, null, null, null, null, null, 'N');

INSERT INTO gggs_data_upload
VALUES
(gggs_data_load_seq.NEXTVAL, 'CU', 'S', 'ARST', 'I', null, null, null, null, null, null, 'N');

INSERT INTO gggs_data_upload
VALUES
(gggs_data_load_seq.NEXTVAL, 'CU', 'N', 'CLOX', 'MB', 'Claire', 'Oxford', 'Winnipeg', 2045559244, null, null, 'N');

INSERT INTO gggs_data_upload
VALUES
(gggs_data_load_seq.NEXTVAL, 'CU', 'N', 'JOSM', 'AB', 'John', 'Smith', 'Calgary', 4035558547, null, null, 'N');

INSERT INTO gggs_data_upload
VALUES
(gggs_data_load_seq.NEXTVAL, 'VE', 'S', 'TALFARM', 'I', null, null, null, null, null, null, 'N');

INSERT INTO gggs_data_upload
VALUES
(gggs_data_load_seq.NEXTVAL, 'VE', 'C', 'SWLAFARM', 'NC', 'Mateo', 'Wolf', 'NC', null, null, null, 'N');

INSERT INTO gggs_data_upload
VALUES
(gggs_data_load_seq.NEXTVAL, 'CU', 'A', 'GBULBS', 'I', null, null, null, null, null, null, 'N');

INSERT INTO gggs_data_upload
VALUES
(gggs_data_load_seq.NEXTVAL, 'CA', 'S', 'GBULBS', 'I', null, null, null, null, null, null, 'N');

INSERT INTO gggs_data_upload
VALUES
(gggs_data_load_seq.NEXTVAL, 'CO', 'S', 'GBULBS', 'I', null, null, null, null, null, null, 'N');

INSERT INTO gggs_data_upload
VALUES
(gggs_data_load_seq.NEXTVAL, 'VE', 'N', 'PAVGARD', 'Pavillion Gardens', 'Emmanuel', 'Jones', null, 4035558932, null, null, 'N');

INSERT INTO gggs_data_upload
VALUES
(gggs_data_load_seq.NEXTVAL, 'ST', 'N', 'GBULBS', 'ROCTEFARM', 'GBRDG', 'Dicentra Garden', null, null, 29.99, 42, 'Y');

INSERT INTO gggs_data_upload
VALUES
(gggs_data_load_seq.NEXTVAL, 'VE', 'L', 'GBULBS', 'I', null, null, null, null, null, null, 'N');

INSERT INTO gggs_data_upload
VALUES
(gggs_data_load_seq.NEXTVAL, 'ST', 'N', 'GSEEDS', 'DOULAFARM', 'GSDB', 'Broccoli', null, null, 2.49, 100, 'Y');

INSERT INTO gggs_data_upload
VALUES
(gggs_data_load_seq.NEXTVAL, 'ST', 'N', 'GSEEDS', 'DOULAFARM', 'GSDE', 'Eggplant', null, null, 5.99, 3, 'Y');

INSERT INTO gggs_data_upload
VALUES
(gggs_data_load_seq.NEXTVAL, 'ST', 'N', 'GSEEDS', 'SWLAFARM', 'GSOKCFQ', 'Crane Feather Queen Ornamental Kale', null, null, 4.49, 100, 'N');

INSERT INTO gggs_data_upload
VALUES
(gggs_data_load_seq.NEXTVAL, 'ST', 'N', 'FLBULBS', 'ROCTEFARM', 'FBDASP', 'Dahlia Special', null, null, 6.99, 55, 'N');

INSERT INTO gggs_data_upload
VALUES
(gggs_data_load_seq.NEXTVAL, 'ST', 'S', 'GSDE', 'I', null, null, null, null, null, null, 'N');

INSERT INTO gggs_data_upload
VALUES
(gggs_data_load_seq.NEXTVAL, 'ST', 'C', 'GBRGFF', 'NC', 'NC', 'Gladiolus', 'NC', null, 7.49, null, 'N');

INSERT INTO gggs_data_upload
VALUES
(gggs_data_load_seq.NEXTVAL, 'ST', 'C', 'GSDB', 'NC', 'NC', 'NC', 'NC', null, null, 33, 'N');



COMMIT;

-- Inserts into GGGS_CATEGORY

INSERT INTO gggs_category
VALUES
(gggs_category_seq.NEXTVAL, 'GSEEDS', 'General Seeds', 'A');

INSERT INTO gggs_category
VALUES
(gggs_category_seq.NEXTVAL, 'GBULBS', 'General Bulbs', 'A');

INSERT INTO gggs_category
VALUES
(gggs_category_seq.NEXTVAL, 'GPLST', 'General Plant Stock', 'A');

INSERT INTO gggs_category
VALUES
(gggs_category_seq.NEXTVAL, 'GGASU', 'General Gardend Supply', 'A');


COMMIT;

-- Inserts into GGGS_VENDOR

INSERT INTO gggs_vendor
VALUES
(gggs_vendor_seq.NEXTVAL, 'TALFARM', 'Taylor Farms', 'Dewei', 'Barlowe', 4035551234, 'A');

INSERT INTO gggs_vendor
VALUES
(gggs_vendor_seq.NEXTVAL, 'ROCTEFARM', 'Rocky Terrace Farms', 'Mae', 'Sai', 4035556542, 'A');

INSERT INTO gggs_vendor
VALUES
(gggs_vendor_seq.NEXTVAL, 'WHITRFARM', 'White Tree Farms', 'Otis', 'Caddel', 4035555654, 'A');

INSERT INTO gggs_vendor
VALUES
(gggs_vendor_seq.NEXTVAL, 'DOULAFARM', 'Douglas Lake Farms', 'Kalen', 'Francis', 4035559256, 'A');

INSERT INTO gggs_vendor
VALUES
(gggs_vendor_seq.NEXTVAL, 'SWLAFARM', 'Swan Lake Farms', 'Luna', 'Hendrix', 4035552584, 'A');

INSERT INTO gggs_vendor
VALUES
(gggs_vendor_seq.NEXTVAL, 'HTFARM', 'Happy Turkey Farms', 'Bob', 'Turkey', 4035552584, 'A');

COMMIT;

-- Inserts into GGGS_STOCK

INSERT INTO gggs_stock 
VALUES 
(gggs_stock_seq.NEXTVAL, 2, 2, 'GBRGFF', 'Gladiolus Funny Fiction', 6.99, 23, 'A');

INSERT INTO gggs_stock 
VALUES 
(gggs_stock_seq.NEXTVAL, 2, 2, 'GBRDG', 'Dicentra Garden', 29.99, 42, 'A');

INSERT INTO gggs_stock 
VALUES 
(gggs_stock_seq.NEXTVAL, 1, 4, 'GSDB', 'Broccoli', 2.49, 100, 'A');

INSERT INTO gggs_stock 
VALUES 
(gggs_stock_seq.NEXTVAL, 1, 4, 'GSDE', 'Eggplant', 5.99, 3, 'A');


COMMIT;

-- Inserts into GGGS_STATUS

INSERT INTO gggs_status
VALUES
('OS', 'Out Of Stock');

INSERT INTO gggs_status
VALUES
('IS', 'In Stock');

COMMIT;

-- Inserts into GGGS_PROVINCE

INSERT INTO gggs_province
VALUES
('BC', 'British Columbia');

INSERT INTO gggs_province
VALUES
('AB', 'Alberta');

INSERT INTO gggs_province
VALUES
('SK', 'Saskatchewan');

INSERT INTO gggs_province
VALUES
('MB', 'Manitoba');

INSERT INTO gggs_province
VALUES
('ON', 'Ontario');

INSERT INTO gggs_province
VALUES
('QC', 'Quebec');

INSERT INTO gggs_province
VALUES
('NB', 'New Brunswick');

INSERT INTO gggs_province
VALUES
('NL', 'Newfoundland and Labrador');

INSERT INTO gggs_province
VALUES
('NS', 'Nova Scotia');

INSERT INTO gggs_province
VALUES
('PE', 'Prince Edward Island');

INSERT INTO gggs_province
VALUES
('NT', 'Northwest Territories');

INSERT INTO gggs_province
VALUES
('NU', 'Nunavut');

INSERT INTO gggs_province
VALUES
('YT', 'Yukon');



COMMIT;

-- Inserts into GGGS_CUSTOMER

INSERT INTO gggs_customer
VALUES
(gggs_customer_seq.NEXTVAL, 'FRLA', 'NS', 'Fred', 'Laurier', 'Dartmouth', 7825556789, 'A');

INSERT INTO gggs_customer
VALUES
(gggs_customer_seq.NEXTVAL, 'KEAH', 'NL', 'Kenji', 'Ashford', 'Mount Pearl', 7095556894, 'A');

INSERT INTO gggs_customer
VALUES
(gggs_customer_seq.NEXTVAL, 'TOLA', 'NB', 'Tobias', 'Langston', 'Miramichi', 5065556158, 'A');

INSERT INTO gggs_customer
VALUES
(gggs_customer_seq.NEXTVAL, 'AMTR', 'QC', 'Amelie', 'Trevino', 'Gatineau', 8195554895, 'A');

INSERT INTO gggs_customer
VALUES
(gggs_customer_seq.NEXTVAL, 'ARST', 'MB', 'Ariel', 'Stallard', 'Flin Flon', 2045553578, 'A');

INSERT INTO gggs_customer
VALUES
(gggs_customer_seq.NEXTVAL, 'SOME', 'SK', 'Sophia', 'Menken', 'Esterhazy', 3065556789, 'A');

INSERT INTO gggs_customer
VALUES
(gggs_customer_seq.NEXTVAL, 'SICO', 'NU', 'Siku', 'Cournoyea', 'Arviat', 8675551876, 'A');


COMMIT;

-- Inserts into GGGS_ORDERS

-- NO DATA TO INSERT

COMMIT;

-- Inserts into GGGS_ORDER_DETAIL

-- NO DATA TO INSERT

COMMIT;
