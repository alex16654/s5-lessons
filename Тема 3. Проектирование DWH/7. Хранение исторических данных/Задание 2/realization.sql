-- Удалите внешний ключ из sales
ALTER TABLE sales DROP CONSTRAINT sales_products_product_id_fk;
ALTER TABLE products DROP CONSTRAINT products_pk;
ALTER TABLE products ADD COLUMN id serial;
ALTER TABLE products ADD PRIMARY KEY (id);
ALTER TABLE products ADD COLUMN valid_from timestamptz;
ALTER TABLE products ADD COLUMN valid_to timestamptz;
ALTER TABLE sales ADD CONSTRAINT sales_products_product_id_fk FOREIGN KEY (product_id) REFERENCES products;
