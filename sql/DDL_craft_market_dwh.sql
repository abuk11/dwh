--create schema dwh;

DROP TABLE IF EXISTS dwh.d_customers CASCADE;
CREATE TABLE IF NOT EXISTS dwh.d_customers (
	customer_id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	customer_name VARCHAR NULL,
	customer_address VARCHAR NULL,
	customer_birthday DATE NULL,
	customer_email VARCHAR NOT NULL,
	load_dttm timestamp NOT NULL,
	CONSTRAINT customers_pk PRIMARY KEY (customer_id)
);
COMMENT ON TABLE dwh.d_customers IS 'Справочник заказчиков';
COMMENT ON COLUMN dwh.d_customers.customer_id IS 'идентификатор заказчика';
COMMENT ON COLUMN dwh.d_customers.customer_name IS 'ФИО заказчика';
COMMENT ON COLUMN dwh.d_customers.customer_address IS 'адрес заказчика';
COMMENT ON COLUMN dwh.d_customers.customer_birthday IS 'дата рождения заказчика';
COMMENT ON COLUMN dwh.d_customers.customer_email IS 'электронная почта заказчика';
COMMENT ON COLUMN dwh.d_customers.load_dttm IS 'дата и время загрузки';


DROP TABLE IF EXISTS dwh.d_products CASCADE;
CREATE TABLE IF NOT EXISTS dwh.d_products (
	product_id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	product_name VARCHAR NOT NULL,
	product_description VARCHAR NOT NULL,
	product_type VARCHAR NOT NULL,
	product_price int8 NOT NULL,
	load_dttm timestamp NOT NULL,
	CONSTRAINT products_pk PRIMARY KEY (product_id)
);
COMMENT ON TABLE dwh.d_products IS 'Справочник товаров ручной работы';
COMMENT ON COLUMN dwh.d_products.product_id IS 'идентификатор товара ручной работы';
COMMENT ON COLUMN dwh.d_products.product_name IS 'наименование товара ручной работы';
COMMENT ON COLUMN dwh.d_products.product_description IS 'описание товара ручной работы';
COMMENT ON COLUMN dwh.d_products.product_type IS 'тип товара ручной работы';
COMMENT ON COLUMN dwh.d_products.product_price IS 'цена товара ручной работы';
COMMENT ON COLUMN dwh.d_products.load_dttm IS 'дата и время загрузки';


DROP TABLE IF EXISTS dwh.d_craftsman CASCADE;
CREATE TABLE IF NOT EXISTS dwh.d_craftsmans (
	craftsman_id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	craftsman_name VARCHAR NOT NULL,
	craftsman_address VARCHAR NOT NULL,
	craftsman_birthday DATE NOT NULL,
	craftsman_email VARCHAR NOT NULL,
	load_dttm timestamp NOT NULL,
	CONSTRAINT craftsmans_pk PRIMARY KEY (craftsman_id)
);
COMMENT ON TABLE dwh.d_craftsmans IS 'Справочник мастеров';
COMMENT ON COLUMN dwh.d_craftsmans.craftsman_id IS 'идентификатор мастера';
COMMENT ON COLUMN dwh.d_craftsmans.craftsman_name IS 'ФИО мастера';
COMMENT ON COLUMN dwh.d_craftsmans.craftsman_address IS 'адрес мастера';
COMMENT ON COLUMN dwh.d_craftsmans.craftsman_birthday IS 'дата рождения мастера';
COMMENT ON COLUMN dwh.d_craftsmans.craftsman_email IS 'электронная почта мастера';
COMMENT ON COLUMN dwh.d_craftsmans.load_dttm IS 'дата и время загрузки';


DROP TABLE IF EXISTS dwh.f_orders CASCADE;
CREATE TABLE dwh.f_orders (
	order_id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	product_id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	craftsman_id int8 NOT NULL,
	customer_id int8 NOT NULL,
	order_created_date DATE NULL,
	order_completion_date DATE NULL,
	order_status VARCHAR NOT NULL,
	load_dttm timestamp NOT NULL,
	CONSTRAINT orders_pk PRIMARY KEY (order_id),
	CONSTRAINT orders_craftsmans_fk FOREIGN KEY (craftsman_id) REFERENCES dwh.d_craftsmans(craftsman_id) ON DELETE restrict,
	CONSTRAINT orders_customers_fk FOREIGN KEY (customer_id) REFERENCES dwh.d_customers(customer_id) ON DELETE restrict,
	CONSTRAINT orders_products_fk FOREIGN KEY (product_id) REFERENCES dwh.d_products(product_id) ON DELETE restrict
);



COMMENT ON TABLE dwh.f_orders IS 'Фактовая таблица с заказами';
COMMENT ON COLUMN dwh.f_orders.order_id IS 'идентификатор заказа';
COMMENT ON COLUMN dwh.f_orders.product_id IS 'идентификтор товара ручной работы';
COMMENT ON COLUMN dwh.f_orders.craftsman_id IS 'идентификатор мастера';
COMMENT ON COLUMN dwh.f_orders.customer_id IS 'идентификатор заказчика';
COMMENT ON COLUMN dwh.f_orders.order_created_date IS 'дата создания заказа';
COMMENT ON COLUMN dwh.f_orders.order_completion_date IS 'дата выполнения заказа';
COMMENT ON COLUMN dwh.f_orders.order_status IS 'статус выполнения заказа (created, in progress, delivery, done)';
COMMENT ON COLUMN dwh.f_orders.load_dttm IS 'дата и время загрузки';


