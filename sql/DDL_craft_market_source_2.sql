-- DROP SCHEMA source2;

CREATE SCHEMA IF NOT EXISTS source2;

-- DROP TABLE source2.craft_market_masters_products;

CREATE TABLE IF NOT EXISTS source2.craft_market_masters_products (
	craftsman_id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL, -- идентификатор мастера
	craftsman_name VARCHAR NOT NULL, -- ФИО мастера
	craftsman_address VARCHAR NOT NULL, -- адрес мастера
	craftsman_birthday DATE NOT NULL, -- дата рождения мастера
	craftsman_email VARCHAR NOT NULL, -- электронная почта мастера
	product_id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL, -- идентификтор товара ручной работы
	product_name VARCHAR NOT NULL, -- название товара ручной работы
	product_description VARCHAR NOT NULL, -- описание товара ручной работы
	product_type VARCHAR NOT NULL, -- тип товара ручной работы
	product_price int8 NOT NULL, -- цена товара ручной работы
	CONSTRAINT craft_market_masters_products_pk PRIMARY KEY (craftsman_id)
);
COMMENT ON TABLE source2.craft_market_masters_products IS 'Таблица содержит информацию по мастерам и их продуктам';

-- Column comments

COMMENT ON COLUMN source2.craft_market_masters_products.craftsman_id IS 'идентификатор мастера';
COMMENT ON COLUMN source2.craft_market_masters_products.craftsman_name IS 'ФИО мастера';
COMMENT ON COLUMN source2.craft_market_masters_products.craftsman_address IS 'адрес мастера';
COMMENT ON COLUMN source2.craft_market_masters_products.craftsman_birthday IS 'дата рождения мастера';
COMMENT ON COLUMN source2.craft_market_masters_products.craftsman_email IS 'электронная почта мастера';
COMMENT ON COLUMN source2.craft_market_masters_products.product_id IS 'идентификтор товара ручной работы';
COMMENT ON COLUMN source2.craft_market_masters_products.product_name IS 'название товара ручной работы';
COMMENT ON COLUMN source2.craft_market_masters_products.product_description IS 'описание товара ручной работы';
COMMENT ON COLUMN source2.craft_market_masters_products.product_type IS 'тип товара ручной работы';
COMMENT ON COLUMN source2.craft_market_masters_products.product_price IS 'цена товара ручной работы';


-- source2.craft_market_orders_customers definition

-- Drop table

-- DROP TABLE source2.craft_market_orders_customers;

CREATE TABLE IF NOT EXISTS source2.craft_market_orders_customers (
	order_id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL, -- идентификатор заказа
	craftsman_id int8 NOT NULL, -- идентификатор мастера
	product_id int8 NOT NULL, -- идентификтор товара ручной работы
	order_created_date DATE NULL, -- дата создания заказа
	order_completion_date DATE NULL, -- дата выполнения заказа
	order_status VARCHAR NOT NULL, -- статус выполнения заказа (created, in progress, delivery, done)
	customer_id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL, -- идентификатор заказчика
	customer_name VARCHAR NULL, -- ФИО заказчика
	customer_address VARCHAR NOT NULL, -- адрес заказчика
	customer_birthday DATE NULL, -- дата рождения заказчика
	customer_email VARCHAR NOT NULL, -- электронная почта заказчика
	CONSTRAINT craft_market_orders_customers_pk PRIMARY KEY (order_id),
	CONSTRAINT craft_market_orders_customers_fk FOREIGN KEY (craftsman_id) REFERENCES source2.craft_market_masters_products(craftsman_id) ON DELETE RESTRICT
);
COMMENT ON TABLE source2.craft_market_orders_customers IS 'Таблица содержит информацию по заказам и заказчикам';

-- Column comments

COMMENT ON COLUMN source2.craft_market_orders_customers.order_id IS 'идентификатор заказа';
COMMENT ON COLUMN source2.craft_market_orders_customers.craftsman_id IS 'идентификатор мастера';
COMMENT ON COLUMN source2.craft_market_orders_customers.product_id IS 'идентификтор товара ручной работы';
COMMENT ON COLUMN source2.craft_market_orders_customers.order_created_date IS 'дата создания заказа';
COMMENT ON COLUMN source2.craft_market_orders_customers.order_completion_date IS 'дата выполнения заказа';
COMMENT ON COLUMN source2.craft_market_orders_customers.order_status IS 'статус выполнения заказа (created, in progress, delivery, done)';
COMMENT ON COLUMN source2.craft_market_orders_customers.customer_id IS 'идентификатор заказчика';
COMMENT ON COLUMN source2.craft_market_orders_customers.customer_name IS 'ФИО заказчика';
COMMENT ON COLUMN source2.craft_market_orders_customers.customer_address IS 'адрес заказчика';
COMMENT ON COLUMN source2.craft_market_orders_customers.customer_birthday IS 'дата рождения заказчика';
COMMENT ON COLUMN source2.craft_market_orders_customers.customer_email IS 'электронная почта заказчика';

