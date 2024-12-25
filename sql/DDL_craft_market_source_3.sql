-- DROP SCHEMA source3;

CREATE SCHEMA IF NOT EXISTS source3;

-- Drop table

-- DROP TABLE source3.craft_market_craftsmans;

CREATE TABLE IF NOT EXISTS source3.craft_market_craftsmans (
	craftsman_id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL, -- идентификатор мастера
	craftsman_name VARCHAR NOT NULL, -- ФИО мастера
	craftsman_address VARCHAR NOT NULL, -- адрес мастера
	craftsman_birthday DATE NOT NULL, -- дата рождения мастера
	craftsman_email VARCHAR NOT NULL, -- электронная почта мастера
	CONSTRAINT craft_market_craftsmans_pk PRIMARY KEY (craftsman_id)
);
COMMENT ON TABLE source3.craft_market_craftsmans IS 'Таблица с мастерами';

-- Column comments

COMMENT ON COLUMN source3.craft_market_craftsmans.craftsman_id IS 'идентификатор мастера';
COMMENT ON COLUMN source3.craft_market_craftsmans.craftsman_name IS 'ФИО мастера';
COMMENT ON COLUMN source3.craft_market_craftsmans.craftsman_address IS 'адрес мастера';
COMMENT ON COLUMN source3.craft_market_craftsmans.craftsman_birthday IS 'дата рождения мастера';
COMMENT ON COLUMN source3.craft_market_craftsmans.craftsman_email IS 'электронная почта мастера';


-- source3.craft_market_customers definition

-- Drop table

-- DROP TABLE source3.craft_market_customers;

CREATE TABLE IF NOT EXISTS source3.craft_market_customers (
	customer_id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL, -- идентификатор заказчика
	customer_name VARCHAR NULL, -- ФИО заказчика
	customer_address VARCHAR NULL, -- адрес заказчика
	customer_birthday DATE NULL, -- дата рождения заказчика
	customer_email VARCHAR NOT NULL, -- электронная почта заказчика
	CONSTRAINT craft_market_customers_pk PRIMARY KEY (customer_id)
);
COMMENT ON TABLE source3.craft_market_customers IS 'Таблица с заказчиками';

-- Column comments

COMMENT ON COLUMN source3.craft_market_customers.customer_id IS 'идентификатор заказчика';
COMMENT ON COLUMN source3.craft_market_customers.customer_name IS 'ФИО заказчика';
COMMENT ON COLUMN source3.craft_market_customers.customer_address IS 'адрес заказчика';
COMMENT ON COLUMN source3.craft_market_customers.customer_birthday IS 'дата рождения заказчика';
COMMENT ON COLUMN source3.craft_market_customers.customer_email IS 'электронная почта заказчика';


-- source3.craft_market_orders definition

-- Drop table

-- DROP TABLE source3.craft_market_orders;

CREATE TABLE IF NOT EXISTS source3.craft_market_orders (
	order_id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL, -- идентификатор заказа
	product_id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL, -- идентификтор товара ручной работы
	craftsman_id int8 NOT NULL, -- идентификатор мастера
	customer_id int8 NOT NULL, -- идентификатор заказчика
	order_created_date DATE NULL, -- дата создания заказа
	order_completion_date DATE NULL, -- дата выполнения заказа
	order_status VARCHAR NOT NULL, -- статус выполнения заказа (created, in progress, delivery, done)
	product_name VARCHAR NOT NULL, -- название товара ручной работы
	product_description VARCHAR NOT NULL, -- описание товара ручной работы
	product_type VARCHAR NOT NULL, -- тип товара ручной работы
	product_price int8 NOT NULL, -- цена товара ручной работы
	CONSTRAINT craft_market_orders_pk PRIMARY KEY (order_id),
	CONSTRAINT craft_market_orders_craftsmans_fk FOREIGN KEY (craftsman_id) REFERENCES source3.craft_market_craftsmans(craftsman_id) ON DELETE RESTRICT,
	CONSTRAINT craft_market_orders_customers_fk FOREIGN KEY (customer_id) REFERENCES source3.craft_market_customers(customer_id) ON DELETE RESTRICT
);
COMMENT ON TABLE source3.craft_market_orders IS 'Таблица с заказами';

-- Column comments

COMMENT ON COLUMN source3.craft_market_orders.order_id IS 'идентификатор заказа';
COMMENT ON COLUMN source3.craft_market_orders.product_id IS 'идентификтор товара ручной работы';
COMMENT ON COLUMN source3.craft_market_orders.craftsman_id IS 'идентификатор мастера';
COMMENT ON COLUMN source3.craft_market_orders.customer_id IS 'идентификатор заказчика';
COMMENT ON COLUMN source3.craft_market_orders.order_created_date IS 'дата создания заказа';
COMMENT ON COLUMN source3.craft_market_orders.order_completion_date IS 'дата выполнения заказа';
COMMENT ON COLUMN source3.craft_market_orders.order_status IS 'статус выполнения заказа (created, in progress, delivery, done)';
COMMENT ON COLUMN source3.craft_market_orders.product_name IS 'название товара ручной работы';
COMMENT ON COLUMN source3.craft_market_orders.product_description IS 'описание товара ручной работы';
COMMENT ON COLUMN source3.craft_market_orders.product_type IS 'тип товара ручной работы';
COMMENT ON COLUMN source3.craft_market_orders.product_price IS 'цена товара ручной работы';

