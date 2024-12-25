-- DROP SCHEMA source1;

CREATE SCHEMA IF NOT EXISTS source1;

CREATE TABLE IF NOT EXISTS source1.craft_market_wide (
	id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL, -- идентификатор записи
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
	order_id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL, -- идентификатор заказа
	order_created_date DATE NULL, -- дата создания заказа
	order_completion_date DATE NULL, -- дата выполнения заказа
	order_status VARCHAR NOT NULL, -- статус выполнения заказа (created, in progress, delivery, done)
	customer_id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL, -- идентификатор заказчика
	customer_name VARCHAR NULL, -- ФИО заказчика
	customer_address VARCHAR NULL, -- адрес заказчика
	customer_birthday DATE NULL, -- дата рождения заказчика
	customer_email VARCHAR NOT NULL, -- электронная почта заказчика
	CONSTRAINT craft_market_wide_pk PRIMARY KEY (id)
);
COMMENT ON TABLE source1.craft_market_wide IS 'Широкая таблица с информацией по мастерам, товарам, заказам и заказчиками';

-- Column comments

COMMENT ON COLUMN source1.craft_market_wide.id IS 'идентификатор записи';
COMMENT ON COLUMN source1.craft_market_wide.craftsman_id IS 'идентификатор мастера';
COMMENT ON COLUMN source1.craft_market_wide.craftsman_name IS 'ФИО мастера';
COMMENT ON COLUMN source1.craft_market_wide.craftsman_address IS 'адрес мастера';
COMMENT ON COLUMN source1.craft_market_wide.craftsman_birthday IS 'дата рождения мастера';
COMMENT ON COLUMN source1.craft_market_wide.craftsman_email IS 'электронная почта мастера';
COMMENT ON COLUMN source1.craft_market_wide.product_id IS 'идентификтор товара ручной работы';
COMMENT ON COLUMN source1.craft_market_wide.product_name IS 'название товара ручной работы';
COMMENT ON COLUMN source1.craft_market_wide.product_description IS 'описание товара ручной работы';
COMMENT ON COLUMN source1.craft_market_wide.product_type IS 'тип товара ручной работы';
COMMENT ON COLUMN source1.craft_market_wide.product_price IS 'цена товара ручной работы';
COMMENT ON COLUMN source1.craft_market_wide.order_id IS 'идентификатор заказа';
COMMENT ON COLUMN source1.craft_market_wide.order_created_date IS 'дата создания заказа';
COMMENT ON COLUMN source1.craft_market_wide.order_completion_date IS 'дата выполнения заказа';
COMMENT ON COLUMN source1.craft_market_wide.order_status IS 'статус выполнения заказа (created, in progress, delivery, done)';
COMMENT ON COLUMN source1.craft_market_wide.customer_id IS 'идентификатор заказчика';
COMMENT ON COLUMN source1.craft_market_wide.customer_name IS 'ФИО заказчика';
COMMENT ON COLUMN source1.craft_market_wide.customer_address IS 'адрес заказчика';
COMMENT ON COLUMN source1.craft_market_wide.customer_birthday IS 'дата рождения заказчика';
COMMENT ON COLUMN source1.craft_market_wide.customer_email IS 'электронная почта заказчика';

