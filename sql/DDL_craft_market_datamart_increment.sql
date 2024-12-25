-- DDL витрины данных
DROP TABLE IF EXISTS dwh.craftsman_report_datamart CASCADE;

CREATE TABLE IF NOT EXISTS dwh.craftsman_report_datamart (
	id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL, -- идентификатор записи
	craftsman_id BIGINT NOT NULL, -- идентификатор мастера
	craftsman_name VARCHAR NOT NULL, -- ФИО мастера
	craftsman_address VARCHAR NOT NULL, -- адрес мастера
	craftsman_birthday DATE NOT NULL, -- дата рождения мастера
	craftsman_email VARCHAR NOT NULL, -- электронная почта мастера
	craftsman_money NUMERIC(15,2) NOT NULL, -- сумма денег, которую заработал мастер (-10% на платформы) за месяц
	platform_money BIGINT NOT NULL, -- сумма денег, которая заработала платформа от продаж мастера за месяц
	count_order BIGINT NOT NULL, -- количество заказов у мастера за месяц
	avg_price_order NUMERIC(10,2) NOT NULL, -- средняя стоимость одного заказа у мастера за месяц
	avg_age_customer NUMERIC(3,1) NOT NULL, -- средний возраст покупателей
	median_time_order_completed NUMERIC(10,1), -- медианное время в днях от момента создания заказа до его завершения  за месяц
	top_product_category VARCHAR NOT NULL, -- самая популярная категория товаров у этого мастера  за месяц
	count_order_created BIGINT NOT NULL, -- количество созданных заказов за месяц
	count_order_in_progress BIGINT NOT NULL, -- количество заказов в процессе изготовки за месяц
	count_order_delivery BIGINT NOT NULL, -- количество заказов в доставке за месяц
	count_order_done BIGINT NOT NULL, -- количество завершенных заказов за месяц
	count_order_not_done BIGINT NOT NULL, -- количество незавершенных заказов за месяц
	report_period VARCHAR NOT NULL, -- отчетный период год и месяц
	CONSTRAINT craftsman_report_datamart_pk PRIMARY KEY (id)
);

-- DDL таблицы инкрементальных загрузок
DROP TABLE IF EXISTS dwh.load_dates_craftsman_report_datamart CASCADE;

CREATE TABLE IF NOT EXISTS dwh.load_dates_craftsman_report_datamart (
	id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
	load_dttm DATE NOT NULL,
	CONSTRAINT load_dates_craftsman_report_datamart_pk PRIMARY KEY (id)
);
