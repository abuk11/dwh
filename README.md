# ETL Pipeline с использованием Spark и PostgreSQL

Этот проект реализует ETL-пайплайн с использованием Apache Spark и PostgreSQL. Пайплайн поддерживает инкрементальную загрузку данных и создание витрины данных.
## Cтруктура проекта
├── csv_data \
│   ├── complete_craft_market_craftsmans_202305071534.csv \
│   ├── complete_craft_market_customers_202305071535.csv \
│   ├── complete_craft_market_masters_products_202305071535.csv \
│   ├── complete_craft_market_orders_202305071535.csv \
│   ├── complete_craft_market_orders_customers_202305071535.csv \
│   ├── complete_craft_market_wide_20230730.csv \
│   ├── complete_craft_products_orders_202310282315.csv \
│   └── complete_customers_202310282242.csv \
├── docker-compose.yml \
├── postgresql-42.7.4.jar \
├── README.md \
├── scripts \
│   ├── etl_pypeline.py \
│   └── requirements.txt \
└── sql \
    ├── DDL_craft_market_datamart_increment.sql \
    ├── DDL_craft_market_dwh.sql \
    ├── DDL_craft_market_source_1.sql \
    ├── DDL_craft_market_source_2.sql \
    └── DDL_craft_market_source_3.sql \


# Требования
    Docker
    Docker Compose

## Инструкции по установке

1. Клонируйте репо:
   ```bash
   git clone <repository_url>
   cd <repository_name>
   ```

2. Запустите контейнеры Docker:
    ```
    docker-compose up -d
    ```

3. Зайдите в контейнер spark:
   ```
   docker exec -it spark-container bash
    ```

4. Установите зависимости Python внутри контейнера Spark (если они ещё не установлены):
   ```
   pip install -r /home/jovyan/scripts/requirements.txt
   ```

5. Запустите ETL-пайплайн:
   ```
   python /home/jovyan/scripts/etl_pypeline.py
   ```
