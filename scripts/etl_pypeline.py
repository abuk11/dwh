"""
Финальный ETL-пайплайн:
1) Создаём источники (DDL для source1, source2, source3)
2) Загружаем все CSV-файлы в таблицы источников
3) Создаём DWH (факты и измерения)
4) Создаём Incremental Datamart (инкрементальная витрина данных)
5) Записываем дату последней загрузки
"""

import os
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit, when, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType, LongType

POSTGRES_HOST = "postgres-container"
POSTGRES_PORT = "5432"
POSTGRES_DB = "postgres"
POSTGRES_USER = "postgres"
POSTGRES_PWD = "1234"

# JDBC-параметры для Spark
JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
JDBC_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PWD,
    "driver": "org.postgresql.Driver"
}

# Пути к DDL-скриптам и CSV-файлам
DDL_FILES = [
    "/home/jovyan/sql/DDL_craft_market_source_1.sql",
    "/home/jovyan/sql/DDL_craft_market_source_2.sql",
    "/home/jovyan/sql/DDL_craft_market_source_3.sql",
    "/home/jovyan/sql/DDL_craft_market_dwh.sql",
    "/home/jovyan/sql/DDL_craft_market_datamart_increment.sql"
]

CSV_FILES = {
    "source1": "/home/jovyan/csv_data/complete_craft_market_wide_20230730.csv",
    "source2": {
        "orders_customers": "/home/jovyan/csv_data/complete_craft_market_orders_customers_202305071535.csv",
        "masters_products": "/home/jovyan/csv_data/complete_craft_market_masters_products_202305071535.csv"
    },
    "source3": {
        "orders": "/home/jovyan/csv_data/complete_craft_market_orders_202305071535.csv",
        "craftsmans": "/home/jovyan/csv_data/complete_craft_market_craftsmans_202305071534.csv",
        "customers": "/home/jovyan/csv_data/complete_craft_market_customers_202305071535.csv"
    }
}

def remove_generated_columns(df, generated_columns):
    """
    Удаляет автоматически генерируемые столбцы из DataFrame.
    """
    for col_name in generated_columns:
        if col_name in df.columns:
            df = df.drop(col_name)
    return df

def run_sql_script(sql_file_path):
    """
    Выполнение SQL-скрипта для создания таблиц в базе данных.
    """
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PWD
        )
        conn.autocommit = True
        with open(sql_file_path, "r", encoding="utf-8") as f, conn.cursor() as cursor:
            cursor.execute(f.read())
        print(f"Выполнен SQL-скрипт: {sql_file_path}")
    except Exception as e:
        print(f"Ошибка при выполнении SQL-скрипта {sql_file_path}: {e}")
    finally:
        conn.close()

def get_not_null_columns(table_name, schema_name):
    query = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' AND is_nullable = 'NO';
    """
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PWD
    )
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchall()
    conn.close()
    return result


def map_postgres_to_spark_type(pg_type):
    """
    Сопоставляет тип PostgreSQL с типом Spark.
    """
    mapping = {
        "bigint": LongType(),
        "int8": LongType(),
        "integer": IntegerType(),
        "numeric": DecimalType(10, 2),
        "date": DateType(),
        "character varying": StringType(),
        "varchar": StringType(),
        "text": StringType()
    }
    return mapping.get(pg_type, StringType())  # default to String if unknown


def get_table_columns(schema_name, table_name):
    """
    Возвращает [(column_name, data_type, is_nullable), ...]
    для указанной таблицы в PostgreSQL.
    """
    query = f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
        ORDER BY ordinal_position;
    """
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PWD
    )
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
        return rows
    except Exception as e:
        print("Не удалось получить колонки:", e)
        return []
    finally:
        conn.close()

def map_postgres_to_spark_type(pg_type):
    """
    Сопоставляет тип PostgreSQL с типом Spark.
    """
    mapping = {
        "bigint": LongType(),
        "int8": LongType(),
        "integer": IntegerType(),
        "numeric": DecimalType(10, 2),
        "date": DateType(),
        "character varying": StringType(),
        "varchar": StringType(),
        "text": StringType()
    }
    return mapping.get(pg_type, StringType())  # default to String if unknown

def get_table_columns(schema_name, table_name):
    """
    Возвращает [(column_name, data_type, is_nullable), ...]
    для указанной таблицы в PostgreSQL.
    """
    query = f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
        ORDER BY ordinal_position;
    """
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PWD
    )
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
        return rows
    except Exception as e:
        print("Не удалось получить колонки:", e)
        return []
    finally:
        conn.close()

def create_spark_schema(schema_name, table_name):
    """
    Создаёт Spark StructType на основе структуры таблицы PostgreSQL.
    """
    columns_info = get_table_columns(schema_name, table_name)
    fields = []
    for col_name, pg_type, is_nullable in columns_info:
        spark_type = map_postgres_to_spark_type(pg_type)
        nullable = (is_nullable == "YES")
        fields.append(StructField(col_name, spark_type, nullable))
    return StructType(fields)

def create_spark_schema(schema_name, table_name):
    """
    Создаёт Spark StructType на основе структуры таблицы PostgreSQL.
    """
    columns_info = get_table_columns(schema_name, table_name)
    fields = []
    for col_name, pg_type, is_nullable in columns_info:
        spark_type = map_postgres_to_spark_type(pg_type)
        nullable = (is_nullable == "YES")
        fields.append(StructField(col_name, spark_type, nullable))
    return StructType(fields)

def get_table_schema(spark, table_name):
    """
    Fetches the schema of the PostgreSQL table and returns a corresponding Spark StructType schema.
    """
    query = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = '{table_name.split('.')[0]}' AND table_name = '{table_name.split('.')[1]}';
    """
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PWD
    )
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchall()
    conn.close()

    # Mэпим PostgreSQL типы в Spark типы
    type_mapping = {
        "character varying": StringType(),
        "varchar": StringType(),
        "text": StringType(),
        "integer": IntegerType(),
        "bigint": LongType(),
        "int8": LongType(),
        "numeric": DecimalType(10, 2),
        "date": DateType()
    }

    fields = [
        StructField(column_name, type_mapping[data_type], True)
        for column_name, data_type in result
    ]
    return StructType(fields)
    

def load_csv_to_db(spark, csv_path, schema_name, table_name):
    """
    1) Получаем схему PostgreSQL для table_name
    2) Читаем CSV в DataFrame (raw)
    3) Кастуем поля raw в соответствии со схемой и применяем NOT NULL
    4) Применяем nullable=True/False в зависимости от PostgreSQL
    5) Drop auto-generated columns
    6) fillna для NOT NULL колонок
    7) Insert в PostgreSQL
    """
    # 1) PostgreSQL schema -> Spark schema
    pg_columns = get_table_columns(schema_name, table_name)

    # Составим список NOT NULL колонок
    not_null_cols = [col_name for (col_name, pg_type, is_nullable) in pg_columns
                     if is_nullable == "NO"]

    # 2) Чтение CSV
    raw_df = spark.read.option("header", True).csv(csv_path)

    # 3) Каст полей
    casted_df = raw_df
    for col_name, pg_type, is_nullable in pg_columns:
        if col_name in casted_df.columns:
            # Определяем целевой Spark-тип
            spark_type = map_postgres_to_spark_type(pg_type)

            # Кастуем данные
            if isinstance(spark_type, DateType):
                casted_df = casted_df.withColumn(
                    col_name, to_date(col(col_name), "yyyy-MM-dd")
                )
            else:
                casted_df = casted_df.withColumn(
                    col_name, col(col_name).cast(spark_type)
                )

    # 4) Убираем авто-сгенерированные столбцы (не факт, что нужно)
    # auto_generated = ["id", "craftsman_id", "product_id", "order_id", "customer_id"]
    # for ag in auto_generated:
    #     if ag in casted_df.columns:
    #         casted_df = casted_df.drop(ag)

    # 5) Применение nullable=True/False
    def enforce_nullable(df, pg_columns):
        """
        Применяет правильное значение nullable для колонок DataFrame.
        """
        fields = []
        for col_name, pg_type, is_nullable in pg_columns:
            spark_type = map_postgres_to_spark_type(pg_type)
            nullable = (is_nullable == "YES")
            fields.append(StructField(col_name, spark_type, nullable))
        schema = StructType(fields)
        return spark.createDataFrame(df.rdd, schema)

    casted_df = enforce_nullable(casted_df, pg_columns)

    # 6) fillna для NOT NULL колонок
    for col_name in not_null_cols:
            if col_name in casted_df.columns:
                pg_type = next((col[1] for col in pg_columns if col[0] == col_name), "text")
                # Заполним неизвестные типы
                if pg_type in ("character varying", "text", "varchar"):
                    default_value = "Unknown"
                elif pg_type in ("bigint", "int8", "integer"):
                    default_value = -1
                elif pg_type == "numeric":
                    default_value = 0.0
                elif pg_type == "date":
                    default_value = "1970-01-01"
                else:
                    default_value = None  # Ловим неизвестные типы
                casted_df = casted_df.withColumn(col_name, when(col(col_name).isNull(), lit(default_value)).otherwise(col(col_name)))

    total_rows = casted_df.count()
    missing_values = (
        casted_df.select(
            [
                (count(when(col(c).isNull(), 1)) / total_rows).alias(c)
                for c in casted_df.columns
            ]
        )
    )
    # print('LOL')
    # print("Пропущенные значения по колонкам:")
    print(missing_values.show())


    # Дебаг схемы и данных
    print("Schema after enforcing PostgreSQL constraints:")
    casted_df.printSchema()

    # 7) Вставка в PostgreSQL
    casted_df.write.jdbc(
        url=JDBC_URL,
        table=f"{schema_name}.{table_name}",
        mode="append",
        properties=JDBC_PROPERTIES
    )
    print(f"CSV {os.path.basename(csv_path)} успешно загружен в {schema_name}.{table_name}.")


def populate_dwh(spark):
    """
    Заполнение таблиц измерений и фактов в DWH.
    """
    fact_orders = spark.read.jdbc(JDBC_URL, "source1.craft_market_wide", properties=JDBC_PROPERTIES)
    dim_craftsman = fact_orders.select("craftsman_id", "craftsman_name", "craftsman_address",
                                        "craftsman_birthday", "craftsman_email").distinct()
    dim_craftsman.write.jdbc(url=JDBC_URL, table="dwh.dim_craftsman", mode="overwrite", properties=JDBC_PROPERTIES)

    dim_customer = fact_orders.select("customer_id", "customer_name", "customer_address",
                                       "customer_birthday", "customer_email").distinct()
    dim_customer.write.jdbc(url=JDBC_URL, table="dwh.dim_customer", mode="overwrite", properties=JDBC_PROPERTIES)

    # Заполнение таблицы фактов
    fact_orders.write.jdbc(url=JDBC_URL, table="dwh.fact_orders", mode="overwrite", properties=JDBC_PROPERTIES)
    print("Таблицы DWH заполнены.")

def create_incremental_datamart(spark):
    """
    Создание витрины данных с инкрементальной загрузкой.
    """
    fact_orders = spark.read.jdbc(JDBC_URL, "dwh.fact_orders", properties=JDBC_PROPERTIES)
    dim_craftsman = spark.read.jdbc(JDBC_URL, "dwh.dim_craftsman", properties=JDBC_PROPERTIES)
    dim_customer = spark.read.jdbc(JDBC_URL, "dwh.dim_customer", properties=JDBC_PROPERTIES)

    datamart = fact_orders.join(dim_craftsman, "craftsman_id") \
                          .join(dim_customer, "customer_id") \
                          .select(
                              "order_id", "craftsman_name", "customer_name",
                              "product_id", "product_price", "order_created_date"
                          )
    datamart.write.jdbc(url=JDBC_URL, table="dwh.craftsman_report_datamart", mode="append", properties=JDBC_PROPERTIES)
    print("Витрина данных создана инкрементально.")

def main():
    """
    Основной процесс ETL.
    """
    spark = SparkSession.builder \
        .appName("ETL Pipeline") \
        .config("spark.jars", "/home/jovyan/postgresql-42.7.4.jar") \
        .getOrCreate()

    # Выполнение SQL-скриптов для создания таблиц
    for ddl_file in DDL_FILES:
        run_sql_script(ddl_file)

    # Загрузка данных из источников
    # source1
    load_csv_to_db(spark, CSV_FILES["source1"], "source1", "craft_market_wide")

    # source2
    load_csv_to_db(spark, CSV_FILES["source2"]["orders_customers"], "source2", "craft_market_orders_customers")
    load_csv_to_db(spark, CSV_FILES["source2"]["masters_products"], "source2", "craft_market_masters_products")

    # source3
    load_csv_to_db(spark, CSV_FILES["source3"]["orders"], "source3", "craft_market_orders")
    load_csv_to_db(spark, CSV_FILES["source3"]["craftsmans"], "source3", "craft_market_craftsmans")
    load_csv_to_db(spark, CSV_FILES["source3"]["customers"], "source3", "craft_market_customers")

    # Заполнение DWH
    populate_dwh(spark)

    # Создание витрины данных
    create_incremental_datamart(spark)

    spark.stop()
    print("ETL процесс завершён.")

if __name__ == "__main__":
    main()

