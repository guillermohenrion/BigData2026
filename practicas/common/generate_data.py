# Databricks notebook source
"""Generador determinista de datos para el caso de fraude en e-commerce."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _pick(values, numeric_column):
    """Selecciona de un array SQL usando un índice determinista."""
    index = (numeric_column + F.lit(1)).cast("int")
    return F.element_at(F.array(*[F.lit(value) for value in values]), index)


def _timestamp_from_offset(base_timestamp: str, seconds_column):
    """Suma segundos a un instante sin depender de aritmética de intervalos."""
    base_epoch = F.unix_timestamp(F.lit(base_timestamp), "yyyy-MM-dd HH:mm:ss")
    return F.from_unixtime(base_epoch + seconds_column).cast("timestamp")


def build_course_data(spark, config) -> dict[str, DataFrame]:
    """Construye clientes, productos, transacciones y eventos reproducibles."""
    rows = config.rows
    seed = config.seed

    customers = (
        spark.range(rows["customers"])
        .withColumnRenamed("id", "customer_id")
        .withColumn("country", _pick(["AR", "BR", "CL", "UY", "MX"], F.pmod(F.hash("customer_id", F.lit(seed)), F.lit(5))))
        .withColumn("segment", _pick(["new", "regular", "premium"], F.pmod(F.hash("customer_id", F.lit(seed + 1)), F.lit(3))))
        .withColumn("created_date", F.date_add(F.lit("2024-01-01"), F.pmod(F.col("customer_id"), F.lit(730)).cast("int")))
        .withColumn("email", F.concat(F.lit("user_"), F.col("customer_id"), F.lit("@example.test")))
    )

    products = (
        spark.range(rows["products"])
        .withColumnRenamed("id", "product_id")
        .withColumn("category", _pick(["electronics", "home", "books", "sports", "fashion"], F.pmod(F.hash("product_id", F.lit(seed)), F.lit(5))))
        .withColumn("price", (F.lit(5.0) + F.pmod(F.hash("product_id", F.lit(seed + 2)), F.lit(150000)) / F.lit(100.0)).cast("decimal(12,2)"))
    )

    transactions_typed = (
        spark.range(rows["transactions"])
        .withColumnRenamed("id", "transaction_id")
        .withColumn("customer_id", F.pmod(F.hash("transaction_id", F.lit(seed)), F.lit(rows["customers"])).cast("long"))
        .withColumn("product_id", F.pmod(F.hash("transaction_id", F.lit(seed + 1)), F.lit(rows["products"])).cast("long"))
        .withColumn("event_ts", _timestamp_from_offset("2026-03-01 00:00:00", F.pmod(F.hash("transaction_id", F.lit(seed + 2)), F.lit(604800))))
        .withColumn("amount", (F.lit(1.0) + F.pmod(F.hash("transaction_id", F.lit(seed + 3)), F.lit(200000)) / F.lit(100.0)).cast("decimal(12,2)"))
        .withColumn("payment_channel", _pick(["card", "wallet", "transfer"], F.pmod(F.hash("transaction_id", F.lit(seed + 4)), F.lit(3))))
        .withColumn("device_id", F.concat(F.lit("device_"), F.pmod(F.hash("customer_id", F.lit(seed + 5)), F.lit(max(10, rows["customers"] // 2)))))
        .withColumn("is_fraud", ((F.col("amount") > 1750) | ((F.col("payment_channel") == "transfer") & (F.pmod(F.col("transaction_id"), F.lit(97)) == 0))).cast("int"))
    )

    # La fuente CSV imita problemas reales: tipos como texto, importes inválidos y duplicados.
    transactions_raw = (
        transactions_typed
        .withColumn("amount", F.when(F.pmod("transaction_id", F.lit(997)) == 0, F.lit("N/A")).otherwise(F.col("amount").cast("string")))
        .withColumn("event_ts", F.date_format("event_ts", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("is_fraud", F.col("is_fraud").cast("string"))
    )
    duplicates = transactions_raw.where(F.pmod("transaction_id", F.lit(4999)) == 0)
    transactions_raw = transactions_raw.unionByName(duplicates)

    events = (
        spark.range(rows["events"])
        .withColumnRenamed("id", "event_id")
        .withColumn("customer_id", F.pmod(F.hash("event_id", F.lit(seed)), F.lit(rows["customers"])).cast("long"))
        .withColumn("product_id", F.pmod(F.hash("event_id", F.lit(seed + 1)), F.lit(rows["products"])).cast("long"))
        .withColumn("event_type", _pick(["view", "search", "add_to_cart", "checkout"], F.pmod(F.hash("event_id", F.lit(seed + 2)), F.lit(4))))
        .withColumn("event_ts", _timestamp_from_offset("2026-03-01 00:00:00", F.pmod(F.hash("event_id", F.lit(seed + 3)), F.lit(604800))))
        .withColumn("context", F.struct(_pick(["web", "android", "ios"], F.pmod(F.hash("event_id", F.lit(seed + 4)), F.lit(3))).alias("platform"), F.concat(F.lit("session_"), F.pmod(F.hash("event_id", F.lit(seed + 5)), F.lit(max(10, rows["customers"] * 2)))).alias("session_id")))
    )

    return {
        "customers": customers,
        "products": products,
        "transactions_typed": transactions_typed,
        "transactions_raw": transactions_raw,
        "events": events,
    }


def write_landing_files(data: dict[str, DataFrame], config) -> dict[str, str]:
    """Materializa fuentes heterogéneas en el volumen landing."""
    base = config.volume_path
    paths = {
        "customers_csv": f"{base}/customers_csv",
        "products_parquet": f"{base}/products_parquet",
        "transactions_csv": f"{base}/transactions_csv",
        "events_json": f"{base}/events_json",
    }
    data["customers"].coalesce(2).write.mode("overwrite").option("header", True).csv(paths["customers_csv"])
    data["products"].write.mode("overwrite").parquet(paths["products_parquet"])
    data["transactions_raw"].coalesce(4).write.mode("overwrite").option("header", True).csv(paths["transactions_csv"])
    data["events"].coalesce(4).write.mode("overwrite").json(paths["events_json"])
    return paths


def build_challenge_batch(spark, config) -> DataFrame:
    """Lote con evolución de esquema, duplicados y valores problemáticos."""
    start = config.rows["events"]
    batch = (
        spark.range(start, start + max(100, config.rows["events"] // 20))
        .withColumnRenamed("id", "event_id")
        .withColumn("customer_id", F.pmod(F.hash("event_id", F.lit(config.seed)), F.lit(config.rows["customers"])).cast("long"))
        .withColumn("product_id", F.pmod(F.hash("event_id", F.lit(config.seed + 1)), F.lit(config.rows["products"])).cast("long"))
        .withColumn("event_type", _pick(["view", "checkout", "refund"], F.pmod(F.hash("event_id"), F.lit(3))))
        .withColumn("event_ts", _timestamp_from_offset("2026-03-08 00:00:00", F.pmod(F.hash("event_id"), F.lit(86400))))
        .withColumn("context", F.struct(_pick(["web", "android", "ios"], F.pmod(F.hash("event_id", F.lit(config.seed + 4)), F.lit(3))).alias("platform"), F.concat(F.lit("session_"), F.pmod(F.hash("event_id"), F.lit(max(10, config.rows["customers"] * 2)))).alias("session_id")))
        .withColumn("app_version", _pick(["2.0.0", "2.1.0"], F.pmod(F.hash("event_id"), F.lit(2))))
    )
    duplicate = batch.limit(1)
    return batch.unionByName(duplicate)
