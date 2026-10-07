"""Jobs de Spark Structured Streaming sobre el topic ventas-stream de Kafka (ver demo.sh).

    streaming.py crudo        -> los mensajes tal como llegan, micro-batch por micro-batch
    streaming.py acumulado    -> facturación por sucursal acumulada desde el principio
    streaming.py ventanas     -> ventas por ventana de 10 s de tiempo de evento, con watermark
    streaming.py alertas      -> ventas grandes escritas a otro topic de Kafka (alertas)
    streaming.py checkpoint   -> 'acumulado' con checkpoint: si se corta, retoma donde quedó

Corre DURACION segundos (variable de entorno, default 30) y termina.
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

BOOT = "kafka1:19092,kafka2:19092,kafka3:19092"
DURACION = int(os.environ.get("DURACION", "30"))
TRIGGER = os.environ.get("TRIGGER", "5 seconds")
UMBRAL = 4_000_000

esquema = StructType([
    StructField("ts", StringType()),
    StructField("sucursal", StringType()),
    StructField("producto", StringType()),
    StructField("cantidad", IntegerType()),
    StructField("total", LongType()),
])

modo = sys.argv[1] if len(sys.argv) > 1 else "crudo"
spark = (SparkSession.builder.appName("streaming-" + modo)
         .config("spark.sql.shuffle.partitions", "3")   # default 200: demasiadas tareas para una demo
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

if modo == "preparar":   # solo para que spark-submit baje el conector de Kafka (paso 0)
    spark.stop()
    sys.exit(0)

# 1) Fuente: el topic de Kafka como una tabla que no deja de crecer
crudo = (spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", BOOT)
         .option("subscribe", "ventas-stream")
         .option("startingOffsets", "latest")
         .load())

# 2) value llega en bytes: lo parseamos como JSON y convertimos ts a timestamp
ventas = (crudo
          .select(F.col("partition"), F.col("offset"),
                  F.from_json(F.col("value").cast("string"), esquema).alias("v"))
          .select("partition", "offset", "v.*")
          .withColumn("ts", F.to_timestamp("ts")))

if modo == "crudo":
    query = (ventas.writeStream.format("console")
             .option("truncate", False).option("numRows", 8)
             .trigger(processingTime=TRIGGER).start())

elif modo in ("acumulado", "checkpoint"):
    por_sucursal = (ventas.groupBy("sucursal")
                    .agg(F.count("*").alias("ventas"), F.sum("total").alias("facturado"))
                    .orderBy(F.desc("facturado")))
    w = (por_sucursal.writeStream.format("console").outputMode("complete")
         .trigger(processingTime=TRIGGER))
    if modo == "checkpoint":
        # Offsets leídos y estado de la agregación se guardan acá después de cada micro-batch
        w = w.option("checkpointLocation", "/tmp/checkpoints/acumulado")
    query = w.start()

elif modo == "ventanas":
    por_ventana = (ventas
                   .withWatermark("ts", "10 seconds")      # aceptar datos con hasta 10 s de atraso
                   .groupBy(F.window("ts", "10 seconds"))
                   .agg(F.count("*").alias("ventas"),
                        F.sum("total").alias("facturado"),
                        F.round(F.avg("total")).cast("long").alias("ticket_promedio"),
                        F.max("total").alias("venta_maxima"))
                   .select(F.date_format("window.start", "HH:mm:ss").alias("desde"),
                           F.date_format("window.end", "HH:mm:ss").alias("hasta"),
                           "ventas", "facturado", "ticket_promedio", "venta_maxima"))
    query = (por_ventana.writeStream.format("console").outputMode("update")
             .option("truncate", False).trigger(processingTime=TRIGGER).start())

elif modo == "alertas":
    grandes = (ventas.filter(F.col("total") >= UMBRAL)
               .select(F.col("sucursal").alias("key"),
                       F.to_json(F.struct("ts", "sucursal", "producto", "cantidad", "total")).alias("value")))
    query = (grandes.writeStream.format("kafka")
             .option("kafka.bootstrap.servers", BOOT)
             .option("topic", "alertas")
             .option("checkpointLocation", "/tmp/checkpoints/alertas")   # obligatorio para escribir a Kafka
             .trigger(processingTime="2 seconds").start())

else:
    sys.exit("modo desconocido: " + modo)

query.awaitTermination(DURACION)
p = query.lastProgress
if p:
    print("\n>>> último micro-batch: batchId={}  filas={}  {:.0f} filas/s  offsets={}".format(
        p["batchId"], p["numInputRows"], p.get("processedRowsPerSecond") or 0,
        p["sources"][0]["endOffset"]), flush=True)
query.stop()
spark.stop()
