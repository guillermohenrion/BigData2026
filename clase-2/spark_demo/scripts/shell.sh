#!/usr/bin/env bash
# Abre el shell interactivo de PySpark conectado al cluster, para tipear en vivo.
# Ya trae sc (SparkContext) y spark (SparkSession). Salir con exit() o Ctrl+D.
# Los snippets para copiar están en el README ("Modo en vivo").
export MSYS_NO_PATHCONV=1
exec docker exec -it spark-master /opt/spark/bin/pyspark --master spark://spark-master:7077 "$@"
