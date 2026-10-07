# Demo Spark Structured Streaming — procesando ventas de Kafka en tiempo real

Continúa la demo de [`../kafka-demo`](../kafka-demo): un productor genera ventas en tiempo real
en un topic de Kafka y **Spark Structured Streaming** las procesa a medida que llegan:
micro-batches, agregaciones con estado, ventanas por tiempo de evento con watermark, un pipeline
Kafka → Spark → Kafka y recuperación con checkpoint.

```
spark-streaming-demo/
├─ docker-compose.yml      # contenedor spark-streaming (apache/spark:3.5.3) en la red de kafka-demo
└─ scripts/
   ├─ demo.sh              # guion paso a paso, con pausas (y el productor de ventas)
   ├─ streaming.py         # los jobs: crudo, acumulado, ventanas, alertas, checkpoint
   └─ log4j2.properties    # logs del driver en WARN, para que se vean las tablas de cada batch
```

## Requisitos

- El cluster Kafka corriendo: `cd ../kafka-demo && docker compose up -d`.
- Docker Desktop con ~1 GB más de RAM (el driver de Spark en modo local).
- **Internet la primera vez**: el paso 0 baja el conector de Kafka para Spark
  (`spark-sql-kafka-0-10_2.12:3.5.3`, ~11 jars) y lo deja cacheado en un volumen.
- Git Bash (Windows) o terminal (Mac/Linux).

## Antes de la clase

```bash
cd spark-streaming-demo
docker compose pull          # apache/spark:3.5.3 (la misma imagen de la clase 2)
AUTO=1 ./scripts/demo.sh     # ensayo completo sin pausas (~4 min); baja el conector
```

## Durante la clase

```bash
./scripts/demo.sh           # todo el guion, Enter entre pasos
./scripts/demo.sh 4         # solo un paso (desde el 2, el productor tiene que estar corriendo: paso 1)
./scripts/demo.sh 1 3       # un rango de pasos
RATE=50 ./scripts/demo.sh 1 # el productor genera 50 ventas/s (default 20)
./scripts/demo.sh parar     # detiene el productor (el cierre también lo hace)
```

UIs: **Spark** <http://localhost:4041> → pestaña *Structured Streaming* (solo mientras corre un
job; 4040 lo usa la demo Spark de la clase 2) · **Kafka UI** <http://localhost:8084>, topic
`ventas-stream`, para ver crecer los offsets.

| Paso | Qué se muestra | Duración |
|---|---|---|
| 0 | Contenedor Spark en la red de Kafka, topics `ventas-stream` y `alertas`, conector de Kafka | ~1 min la 1ª vez |
| 1 | Productor dentro de `kafka1`: 20 ventas/s con la hora actual (**tiempo de evento**) | |
| 2 | `readStream` de Kafka → consola: un **micro-batch** cada 5 s (~100 ventas) | 22 s |
| 3 | `groupBy("sucursal").sum("total")` en modo `complete`: el **estado** crece batch a batch | 27 s |
| 4 | **Ventanas de 10 s** sobre `ts` con **watermark** de 10 s, modo `update` | 42 s |
| 5 | Ventas ≥ $4.000.000 → topic `alertas` (Kafka → Spark → Kafka), con un consumidor leyéndolas | 25 s |
| 6 | **Checkpoint**: se corta el job, pasan 15 s, vuelve y retoma en el batch siguiente con los totales intactos | ~55 s |

### Puntos para comentar en clase

- **Mismo código que en batch.** `streaming.py` usa `groupBy`, `agg`, `filter`, `window`: el
  mismo DataFrame de la clase 2. La única diferencia es `readStream`/`writeStream`. Spark
  convierte la consulta en un plan incremental.
- **Paso 4, tiempo de evento vs. tiempo de procesamiento.** Las ventanas se arman con la hora de
  la venta (`ts`), no con la hora a la que llegó a Spark. Una ventana aparece en dos batches
  seguidos porque sus ventas llegan repartidas; el watermark dice hasta cuándo esperar datos
  atrasados antes de cerrarla y liberar el estado.
- **Paso 6, exactly-once.** El checkpoint guarda en `offsets/` qué rango de Kafka toma cada batch
  *antes* de procesarlo, y en `commits/` los que terminó. Si se corta a la mitad (`offsets` tiene
  un batch que `commits` no), al volver ese batch se reprocesa entero con el mismo rango: no se
  pierde ni se cuenta dos veces. El estado de la agregación también está ahí (`state/`).
- **Kafka como buffer.** Mientras el job está apagado, las ventas se acumulan en Kafka; el primer
  batch al volver es más grande porque procesa el atraso. Es el LAG del paso 5 de `kafka-demo`.

## Tipear a mano

```bash
export MSYS_NO_PATHCONV=1
docker exec -it spark-streaming /opt/spark/bin/pyspark \
  --jars "$(docker exec spark-streaming sh -c 'ls /root/.ivy2/jars/*.jar | paste -sd, -')"
```

```python
from pyspark.sql import functions as F
df = (spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19092,kafka3:19092")
      .option("subscribe", "ventas-stream").load())

ventas = df.select(F.from_json(F.col("value").cast("string"),
                   "ts string, sucursal string, producto string, cantidad int, total long").alias("v")).select("v.*")

q = (ventas.groupBy("producto").agg(F.sum("cantidad").alias("unidades"))
     .writeStream.outputMode("complete").format("console").start())
# ... mirar unos batches ...
q.stop()
```

Con el productor corriendo (`./scripts/demo.sh 1`). En otra terminal se puede ver la pestaña
*Structured Streaming* en <http://localhost:4041>: filas por segundo de entrada y de proceso,
duración de cada batch y tamaño del estado.

## Troubleshooting

- **`El cluster Kafka no está corriendo`**: `cd ../kafka-demo && docker compose up -d`.
- **Los batches salen vacíos**: el productor no está corriendo. `./scripts/demo.sh 1`.
- **El paso 0 tarda o falla bajando el conector**: necesita internet para Maven Central. Queda
  cacheado en el volumen `ivy_cache`; las siguientes veces no baja nada.
- **Mensajes `ERROR WriteToDataSourceV2Exec ... aborting` o `TaskKilled`** al terminar un job:
  es el micro-batch en curso que se cancela al hacer `query.stop()`. `demo.sh` los oculta.
- **El paso 6 arranca desde cero**: el paso borra el checkpoint al empezar a propósito; la
  segunda ejecución dentro del mismo paso es la que retoma.
- **Limpiar**: `docker compose down -v` (borra también el cache del conector).
