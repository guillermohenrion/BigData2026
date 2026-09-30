# Demo Spark — 1 Master + 2 Workers en Docker, leyendo del cluster HDFS

Continúa la demo de [`../hdfs-demo`](../hdfs-demo): Spark lee el mismo `dataset_demo.csv`
de HDFS, reparte el cómputo entre 2 Workers, compara **recalcular vs. cachear en memoria** y
(opcional) sobrevive a que se caiga un Worker a mitad de un job.

Guion original: [`demo_spark.md`](demo_spark.md).

```
spark_demo/
├─ docker-compose.yml      # spark-master + spark-worker-1/2 en la red hdfs-demo_default
└─ scripts/
   ├─ demo.sh              # guion paso a paso, con pausas
   ├─ demo_spark.py        # los pasos 1-5 (corre dentro de spark-master con spark-submit)
   ├─ shell.sh             # shell interactivo pyspark, para tipear en vivo
   └─ log4j2.properties    # logs del driver en WARN (se ven los "Lost executor" del paso 5)
```

## Requisitos

- El cluster HDFS corriendo con el archivo subido: `cd ../hdfs-demo && ./scripts/demo.sh 0 1`.
- Docker Desktop con **~8 GB de RAM** (HDFS + Spark a la vez).
- Git Bash (Windows) o terminal (Mac/Linux).

## Antes de la clase

```bash
cd spark_demo
docker compose pull          # apache/spark:3.5.3, ~1 GB
AUTO=1 ./scripts/demo.sh     # ensayo completo sin pausas (~2 min)
```

## Durante la clase

```bash
./scripts/demo.sh           # todo el guion, Enter entre pasos
./scripts/demo.sh 4         # solo un paso
./scripts/demo.sh 2 4       # un rango de pasos
./scripts/demo.sh 5         # solo la caída del Worker
KILL_AFTER=8 VICTIMA=spark-worker-2 ./scripts/demo.sh 5
```

UIs para tener abiertas: **Master** <http://localhost:8080> · **Driver** <http://localhost:4040>
(solo mientras corre la aplicación) · HDFS <http://localhost:9870>.

| Paso | Qué se muestra | Tiempos de referencia |
|---|---|---|
| 0 | Levantar Spark y ver los 2 Workers en el Master | |
| 1 | El driver (`sc`) y un executor por Worker | |
| 2 | `sc.textFile("hdfs://namenode:9000/...")`: **3 bloques HDFS → 3 particiones → 3 tareas**, y en qué Worker corrió cada una | `count()` ≈ 3 s |
| 3 | Facturación por sucursal con `map` + `reduceByKey`: Map → Shuffle → Reduce, 2 stages | ≈ 7 s |
| 4 | **Sin cache vs. con `.cache()`** + 5 pasadas "iterativas" sobre el mismo dataset | 4,5 s → 0,9 s · 25 s → 7,5 s |
| 5 | (opcional) Job de 24 tareas; a los 12 s `docker stop spark-worker-1`: las tareas perdidas se reintentan en el otro Worker y **el total coincide con el del paso 3** | ≈ 40 s |

Los pasos 1-4 corren en **una sola aplicación Spark**: la UI de `:4040` acumula todos los jobs y
el RDD cacheado (pestaña *Storage*). Al terminar el paso 4 el script espera un Enter antes de
cerrar la aplicación, así podés recorrer la UI con calma.

Salida del paso 5:

```
$ docker stop spark-worker-1
ERROR TaskSchedulerImpl: Lost executor 1 on 172.20.0.7: Command exited with code 143
WARN TaskSetManager: Lost task 5.0 in stage 0.0 (TID 5) (172.20.0.7 executor 1): ExecutorLostFailure ...
  sum() sobre 24 tareas                       40.00 s
  Total facturado: $6.856.859.173.279
  stage 0  tarea  4  → spark-worker-1    1.76 s  FAILED
  stage 0  tarea  4  → spark-worker-2    3.51 s
  stage 0  tarea  5  → spark-worker-1    1.75 s  FAILED
  stage 0  tarea  5  → spark-worker-2    3.50 s
```

## Sesión interactiva (tipeando en el shell)

La misma demo, pero escrita en vivo en `pyspark`, un bloque por vez. Los valores de salida
son los del `dataset_demo.csv` que genera la demo de HDFS.

### Abrir el shell

```bash
./scripts/shell.sh --total-executor-cores 2    # deja 2 cores libres para otra aplicación
```

Aparece el logo de Spark y el prompt `>>>`. Ya vienen creados `sc` (SparkContext, para RDDs)
y `spark` (SparkSession, para DataFrames y SQL). Para salir: `exit()` o Ctrl+D.

### 1. Conectado al cluster

```python
sc.master                 # 'spark://spark-master:7077'
sc.defaultParallelism     # 2  (los cores que pediste)
```

En <http://localhost:8080> aparece la aplicación `PySparkShell` con un executor por Worker.

### 2. Evaluación perezosa: las transformaciones no ejecutan nada

```python
lines = sc.textFile("hdfs://namenode:9000/demo/dataset_demo.csv")
ventas = lines.map(lambda l: l.split(","))      # vuelve al instante: todavía no leyó nada
```

<http://localhost:4040> → *Jobs* está vacío. Recién la **acción** dispara el trabajo:

```python
lines.count()             # 9013345  → ahora sí aparece un job en la UI
lines.getNumPartitions()  # 3  (= bloques en HDFS)
lines.take(3)
```

### 3. Map → Shuffle → Reduce con RDDs

Columnas: `fecha,sucursal,producto,cantidad,precio_unitario,total`.

```python
from operator import add
datos = lines.filter(lambda l: not l.startswith("fecha")).map(lambda l: l.split(","))

unidades = datos.map(lambda f: (f[2], int(f[3]))).reduceByKey(add)
unidades.takeOrdered(3, key=lambda x: -x[1])
# [('Impresora', 3382466), ('Celular', 3382452), ('Tablet', 3381814)]

print(unidades.toDebugString().decode())       # linaje: el "+-" es el shuffle
```

```
(3) PythonRDD[6] ...
 |  ShuffledRDD[4] at partitionBy          ← stage 2 (después del shuffle)
 +-(3) PairwiseRDD[3] at reduceByKey       ← el "+-" marca el corte de stage
    |  PythonRDD[2] ...
    |  hdfs://.../dataset_demo.csv HadoopRDD[0]   ← stage 1: lectura de HDFS
```

El DAG gráfico: <http://localhost:4040> → *Jobs* → el job → *DAG Visualization*.

### 4. Cache

```python
import time
def medir(f):
    t = time.time(); r = f(); print(round(time.time() - t, 2), "s"); return r

medir(datos.count)        # ~4-5 s
medir(datos.count)        # ~4-5 s otra vez: recalcula desde HDFS
datos.cache()
medir(datos.count)        # ~6 s: calcula y guarda en memoria
medir(datos.count)        # <1 s: sale de memoria → mirá la pestaña Storage
```

Después de pegar el `def medir`, dale Enter en la línea vacía para cerrar la función.

### 5. DataFrames y el optimizer (Catalyst)

Catalyst optimiza **DataFrames y SQL**, no RDDs: con `map`/`reduceByKey` Spark ejecuta
exactamente lo que escribiste.

```python
from pyspark.sql import functions as F
df = spark.read.csv("hdfs://namenode:9000/demo/dataset_demo.csv", header=True, inferSchema=True)
df.printSchema()
df.show(5)

q = (df.filter(F.col("producto") == "Notebook")
       .groupBy("sucursal").agg(F.sum("total").alias("facturado"))
       .filter(F.col("sucursal") != "Salta")      # a propósito DESPUÉS del groupBy
       .orderBy(F.desc("facturado")))
q.show()
q.explain(True)           # Parsed → Analyzed → Optimized → Physical
```

`inferSchema=True` lee el archivo una vez de más para adivinar los tipos: tarda unos segundos.

`q.show()` da Rosario primero con **$360.774.174.285**: el mismo número que calculó
`consulta.sh` en la demo de HDFS con `hdfs dfs -cat | awk`, leyendo bloque por bloque desde un
solo proceso. Acá lo calcularon varias tareas en paralelo, en una línea.

Qué mirar en el `explain`:

| Optimización | Dónde se ve |
|---|---|
| **Predicate pushdown** | En el *Optimized Logical Plan*, el filtro `sucursal != Salta` bajó por debajo del `Aggregate` y se unió con el de `Notebook` en un solo `Filter`. |
| **Column pruning** | `ReadSchema` lista solo `sucursal`, `producto` y `total`: de las 6 columnas del CSV lee 3. |
| **Filtros empujados al lector** | `PushedFilters: [... EqualTo(producto,Notebook), Not(EqualTo(sucursal,Salta))]` |
| **Combiner automático** | Dos `HashAggregate`: `partial_sum` antes del `Exchange` (el shuffle) y `sum` después. |

Versión más legible: `q.explain("formatted")`. En la UI: <http://localhost:4040> →
*SQL / DataFrame* → la consulta, con el plan físico como grafo y las filas de cada operador.

### 6. Spark SQL

```python
df.createOrReplaceTempView("ventas")

spark.sql("""
  SELECT producto, SUM(cantidad) AS unidades, ROUND(AVG(precio_unitario)) AS precio_medio
  FROM ventas
  GROUP BY producto
  ORDER BY unidades DESC
""").show()
```

Por dentro es el mismo motor y el mismo optimizer que el DataFrame del paso 5.

### 7. Tolerancia a fallas (opcional)

Lanzá un job de ~40 s y, mientras corre, en otra terminal `docker stop spark-worker-1`
(al final, `docker start spark-worker-1`):

```python
def lento(it):
    time.sleep(3)                      # cada tarea tarda unos segundos
    yield sum(1 for _ in it)

sc.textFile("hdfs://namenode:9000/demo/dataset_demo.csv", 24).mapPartitions(lento).sum()
```

En el shell aparecen los `Lost executor` / `Lost task` y el resultado sale igual.

### Tips para tipear en vivo

- Un bloque de varias líneas entre paréntesis o triples comillas se pega entero.
- `lines.reduceByKey?` no funciona (es el REPL de Python, no IPython): usá `help(lines.reduceByKey)`.
- Si una acción queda colgada sin avanzar, mirá <http://localhost:8080>: si otra aplicación
  tiene todos los cores, la tuya queda en *WAITING*. Por eso conviene `--total-executor-cores 2`.
- Ctrl+C cancela el job en curso sin cerrar el shell.

## Diferencias con el guion original

- **Imagen `apache/spark:3.5.3` en vez de `bitnami/spark:3.5`**: Bitnami retiró sus imágenes
  gratuitas de Docker Hub en 2025 y `bitnami/spark:3.5` ya no existe. La oficial no usa
  `SPARK_MODE`: el compose arranca Master y Workers con `spark-class` y los paths son
  `/opt/spark/...` en vez de `/opt/bitnami/spark/...`.
- **`dataset_demo.csv` en vez de `.txt`**: es el CSV de ventas que sube la demo de HDFS. En lugar
  de un word count, el paso 3 calcula la facturación por sucursal: es el mismo patrón
  `map` → `reduceByKey`, con un resultado que se puede leer.
- **Transformación cara = parsear el CSV** en vez de `time.sleep(0.0001)` por línea: con 9 millones
  de líneas ese sleep son ~5 minutos por pasada. Parsear da una diferencia honesta
  (≈ 4,5 s vs. 0,9 s) y el paso 4b la amplifica con 5 pasadas, como un algoritmo iterativo.
- **Workers con 2 GB** en vez de 1 GB, para que el RDD cacheado entre entero en memoria.
- **Carpeta `spark_demo/`** (la que ya existía) en vez de `spark-demo/`.
- Sin `version: "3"` (Compose v2 lo marca como obsoleto).

## Troubleshooting

- **`network hdfs-demo_default not found`**: el cluster HDFS no está levantado. `cd ../hdfs-demo && docker compose up -d`.
- **`the input device is not a TTY`** en `shell.sh` o en las pausas: usá la terminal de VS Code o
  Windows Terminal, o anteponé `winpty` (`winpty ./scripts/shell.sh`). `AUTO=1` corre sin TTY.
- **`hdfs://namenode:9000/...` da error de conexión**: `docker ps` (¿está `namenode`?) y
  `MSYS_NO_PATHCONV=1 docker exec namenode hdfs dfs -ls /demo`.
- **`http://localhost:4040` no abre**: la UI del driver existe solo mientras la aplicación está
  viva (durante `demo.sh` o con `shell.sh` abierto).
- **Poca memoria**: bajá `--memory 2G` a `1G` en el compose (el cache puede no entrar entero
  y la diferencia del paso 4 se achica), o corré con un solo Worker.
- **Limpiar**: `docker compose down` acá (y `./scripts/reset.sh` en `hdfs-demo/` para borrar HDFS).
