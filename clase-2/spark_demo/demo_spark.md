# Demo de Spark para la clase
### Cluster Standalone (1 Master + 2 Workers) en Docker, conectado al cluster HDFS de la demo anterior, con guion de demo en vivo

---

## 0. Idea general de la demo

Esta demo continúa directamente la de HDFS. La idea es mostrar tres cosas en vivo, en este orden:

1. Spark puede leer datos directamente del cluster HDFS que ya armamos (continuidad storage → procesamiento).
2. Un mismo dataset se procesa **en paralelo** entre varios Workers, y se ve en la Spark UI cómo se reparten las tareas (paralelismo/particionamiento, lo mismo que vimos con MapReduce, pero mucho más rápido).
3. La diferencia entre **recalcular** un RDD cada vez y **cachearlo en memoria** — el punto central que separa a Spark de MapReduce.

Si te sobra tiempo, al final hay un paso opcional para mostrar tolerancia a fallas matando un Worker a mitad de un job, en el mismo espíritu que la demo de "matar un DataNode".

---

## 1. Preparación (antes de la clase)

### Requisitos
- Docker y Docker Compose instalados.
- Tener ya levantado el cluster HDFS de la demo anterior (`hdfs-demo/`), con el archivo `dataset_demo.txt` ya subido a `/demo/dataset_demo.txt`. Si no lo tenés corriendo, repetí los pasos 1 y 2 de esa demo antes de seguir.
- Al menos 6-8GB de RAM libres asignados a Docker (vamos a tener HDFS + Spark corriendo a la vez).

### `docker-compose.yml` del cluster Spark

Creá una carpeta al lado de `hdfs-demo/` (ej. `spark-demo/`) con este `docker-compose.yml`. Usamos las imágenes de Bitnami, que traen Spark preconfigurado, y conectamos este cluster a la misma red que el cluster HDFS para que los Workers puedan leer del NameNode.

```yaml
version: "3"

services:
  spark-master:
    image: bitnami/spark:3.5
    container_name: spark-master
    environment:
      - SPARK_MODE=master
    ports:
      - "8080:8080"   # UI web del Master (lista de Workers, jobs)
      - "7077:7077"   # Puerto del Master para submits
      - "4040:4040"   # UI web del driver (detalle de jobs/stages/tasks)
    networks:
      - hdfs-demo_default

  spark-worker-1:
    image: bitnami/spark:3.5
    container_name: spark-worker-1
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=1G
      - SPARK_WORKER_CORES=2
    ports:
      - "8081:8081"
    depends_on:
      - spark-master
    networks:
      - hdfs-demo_default

  spark-worker-2:
    image: bitnami/spark:3.5
    container_name: spark-worker-2
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=1G
      - SPARK_WORKER_CORES=2
    ports:
      - "8082:8081"
    depends_on:
      - spark-master
    networks:
      - hdfs-demo_default

networks:
  hdfs-demo_default:
    external: true
```

> El nombre `hdfs-demo_default` es la red que Docker Compose crea automáticamente para el proyecto `hdfs-demo` (Compose arma el nombre como `<carpeta>_default`). Si tu carpeta de HDFS se llama distinto, corré `docker network ls` para confirmar el nombre exacto antes de levantar Spark.

### Levantar el cluster

```bash
cd spark-demo
docker-compose up -d
```

Esperá unos 20 segundos y verificá:

```bash
docker ps
```

Deberías ver `spark-master`, `spark-worker-1` y `spark-worker-2`, además de los contenedores de HDFS ya corriendo de antes.

### Verificar la UI web del Master (muy visual para la clase)

Abrí `http://localhost:8080`. Vas a ver el Master con **2 Workers** registrados, cuántos cores y memoria tiene cada uno disponible, y (una vez que corramos algo) la lista de aplicaciones en ejecución. Dejá esta pestaña abierta durante toda la demo, al lado de la UI de HDFS (`http://localhost:9870`) que ya tenías abierta.

---

## 2. Guion de la demo en vivo

### Paso 1 — Entrar al shell interactivo de PySpark

```bash
docker exec -it spark-master /opt/bitnami/spark/bin/pyspark --master spark://spark-master:7077
```

Vas a ver el log de arranque y después el prompt `>>>`. Comentario para la clase: *"Este es el mismo shell interactivo que vimos en la teoría — acá abajo ya tenemos un SparkContext (`sc`) conectado al cluster de 2 Workers que están viendo en la UI."*

### Paso 2 — Leer el archivo directamente desde HDFS

Acá está la continuidad con la demo anterior: en vez de leer un archivo local, leemos el mismo `dataset_demo.txt` que subimos a HDFS.

```python
lines = sc.textFile("hdfs://namenode:9000/demo/dataset_demo.txt")
lines.count()
```

Comentario: *"Fíjense que no tuvimos que copiar nada — Spark fue directo a buscar los bloques al cluster HDFS que armamos antes. Storage y procesamiento son sistemas separados, y eso es a propósito."*

### Paso 3 — Un word count clásico, para conectar con la teoría de MapReduce

```python
from operator import add

counts = (lines
          .flatMap(lambda line: line.split(" "))
          .map(lambda word: (word, 1))
          .reduceByKey(add))

counts.count()          # cuántas palabras distintas hay
counts.takeOrdered(10, key=lambda x: -x[1])   # las 10 palabras más frecuentes
```

Comentario: *"Esto es exactamente Map → Shuffle → Reduce que vimos con MapReduce — `map` emite pares (palabra, 1), `reduceByKey` es el shuffle+reduce agrupando por clave. La diferencia es que lo escribimos como una sola cadena de transformaciones en un programa, no como jobs separados."*

Mientras corre, mostrá la pestaña **Jobs** de `http://localhost:4040` (la UI del driver, no la del Master): ahí se ve el DAG de stages, y cómo el trabajo se dividió en tareas repartidas entre los dos Workers. Esto es el equivalente visual al `fsck` que usamos para HDFS, pero para cómputo en vez de storage.

### Paso 4 — La demo estrella: recalcular vs. cachear en memoria

Este es el punto que separa a Spark de MapReduce. Simulamos una transformación "cara" (por ejemplo, con una demora artificial) y la reusamos varias veces.

```python
import time

def transformacion_costosa(line):
    time.sleep(0.0001)  # simula algo de trabajo por línea
    return line.upper()

datos = lines.map(transformacion_costosa)

# Primera pasada SIN cache: cada acción recalcula todo desde HDFS
inicio = time.time()
datos.count()
print("Sin cache, 1ra vez:", time.time() - inicio)

inicio = time.time()
datos.count()
print("Sin cache, 2da vez:", time.time() - inicio)
```

Vas a ver que la segunda vez tarda **prácticamente lo mismo** que la primera — porque Spark, por defecto, vuelve a ejecutar todo el linaje de transformaciones desde cero cada vez que se llama a una acción (evaluación perezosa: nada se calculó hasta que llamamos `count()`).

Ahora cacheamos:

```python
datos_cacheados = lines.map(transformacion_costosa).cache()

inicio = time.time()
datos_cacheados.count()   # esta primera vez SÍ hace el cómputo completo, y lo guarda en memoria
print("Con cache, 1ra vez (calcula y guarda):", time.time() - inicio)

inicio = time.time()
datos_cacheados.count()   # esta lee directo de memoria
print("Con cache, 2da vez (desde memoria):", time.time() - inicio)
```

**Acá está el momento clave de la demo:** la segunda llamada con `.cache()` debería ser notoriamente más rápida (varias veces menos tiempo) que sin cache, porque Spark ya tiene el RDD materializado en la memoria de los Workers y no vuelve a leer de HDFS ni a reaplicar la transformación.

Frase sugerida: *"Esto es literalmente la limitación de MapReduce que discutimos la clase pasada, resuelta en tres líneas de código. Un algoritmo iterativo de Machine Learning que necesita pasar 50 veces por el mismo dataset, en MapReduce son 50 lecturas completas desde disco; en Spark, es una sola lectura y 50 pasadas en memoria."*

También podés mostrar en la UI (`http://localhost:4040` → pestaña **Storage**) cómo aparece el RDD cacheado, cuánta memoria ocupa y en cuántas particiones está distribuido entre los Workers.

### Paso 5 (opcional) — Tolerancia a fallas: matar un Worker a mitad de un job

Mismo espíritu que matar un DataNode en la demo de HDFS, pero ahora del lado del cómputo.

Lanzá un job que tarde un poco (podés subir el `time.sleep` del paso anterior a `0.01` para que dure más, o correr `counts.collect()` sobre el dataset completo), y mientras está corriendo, en otra terminal:

```bash
docker stop spark-worker-1
```

El job va a tardar un poco más (porque hay que reasignar las tareas que estaban corriendo en `spark-worker-1`) pero **termina igual**, ya que el Master reprograma esas tareas en `spark-worker-2`. Podés mostrar esto en la pestaña **Executors** de la UI del driver: `spark-worker-1` va a aparecer como "dead" o desaparecer de la lista.

Comentario: *"Noten la diferencia con HDFS: ahí lo que se pierde y se recupera son réplicas de datos. Acá lo que se pierde y se reasigna son tareas de cómputo. Pero el mecanismo de fondo es el mismo principio de tolerancia a fallas que venimos viendo en toda la materia: nada depende de que un solo nodo esté siempre disponible."*

Al final, revivilo:

```bash
docker start spark-worker-1
```

---

## 3. Cierre de la demo (frase sugerida)

*"Lo que acabamos de ver — leer datos directo de HDFS, repartir el procesamiento entre Workers, y sobre todo la diferencia entre recalcular y cachear en memoria — es la razón concreta por la que Spark reemplazó a MapReduce como motor de procesamiento en la mayoría de los casos de uso analíticos. La partición y la replicación que vimos en HDFS le siguen dando el storage confiable a Spark; lo que cambió es que ahora el procesamiento no tiene por qué pasar por disco en cada paso."*

---

## 4. Troubleshooting rápido

- **Los Workers no aparecen en `http://localhost:8080`**: revisá que la red `hdfs-demo_default` exista (`docker network ls`) y que el nombre en el `docker-compose.yml` de Spark coincida exactamente. Si no coincide, los contenedores de Spark no van a poder resolver `namenode`.
- **`hdfs://namenode:9000/...` da error de conexión**: confirmá que el cluster HDFS de la demo anterior siga corriendo (`docker ps`) y que el archivo esté efectivamente en esa ruta (`docker exec -it namenode hdfs dfs -ls /demo`).
- **Los tiempos del paso 4 no muestran una diferencia clara**: el dataset puede ser chico para esta VM, o el `time.sleep` muy corto. Subí el tamaño del archivo de prueba (demo de HDFS) o aumentá el `time.sleep` por línea para que la diferencia sea más visible en pantalla.
- **Poca memoria / contenedores se caen**: bajá `SPARK_WORKER_MEMORY` a algo más chico si hace falta, o corré con un solo Worker en vez de dos (igual sirve para mostrar cache vs. no-cache, solo perdés el paralelismo visual entre dos nodos).
- **Querés limpiar todo al final**: `docker-compose down` en `spark-demo/` (y `docker-compose down -v` en `hdfs-demo/` si también querés borrar los datos de HDFS).
