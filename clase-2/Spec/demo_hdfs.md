# Demo de HDFS para la clase
### Cluster con 1 NameNode + 3 DataNodes en Docker, con guion de demo en vivo

---

## 1. Preparación (antes de la clase)

### Requisitos
- Docker y Docker Compose instalados en la máquina donde vas a hacer la demo.
- Un archivo de prueba de un tamaño razonable para ver la partición en bloques (por ejemplo, un CSV de 300-500MB, así se corte en 3-4 bloques con el tamaño default de 128MB). Si no tenés uno a mano, más abajo hay un comando para generarlo.

### `docker-compose.yml`

Creá una carpeta para la demo (ej. `hdfs-demo/`) y dentro un archivo `docker-compose.yml` con este contenido:

```yaml
version: "3"

services:
  namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    container_name: namenode
    restart: always
    ports:
      - "9870:9870"   # UI web del NameNode
      - "9000:9000"
    volumes:
      - hadoop_namenode:/hadoop/dfs/name
    environment:
      - CLUSTER_NAME=demo-cluster
    env_file:
      - ./hadoop.env

  datanode1:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode1
    restart: always
    volumes:
      - hadoop_datanode1:/hadoop/dfs/data
    environment:
      SERVICE_PRECONDITION: "namenode:9870"
    env_file:
      - ./hadoop.env
    depends_on:
      - namenode

  datanode2:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode2
    restart: always
    volumes:
      - hadoop_datanode2:/hadoop/dfs/data
    environment:
      SERVICE_PRECONDITION: "namenode:9870"
    env_file:
      - ./hadoop.env
    depends_on:
      - namenode

  datanode3:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode3
    restart: always
    volumes:
      - hadoop_datanode3:/hadoop/dfs/data
    environment:
      SERVICE_PRECONDITION: "namenode:9870"
    env_file:
      - ./hadoop.env
    depends_on:
      - namenode

volumes:
  hadoop_namenode:
  hadoop_datanode1:
  hadoop_datanode2:
  hadoop_datanode3:
```

### `hadoop.env`

En la misma carpeta, creá el archivo `hadoop.env` (variables de configuración que usa la imagen bde2020):

```
CORE_CONF_fs_defaultFS=hdfs://namenode:9000
CORE_CONF_hadoop_http_staticuser_user=root
HDFS_CONF_dfs_webhdfs_enabled=true
HDFS_CONF_dfs_permissions_enabled=false
HDFS_CONF_dfs_replication=3
```

### Levantar el cluster

```bash
cd hdfs-demo
docker-compose up -d
```

Esperá unos 20-30 segundos a que todos los contenedores terminen de iniciar. Verificá que estén corriendo:

```bash
docker ps
```

Deberías ver `namenode`, `datanode1`, `datanode2` y `datanode3` en estado `Up`.

### Verificar la UI web (opcional pero recomendado para la demo)

Abrí en el navegador: `http://localhost:9870` — vas a ver el dashboard del NameNode, con la lista de DataNodes activos (Datanodes tab), capacidad, y bloques. Esto es muy visual para mostrarle a los alumnos en pantalla durante toda la demo.

---

## 2. Generar o preparar el archivo de prueba

Si no tenés un dataset a mano, generá uno de ~400MB con datos aleatorios (para que se corte en varios bloques de 128MB):

```bash
# En tu máquina (no dentro del contenedor)
base64 /dev/urandom | head -c 400000000 > dataset_demo.txt
```

O, si preferís algo más "real" para mostrar en clase, cualquier CSV grande que tengas del curso sirve igual.

---

## 3. Guion de la demo en vivo

### Paso 1 — Subir el archivo a HDFS

Copiá el archivo al contenedor del namenode y subilo a HDFS:

```bash
docker cp dataset_demo.txt namenode:/dataset_demo.txt

docker exec -it namenode hdfs dfs -mkdir -p /demo
docker exec -it namenode hdfs dfs -put /dataset_demo.txt /demo/dataset_demo.txt
```

### Paso 2 — Mostrar que el archivo está ahí

```bash
docker exec -it namenode hdfs dfs -ls -h /demo
```

Esto muestra el tamaño del archivo. Podés comentar: *"HDFS ya decidió automáticamente en cuántos bloques partirlo."*

### Paso 3 — Mostrar los bloques y su ubicación (el corazón de la demo)

```bash
docker exec -it namenode hdfs fsck /demo/dataset_demo.txt -files -blocks -locations
```

Esta salida es la más importante para la clase: muestra cuántos bloques tiene el archivo, y en qué DataNodes vive cada réplica de cada bloque (vas a ver 3 IPs por bloque, porque el factor de replicación es 3). Señalá explícitamente: *"Miren, cada bloque no está en un solo lugar, está en 3 DataNodes distintos — eso es la replicación que vimos en la teoría, en vivo."*

También podés mostrarlo visualmente en la UI web: entrá a `http://localhost:9870`, andá a la pestaña "Utilities → Browse the file system", navegá a `/demo/dataset_demo.txt`, y hacé clic para ver el detalle de bloques — la interfaz gráfica muestra lo mismo que el comando, más fácil de leer para los alumnos.

### Paso 4 — La demo estrella: matar un DataNode y mostrar que el archivo se sigue leyendo

Primero, confirmá que podés leer el archivo normalmente:

```bash
docker exec -it namenode hdfs dfs -cat /demo/dataset_demo.txt | wc -c
```

Ahora, matá uno de los DataNodes (simulando una falla real de hardware):

```bash
docker stop datanode1
```

Esperá unos segundos, y volvé a leer el archivo:

```bash
docker exec -it namenode hdfs dfs -cat /demo/dataset_demo.txt | wc -c
```

**El archivo se sigue leyendo perfectamente**, aunque le falta un nodo — porque las réplicas en `datanode2` y `datanode3` siguen disponibles. Este es el momento clave de la demo: *"Acabamos de perder un tercio de la capacidad de storage del cluster, y la aplicación ni se enteró."*

### Paso 5 — Mostrar la detección de la falla y el re-balanceo

Volvé a correr el `fsck` para mostrar que el NameNode ya detectó que faltan réplicas:

```bash
docker exec -it namenode hdfs fsck /demo/dataset_demo.txt -files -blocks -locations
```

Vas a ver que ahora cada bloque solo tiene 2 ubicaciones en vez de 3 (under-replicated). Podés comentar que en un cluster real, HDFS automáticamente empezaría a copiar esos bloques a otro DataNode sano para volver a tener 3 réplicas — acá no lo vas a ver completarse porque no hay un cuarto nodo disponible, pero el mensaje de "under replicated blocks" ya demuestra el mecanismo.

### Paso 6 — Revivir el nodo (cierre limpio)

```bash
docker start datanode1
```

Esperá unos segundos y volvé a correr el `fsck` — deberías ver que vuelve a mostrar 3 ubicaciones por bloque, porque el DataNode se reincorporó y sincronizó sus datos.

### Paso 7 (opcional) — Mostrar escalabilidad agregando un nodo nuevo

Si querés cerrar también el punto de escalabilidad, podés agregar un cuarto DataNode al `docker-compose.yml` (copiando el bloque de `datanode3` y renombrándolo `datanode4`), levantarlo con:

```bash
docker-compose up -d datanode4
```

Y mostrar en la UI web (`http://localhost:9870` → Datanodes) cómo aparece automáticamente como parte del cluster, sin ninguna reconfiguración manual del NameNode.

---

## 4. Cierre de la demo (frase sugerida)

*"Lo que acaban de ver — un archivo partido en bloques, cada bloque replicado en 3 nodos distintos, y el sistema siguiendo funcionando aunque perdimos un nodo — es exactamente la teoría de partición y replicación que vimos, funcionando en un cluster real. Esto es lo que hace posible, por debajo, que herramientas como Spark puedan procesar petabytes de datos de forma confiable."*

---

## 5. Troubleshooting rápido

- **Los contenedores no arrancan / error de memoria**: Hadoop necesita bastante RAM. Asegurate de tener al menos 4-6GB libres asignados a Docker (Docker Desktop → Settings → Resources).
- **`docker exec` da error de "namenode not running"**: esperá más tiempo después del `docker-compose up -d`, o revisá logs con `docker logs namenode`.
- **El `fsck` no muestra 3 ubicaciones**: puede que el archivo se haya subido antes de que los 3 DataNodes terminaran de registrarse. Volvé a subir el archivo o esperá y corré el `fsck` de nuevo.
- **Querés limpiar todo al final**: `docker-compose down -v` (el `-v` borra también los volúmenes, para empezar de cero la próxima vez).
