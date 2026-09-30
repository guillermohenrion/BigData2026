# Demo HDFS — 1 NameNode + 3 DataNodes en Docker

Demo en vivo de **partición en bloques** y **replicación** en HDFS: se sube un archivo,
se ve en qué DataNodes vive cada réplica, se mata un nodo y el archivo se sigue leyendo.

Guion original: [`../Spec/demo_hdfs.md`](../Spec/demo_hdfs.md).

```
hdfs-demo/
├─ docker-compose.yml      # namenode + datanode1..3 (+ datanode4 con --profile scale)
├─ hadoop.env              # dfs.replication=3 + timeouts cortos para la demo
└─ scripts/
   ├─ gen_dataset.sh       # genera dataset_demo.csv (default 400 MB)
   ├─ demo.sh              # guion paso a paso, con pausas
   ├─ consulta.sh          # consulta antes/después de apagar un nodo, mostrando qué nodo sirve cada bloque
   └─ reset.sh             # docker compose down -v
```

## Requisitos

- Docker Desktop con **4-6 GB de RAM** asignados (Settings → Resources).
- Git Bash (Windows) o terminal (Mac/Linux).

## Antes de la clase

```bash
cd hdfs-demo
./scripts/gen_dataset.sh 400     # ~400 MB -> 3 bloques de 128 MB (500 -> 4)
docker compose pull              # baja las imágenes (~1 GB) con buena conexión
```

## Durante la clase

```bash
./scripts/demo.sh           # todo el guion, Enter entre pasos
./scripts/demo.sh 4         # solo un paso
./scripts/demo.sh 3 6       # un rango de pasos
FULL=1 ./scripts/demo.sh 3  # fsck con la salida completa (sin resumir)
AUTO=1 ./scripts/demo.sh    # sin pausas, para ensayar
```

UI web del NameNode: <http://localhost:9870> (pestañas *Datanodes* y *Utilities → Browse the file system*).

| Paso | Qué se muestra | Comando clave (los `hdfs ...` van [dentro del namenode](#correr-comandos-hdfs-a-mano)) |
|---|---|---|
| 0 | Levantar el cluster y esperar 3 DataNodes | `docker compose up -d` |
| 1 | Subir el archivo | `hdfs dfs -put` |
| 2 | El archivo está en HDFS | `hdfs dfs -ls -h /demo` |
| 3 | **Bloques y réplicas por nodo** | `hdfs fsck ... -files -blocks -locations` |
| 4 | **Matar `datanode1` y seguir leyendo** | `docker stop datanode1` |
| 5 | El NameNode detecta la falla: *under-replicated* | `hdfs dfsadmin -report` + `fsck` |
| 6 | Revivir el nodo: vuelven las 3 réplicas | `docker start datanode1` |
| 7 | (opcional) Escalar con `datanode4` | `docker compose --profile scale up -d datanode4` |

## Correr comandos `hdfs` a mano

`hdfs` **no está instalado en tu máquina**, solo dentro de los contenedores. Si tipeás
`hdfs dfs -ls /demo` en Git Bash vas a ver `bash: hdfs: command not found`. Los comandos
`hdfs ...` de la tabla de arriba se corren de alguna de estas formas:

**a) Entrando al contenedor** (la más cómoda para tipear en vivo):

```bash
docker exec -it namenode bash
hdfs dfs -ls -h /demo
hdfs dfs -cat /demo/dataset_demo.csv | head -5
hdfs fsck /demo/dataset_demo.csv -files -blocks -locations
hdfs dfsadmin -report
exit
```

**b) Desde Git Bash, anteponiendo `docker exec namenode`:**

```bash
export MSYS_NO_PATHCONV=1      # si no, Git Bash cambia "/demo" por "C:/Program Files/Git/demo"
docker exec namenode hdfs dfs -ls -h /demo
```

**c) Con un atajo en Git Bash**, para que `hdfs` funcione directo (vale solo para esa terminal;
para dejarlo fijo, agregá estas dos líneas al final de `~/.bashrc`):

```bash
export MSYS_NO_PATHCONV=1
hdfs() { docker exec -i namenode hdfs "$@"; }
```

Los comandos `docker ...` (`docker stop datanode1`, `docker compose ...`) sí se corren en
Git Bash, **fuera** del contenedor.

El paso 3 muestra el `fsck` resumido, con nombres de nodo en vez de IPs:

```
  bloque 0  blk_1073741825  134217728 bytes  réplicas=3  → datanode2, datanode3, datanode1
  bloque 1  blk_1073741826  134217728 bytes  réplicas=3  → datanode3, datanode2, datanode1
  bloque 2  blk_1073741827  131564550 bytes  réplicas=3  → datanode2, datanode3, datanode1
```

## Consulta con failover visible

```bash
./scripts/consulta.sh                              # apaga el nodo que sirvió el bloque 0
./scripts/consulta.sh datanode2                    # o el que elijas
SUCURSAL=Salta PRODUCTO=Celular ./scripts/consulta.sh
```

El dataset es un CSV de ventas (`fecha,sucursal,producto,cantidad,precio_unitario,total`,
~9 millones de filas). La consulta responde *"¿cuántas notebooks se vendieron en Rosario y
cuánto se facturó?"* recorriendo el archivo entero, y con los logs DEBUG del cliente HDFS
muestra de qué DataNode se leyó cada bloque. Después apaga un nodo, repite la consulta y lo
vuelve a prender:

```
════ 1 — Consulta con el cluster sano ════
  bloque 0  → datanode3
  bloque 1  → datanode2
  bloque 2  → datanode3
  Notebook en Rosario: 141602 ventas, 424438 unidades, $360774174285 facturados

════ 3 — Repetimos la misma consulta ════        (datanode3 apagado)
  bloque 0  → datanode2
  bloque 1  → datanode1
  bloque 2  ✘ datanode3 no responde, pruebo otra réplica
  bloque 2  → datanode2
  Notebook en Rosario: 141602 ventas, 424438 unidades, $360774174285 facturados
```

HDFS elige al azar cuál de las 3 réplicas leer, así que los nodos cambian entre corridas;
lo que no cambia es el resultado.

**Bonus después del paso 7:** con 4 nodos, `docker stop datanode2`, esperá ~1 min y corré
`./scripts/demo.sh 3`: los bloques se re-replican solos a `datanode4` (vuelven a `réplicas=3`).

## Diferencias con el guion original

- **Timeouts cortos (`hadoop.env`)**: por defecto el NameNode tarda **~10,5 min** en declarar
  muerto a un DataNode, así que el paso 5 no mostraría nada "a los pocos segundos". Con
  `dfs.namenode.heartbeat.recheck-interval=10s` baja a **~50 s** (el script espera 60 s).
- **`MSYS_NO_PATHCONV=1`**: en Git Bash, `docker exec namenode hdfs dfs -ls /demo` se
  convierte en `C:/Program Files/Git/demo`. Si tipeás comandos a mano, exportá esa variable primero.
- **Sin `-it` en los `docker exec` con pipe** (`... | wc -c`): en Git Bash, `-t` falla con
  *"the input device is not a TTY"*.
- **`MULTIHOMED_NETWORK=0`**: por defecto la imagen bde2020 hace que el cliente contacte a los
  DataNodes por hostname. Con un contenedor apagado ese nombre deja de resolver y la lectura
  **se corta** (`UnresolvedAddressException`) en vez de pasar a otra réplica. Por IP, el
  cliente recibe *connection refused* y hace failover. Si cambiás `hadoop.env`, aplicalo con
  `docker compose up -d --force-recreate` (los datos quedan en los volúmenes).
- **Espera de safe mode** en el paso 0: tras reiniciar, el NameNode queda en solo lectura
  unos segundos y el `-put` fallaría.
- **Dataset CSV de ventas** en vez de texto aleatorio, para poder hacer consultas con sentido.
- `datanode4` ya está definido en el compose con el perfil `scale`: no hace falta editar el archivo en vivo.
- Sin `version: "3"` (Compose v2 lo marca como obsoleto).

## Troubleshooting

- **No arrancan / error de memoria**: subí la RAM de Docker a 4-6 GB.
- **"namenode not running"**: `docker logs namenode`; el script ya espera a que haya 3 DataNodes vivos.
- **El fsck no muestra 3 réplicas**: el archivo se subió antes de que se registraran los 3 nodos. `./scripts/demo.sh 1 3`.
- **Limpiar todo**: `./scripts/reset.sh` (borra los volúmenes; la próxima vez arranca con HDFS vacío).
