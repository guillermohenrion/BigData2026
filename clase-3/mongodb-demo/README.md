# Demo MongoDB — base de documentos con replica set de 3 nodos, en Docker

Demo en vivo de **MongoDB**: documentos JSON con esquema flexible, consultas sobre campos
anidados y arrays, modelado embebiendo, índices (`COLLSCAN` vs. `IXSCAN`), aggregation pipeline
sobre 500.000 ventas y un **replica set** donde se cae el PRIMARY y se elige otro solo.
Incluye **Mongo Express** para ver las colecciones en una UI web.

```
mongodb-demo/
├─ docker-compose.yml      # mongo1..3 (replica set rs0) + mongo-express (8083)
└─ scripts/
   ├─ demo.sh              # guion paso a paso, con pausas
   └─ reset.sh             # docker compose down -v
```

## Requisitos

- Docker Desktop (~600 MB entre los 3 nodos y Mongo Express).
- Git Bash (Windows) o terminal (Mac/Linux).

## Antes de la clase

```bash
cd mongodb-demo
docker compose pull          # mongo:8.0 + mongo-express:1.0.2
AUTO=1 ./scripts/demo.sh     # ensayo completo sin pausas (~1,5 min)
```

## Durante la clase

```bash
./scripts/demo.sh           # todo el guion, Enter entre pasos
./scripts/demo.sh 5         # solo un paso
./scripts/demo.sh 2 6       # un rango de pasos
VENTAS=2000000 ./scripts/demo.sh 4   # más ventas (default 500.000)
```

El paso 0 hace falta la primera vez: levanta los 3 nodos e inicia el replica set
(`rs.initiate`). Sin replica set iniciado, Mongo Express se queda reintentando.

**Mongo Express:** <http://localhost:8083>, base `tienda` (el 8081 lo usa el Worker de Spark de
la clase 2).

| Paso | Qué se muestra | Resultado de referencia |
|---|---|---|
| 0 | 3 nodos + `rs.initiate`: un PRIMARY y dos SECONDARY | |
| 1 | `insertOne` / `insertMany` con documentos de distinta forma, objetos anidados y arrays | sin `CREATE TABLE` |
| 2 | `find` con `$lt`, `"specs.ram_gb"`, búsqueda dentro de un array, `$exists` | |
| 3 | Un pedido con el cliente y los items **embebidos**; `$push` y `$set` | lo que en SQL serían 3 tablas + JOIN |
| 4 | 500.000 ventas con `mongoimport` | ~7 s |
| 5 | `explain("executionStats")` antes y después de `createIndex` | `COLLSCAN` 500.000 docs, ~130 ms → `IXSCAN` 7.849 docs, ~9 ms |
| 6 | **Aggregation pipeline**: `$match` → `$group` → `$sort` | mismos números que Elasticsearch |
| 7 | `docker stop` del PRIMARY → elección → se sigue escribiendo → vuelve como SECONDARY y se pone al día | nuevo PRIMARY en ~5 s |

### Puntos para comentar en clase

- **Paso 6, mismos números en tres motores:** las ventas son las mismas filas que las primeras
  500.000 de `clase-2/hdfs-demo/dataset_demo.csv` (mismo generador y semilla) y que el índice
  `ventas` de `elasticsearch-demo`. La facturación por sucursal da exactamente lo mismo
  (Córdoba $48.078.422.782): cambia el motor, no los datos.
- **Paso 7 vs. HDFS y Elasticsearch:** los tres replican, pero distinto. HDFS replica bloques y
  cualquier réplica sirve para leer; Elasticsearch replica shards; MongoDB tiene **un solo nodo
  que acepta escrituras** (PRIMARY) y, si se cae, los demás votan uno nuevo por mayoría. Con 3
  nodos se tolera perder 1; con 2 nodos no habría mayoría y nadie podría escribir.
- **Elección rápida:** `electionTimeoutMillis` está en 5 s (el default es 10 s) para que se vea bien en clase.

## Tipear a mano

```bash
docker exec -it mongo1 mongosh "mongodb://mongo1,mongo2,mongo3/tienda?replicaSet=rs0"
```

```javascript
show collections
db.productos.find().pretty()
db.productos.find({ precio: { $gte: 100000, $lte: 700000 } }, { nombre: 1, precio: 1, _id: 0 })
db.productos.updateMany({ categoria: "Notebook" }, { $inc: { precio: 50000 } })
db.ventas.getIndexes()
rs.status().members.map(m => [m.name, m.stateStr])

// Ventas por mes de un producto
db.ventas.aggregate([
  { $match: { producto: "Celular" } },
  { $group: { _id: { $month: "$fecha" }, unidades: { $sum: "$cantidad" } } },
  { $sort: { _id: 1 } }
])

// $unwind: un documento por item del pedido
db.pedidos.aggregate([ { $unwind: "$items" }, { $project: { _id: 0, "items.producto": 1, "items.cantidad": 1 } } ])
```

**MongoDB Compass** (cliente gráfico) desde tu máquina:
`mongodb://localhost:27017/?directConnection=true`. Solo `mongo1` publica el puerto, y los
nombres `mongo2`/`mongo3` no resuelven fuera de Docker; por eso `directConnection`. Si `mongo1`
no es el PRIMARY, Compass va a mostrar los datos en modo lectura.

## Desde Python

Con el replica set la URI completa solo funciona dentro de la red de Docker. Desde tu máquina,
conectate directo a `mongo1`:

```python
from pymongo import MongoClient                 # pip install pymongo
db = MongoClient("mongodb://localhost:27017/?directConnection=true")["tienda"]

db.productos.find_one({"categoria": "Celular"})
list(db.ventas.aggregate([
    {"$group": {"_id": "$sucursal", "facturado": {"$sum": "$total"}}},
    {"$sort": {"facturado": -1}},
]))
```

## Troubleshooting

- **`not primary` al escribir con `directConnection`**: `mongo1` no es el PRIMARY en este momento
  (por ejemplo después del paso 7). Usá la URI con los 3 nodos desde `docker exec`, o
  `rs.stepDown()` en el PRIMARY actual hasta que `mongo1` vuelva a serlo.
- **Mongo Express muestra error o se reinicia**: el replica set no está iniciado; corré `./scripts/demo.sh 0`.
- **`port is already allocated` (27017)**: hay otro MongoDB en tu máquina. Cambiá el puerto del host (`"27018:27017"`).
- **`the input device is not a TTY`** con `docker exec -it`: usá la terminal de VS Code o
  Windows Terminal, o anteponé `winpty`.
- **Limpiar todo**: `./scripts/reset.sh` (borra los datos y la configuración del replica set; el
  paso 0 lo vuelve a iniciar).
