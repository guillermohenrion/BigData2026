# Demo Elasticsearch — búsqueda full-text y agregaciones, en Docker

Demo en vivo de **Elasticsearch**: documentos JSON, índice invertido y analizadores, búsqueda
por relevancia, tolerancia a errores de tipeo, filtros + facetas, shards/réplicas y agregaciones
sobre 500.000 ventas. Incluye **Kibana** para tipear las mismas consultas en *Dev Tools*.

```
elasticsearch-demo/
├─ docker-compose.yml      # elasticsearch 9.1.5 (9200) + kibana 9.1.5 (5601), sin seguridad
├─ data/
│  └─ productos.ndjson     # 30 productos con descripciones en español, formato _bulk
└─ scripts/
   ├─ demo.sh              # guion paso a paso, con pausas
   ├─ fmt.py               # resume las respuestas JSON en pocas líneas (lo usa demo.sh)
   └─ reset.sh             # docker compose down -v
```

## Requisitos

- Docker Desktop con **~4 GB de RAM** libres (Elasticsearch ~2 GB, Kibana ~700 MB).
- Git Bash (Windows) o terminal (Mac/Linux).
- Python 3 en el PATH (solo para resumir las respuestas; sin Python se ve el JSON completo).

## Antes de la clase

```bash
cd elasticsearch-demo
docker compose pull          # ~2 GB entre Elasticsearch y Kibana
AUTO=1 ./scripts/demo.sh     # ensayo completo sin pausas (~1 min)
```

## Durante la clase

```bash
./scripts/demo.sh           # todo el guion, Enter entre pasos
./scripts/demo.sh 3         # solo un paso
./scripts/demo.sh 2 5       # un rango de pasos
VENTAS=2000000 ./scripts/demo.sh 6   # más ventas (default 500.000)
```

API: <http://localhost:9200> · Kibana: <http://localhost:5601> → ☰ → *Management* → *Dev Tools*.

| Paso | Qué se muestra | Resultado de referencia |
|---|---|---|
| 0 | Levantar Elasticsearch + Kibana | versión 9.1.5, *"You Know, for Search"* |
| 1 | Crear el índice `productos` (mapping `text` vs. `keyword`) y cargar 30 documentos con `_bulk` | `count: 30` |
| 2 | **Índice invertido**: `_analyze` con el analizador `standard` vs. `spanish` | `livianas` → `livian`; `las`, `más`, `para` desaparecen |
| 3 | **Búsqueda por relevancia** (`multi_match`, BM25) | "notebook liviana para viajar" → el monitor portátil primero (ver abajo) |
| 4 | **Errores de tipeo** (`fuzziness`) y resaltado | "inalanbricos con cancelasion" encuentra los auriculares |
| 5 | `bool` = `must` + `filter`, y **facetas** con `terms` | "gamer" ≤ $200.000 → mouse y headset |
| 6 | 500.000 ventas en un índice de **3 shards + 1 réplica** | 3 primarios, 3 réplicas `UNASSIGNED`, cluster **yellow** |
| 7 | **Agregaciones** (el `GROUP BY`): facturación por sucursal | 500.000 docs en ~30 ms |
| 8 | Dónde ver todo en Kibana | |

### Puntos para comentar en clase

- **Paso 3, por qué no gana la "Notebook Ultraliviana":** la búsqueda se analiza igual que los
  documentos y queda `[notebook] [livian] [viajar]`. *notebook* aparece en muchos documentos, así
  que pesa poco; *livian* es raro y pesa mucho. El monitor portátil y el mouse de viaje tienen
  *livian* y *viajar*; la Ultraliviana dice *ultraliviana*, que para el índice es **otro término**
  (lo muestra el paso 2).
  Es un buen momento para explicar que la búsqueda trabaja con términos, no con "significado",
  y que para eso existen los sinónimos o la búsqueda semántica con embeddings.
- **Paso 6, continuidad con la clase 2:** las ventas son **las mismas filas** que las primeras
  500.000 de `dataset_demo.csv` (mismo generador y semilla). Los shards son la partición, como
  los bloques de HDFS; la réplica no se asigna porque Elasticsearch nunca pone una réplica en el
  mismo nodo que su primario. Con un solo nodo el cluster queda **yellow**.
- **Paso 7 vs. Spark:** Spark recorre el archivo entero en cada consulta; Elasticsearch responde
  en milisegundos porque los datos ya están indexados (estructuras columnares por campo). El
  precio es la carga: indexar es más caro que escribir un archivo en HDFS.

## Tipear a mano en Kibana (Dev Tools)

Las líneas verdes del guion se pegan tal cual. Algunas para explorar:

```
GET /_cat/indices?v
GET /productos/_mapping
GET /productos/_search
{ "query": { "match": { "descripcion": "ideal para viajar" } } }

GET /productos/_search
{ "query": { "match_phrase": { "descripcion": "cancelación activa de ruido" } } }

GET /ventas/_search?size=0
{
  "aggs": {
    "por_mes": {
      "date_histogram": { "field": "fecha", "calendar_interval": "month" },
      "aggs": { "facturado": { "sum": { "field": "total" } } }
    }
  }
}
```

Para gráficos: *Stack Management* → *Data Views* → *Create data view* → índice `ventas`, campo de
tiempo `fecha`. Después *Discover* o *Dashboards* → *Create visualization*. Elegí el rango
de fechas de 2026, que es el año de los datos.

## Desde la terminal (sin Kibana)

```bash
curl localhost:9200/_cat/indices?v
curl "localhost:9200/productos/_search?q=descripcion:viajar&pretty"
```

En Git Bash los cuerpos con acentos hay que mandarlos por stdin (`--data-binary @-`): pasados
como argumento (`-d '...más...'`) llegan en la página de códigos de Windows y Elasticsearch
responde *Invalid UTF-8*. `demo.sh` ya lo hace así.

## Troubleshooting

- **Elasticsearch se cae al arrancar / `exit code 137`**: le falta memoria a Docker. Subí la RAM
  en Docker Desktop o bajá el heap en el compose (`ES_JAVA_OPTS=-Xms512m -Xmx512m`).
- **`max virtual memory areas vm.max_map_count [65530] is too low`** (Linux, no Docker Desktop):
  `sudo sysctl -w vm.max_map_count=262144`.
- **Kibana dice *"Kibana server is not ready yet"***: tarda ~1 minuto más que Elasticsearch en arrancar.
- **Las tildes salen como `Ã¡`**: el `fmt.py` ya lee en UTF-8; si usás otro script, decodificá
  la respuesta como UTF-8.
- **Limpiar todo**: `./scripts/reset.sh` (borra los índices).
- Seguridad desactivada (`xpack.security.enabled=false`) para que la demo no pida usuario ni
  certificados. **No** dejarlo así fuera de una máquina local.
