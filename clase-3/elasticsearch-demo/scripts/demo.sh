#!/usr/bin/env bash
# Guion de la demo Elasticsearch en vivo.
#   ./scripts/demo.sh          -> corre todos los pasos, pausando entre cada uno
#   ./scripts/demo.sh 3        -> corre solo el paso 3
#   ./scripts/demo.sh 2 5      -> corre del paso 2 al 5
#   AUTO=1 ./scripts/demo.sh   -> sin pausas (para ensayar)
#   VENTAS=1000000 ./scripts/demo.sh 6   -> carga más ventas (default 500.000)
set -uo pipefail

cd "$(dirname "$0")/.."
ES="http://localhost:9200"
VENTAS="${VENTAS:-500000}"

BOLD=$'\e[1m'; CYAN=$'\e[36m'; GREEN=$'\e[32m'; YELLOW=$'\e[33m'; RED=$'\e[31m'; RESET=$'\e[0m'

title() { echo; echo "${BOLD}${CYAN}════ $* ════${RESET}"; }
say()   { echo "${YELLOW}💬 $*${RESET}"; }
run()   { echo "${GREEN}\$ $*${RESET}"; "$@"; }
pause() { [[ "${AUTO:-0}" == "1" ]] && return; read -rp $'\n[Enter para continuar] ' _; }

# Python para resumir el JSON; sin Python se muestra el JSON tal cual.
PY=""
for p in python python3; do
  "$p" -c "import json" >/dev/null 2>&1 && { PY="$p"; break; }
done
fmt() { if [[ -n "$PY" ]]; then "$PY" scripts/fmt.py "$1"; else cat; echo; fi; }

# es METODO RUTA [BODY] [MODO]: muestra la request como en Kibana Dev Tools y la ejecuta.
es() {
  local method="$1" path="$2" body="${3:-}" mode="${4:-raw}"
  echo "${GREEN}${method} ${path}${RESET}"
  [[ -n "$body" ]] && echo "${GREEN}${body}${RESET}"
  if [[ -n "$body" ]]; then
    # El cuerpo va por stdin: como argumento, Git Bash le pasa los acentos a curl.exe
    # en la página de códigos de Windows en vez de UTF-8 y Elasticsearch lo rechaza.
    printf '%s' "$body" | curl -s -X "$method" "${ES}${path}" -H 'Content-Type: application/json' --data-binary @-
  else
    curl -s -X "$method" "${ES}${path}"
  fi | fmt "$mode"
}

# Versión silenciosa, para preparar cosas sin ensuciar la pantalla.
es_q() { curl -s -o /dev/null -X "$1" "${ES}$2" -H 'Content-Type: application/json' ${3:+-d "$3"}; }

wait_es() {
  echo "  esperando a Elasticsearch (tarda ~30 s la primera vez)..."
  for _ in $(seq 1 60); do
    curl -s "$ES/_cluster/health" | grep -qE '"status":"(green|yellow)"' && { echo "  ✔ Elasticsearch responde"; return 0; }
    sleep 3
  done
  echo "  ${RED}✘ Elasticsearch no responde (docker logs elasticsearch)${RESET}"; return 1
}

step0() {
  title "Paso 0 — Levantar Elasticsearch y Kibana"
  run docker compose up -d
  wait_es
  es GET "/?filter_path=cluster_name,version.number,tagline"
  [[ -z "$PY" ]] && say "(sin Python en el PATH: se ve el JSON completo en vez del resumen)"
  say "API REST en http://localhost:9200 — Kibana en http://localhost:5601 → Dev Tools (tarda ~1 min más)."
}

step1() {
  title "Paso 1 — Crear un índice y cargar documentos JSON"
  es_q DELETE /productos
  es PUT /productos '{
  "mappings": {
    "properties": {
      "nombre":      { "type": "text", "analyzer": "spanish" },
      "descripcion": { "type": "text", "analyzer": "spanish" },
      "categoria":   { "type": "keyword" },
      "marca":       { "type": "keyword" },
      "precio":      { "type": "integer" }
    }
  }
}'
  echo
  say "text = se analiza para buscar por palabras.  keyword = valor exacto, para filtrar y agrupar."
  echo "${GREEN}POST /productos/_bulk   (data/productos.ndjson: 30 productos)${RESET}"
  curl -s -X POST "$ES/productos/_bulk?refresh=true" -H 'Content-Type: application/x-ndjson' \
       --data-binary @data/productos.ndjson | grep -o '"errors":[a-z]*' | sed 's/^/  /'
  es GET /productos/_count
  echo
  es GET /productos/_doc/13
  say "Un documento es un JSON. No hay tablas ni JOINs: cada producto se guarda completo."
}

step2() {
  title "Paso 2 — El índice invertido: ¿qué guarda Elasticsearch de cada texto?"
  local frase="Las notebooks más livianas para viajar"
  es POST /_analyze "{ \"analyzer\": \"standard\", \"text\": \"$frase\" }" tokens
  es POST /_analyze "{ \"analyzer\": \"spanish\",  \"text\": \"$frase\" }" tokens
  say "El analizador 'spanish' saca palabras vacías (las, más, para) y reduce cada palabra a su raíz."
  es POST /_analyze '{ "analyzer": "spanish", "text": "liviana livianos ultraliviana viajar viaje notebook notebooks" }' tokens
  say "liviana y livianos → 'livian': matchean entre sí. Pero el stemmer no es magia:"
  say "'viajar' ≠ 'viaj', 'notebook' ≠ 'notebooks' y 'ultraliviana' es otro término. Se busca por términos exactos."
  say "Para cada término se guarda la lista de documentos que lo contienen: término → [docs]."
  say "Buscar es leer esas listas, no recorrer todos los textos. Por eso responde en milisegundos."
}

step3() {
  title "Paso 3 — Búsqueda full-text con relevancia (_score)"
  es GET "/productos/_search" '{
  "query": {
    "multi_match": {
      "query":  "notebook liviana para viajar",
      "fields": ["nombre^2", "descripcion"]
    }
  },
  "size": 6
}' hits
  say "No es un LIKE de SQL: devuelve TODO lo que tiene alguna palabra, ordenado por relevancia."
  say "Pesa más un término raro que uno común (BM25) y ^2 le da el doble de peso al nombre."
  say "La búsqueda se analiza igual que los documentos: [notebook] [livian] [viajar]."
  say "'notebook' está en muchos documentos → pesa poco. 'livian' es raro → pesa mucho."
  say "Por eso ganan el monitor portátil y el mouse de viaje (tienen 'livian' y 'viajar'), y la"
  say "'Ultraliviana' no sale primera: dice 'ultraliviana', que para el índice es otro término (paso 2)."
}

step4() {
  title "Paso 4 — Tolerancia a errores de tipeo (fuzzy) y resaltado"
  es GET "/productos/_search" '{
  "query": {
    "match": {
      "descripcion": { "query": "auriculares inalanbricos con cancelasion de ruido", "fuzziness": "AUTO" }
    }
  },
  "highlight": { "fields": { "descripcion": {} } },
  "size": 4
}' hits
  say "'inalanbricos' y 'cancelasion' están mal escritos y los encuentra igual:"
  say "fuzziness acepta términos a 1-2 letras de distancia. Lo amarillo es lo que matcheó."
}

step5() {
  title "Paso 5 — Búsqueda + filtros + facetas (como un e-commerce)"
  es GET "/productos/_search" '{
  "query": {
    "bool": {
      "must":   { "match": { "descripcion": "gamer" } },
      "filter": { "range": { "precio": { "lte": 200000 } } }
    }
  },
  "aggs": { "por_categoria": { "terms": { "field": "categoria" } } },
  "size": 5
}' hits
  es GET "/productos/_search?size=0" '{
  "query": { "match": { "descripcion": "inalámbrico" } },
  "aggs":  { "por_categoria": { "terms": { "field": "categoria" } },
             "por_marca":     { "terms": { "field": "marca" } } }
}' aggs
  say "must = afecta la relevancia. filter = sí/no, no cambia el _score y se cachea."
  say "Las agregaciones son las facetas de la izquierda de cualquier tienda online: 'Auriculares (2) · Mouse (2) · Teclado (1)'."
}

step6() {
  title "Paso 6 — Escala: ${VENTAS} ventas en un índice con 3 shards"
  es_q DELETE /ventas
  es PUT /ventas '{
  "settings": { "number_of_shards": 3, "number_of_replicas": 1 },
  "mappings": {
    "properties": {
      "fecha":    { "type": "date" },
      "sucursal": { "type": "keyword" },
      "producto": { "type": "keyword" },
      "cantidad": { "type": "integer" },
      "precio_unitario": { "type": "long" },
      "total":    { "type": "long" }
    }
  }
}'
  echo
  local tmp; tmp=$(mktemp -d)
  echo "  generando ${VENTAS} ventas (mismo generador y semilla que hdfs-demo/scripts/gen_dataset.sh)..."
  # Mismas filas que las primeras VENTAS líneas de dataset_demo.csv de la clase 2, en formato _bulk.
  awk -v n="$VENTAS" 'BEGIN {
    srand(42)
    ns = split("Buenos Aires,Cordoba,Rosario,Mendoza,La Plata,Mar del Plata,Tucuman,Salta", suc, ",")
    np = split("Notebook,Celular,Tablet,Monitor,Teclado,Mouse,Auriculares,Impresora", prod, ",")
    split("850000,420000,310000,190000,35000,18000,45000,160000", precio, ",")
    for (i = 0; i < n; i++) {
      p = int(rand() * np) + 1
      cant = int(rand() * 5) + 1
      unit = int(precio[p] * (0.9 + rand() * 0.2))
      printf "{\"index\":{}}\n{\"fecha\":\"2026-%02d-%02d\",", int(rand() * 12) + 1, int(rand() * 28) + 1
      printf "\"sucursal\":\"%s\",\"producto\":\"%s\",\"cantidad\":%d,\"precio_unitario\":%d,\"total\":%d}\n",
             suc[int(rand() * ns) + 1], prod[p], cant, unit, cant * unit
    }
  }' | split -l 100000 - "$tmp/bulk_"
  local inicio=$SECONDS
  for f in "$tmp"/bulk_*; do
    curl -s -X POST "$ES/ventas/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @"$f" \
      | grep -q '"errors":false' && printf "." || printf "${RED}✘${RESET}"
  done
  echo " cargadas en $(( SECONDS - inicio )) s"
  rm -rf "$tmp"
  es_q POST /ventas/_refresh
  es GET /ventas/_count
  echo
  echo "${GREEN}GET /_cat/shards/ventas?v${RESET}"
  curl -s "$ES/_cat/shards/ventas?v&h=index,shard,prirep,state,docs,node"
  es GET "/_cluster/health/ventas?filter_path=status,active_primary_shards,unassigned_shards"
  say "El índice se partió en 3 shards (p = primario), como los bloques de HDFS."
  say "Cada shard pidió una réplica (r) pero quedó UNASSIGNED: nunca se pone la réplica en el mismo nodo"
  say "que el primario. Con 1 solo nodo el cluster queda YELLOW: anda, pero sin tolerancia a fallas."
}

step7() {
  title "Paso 7 — Agregaciones: el GROUP BY de Elasticsearch"
  es GET "/ventas/_search?size=0&track_total_hits=true" '{
  "aggs": {
    "por_sucursal": {
      "terms": { "field": "sucursal", "order": { "facturado": "desc" }, "size": 8 },
      "aggs":  { "facturado": { "sum": { "field": "total" } } }
    }
  }
}' aggs
  es GET "/ventas/_search?size=0&track_total_hits=true" '{
  "query": { "bool": { "filter": [
    { "term":  { "producto": "Notebook" } },
    { "range": { "fecha": { "gte": "2026-12-01", "lte": "2026-12-31" } } }
  ] } },
  "aggs": {
    "por_sucursal": {
      "terms": { "field": "sucursal", "size": 3, "order": { "facturado": "desc" } },
      "aggs":  { "facturado": { "sum": { "field": "total" } } }
    }
  }
}' aggs
  say "Notebooks vendidas en diciembre, top 3 sucursales: filtros + group by en milisegundos."
  say "Es lo que hace un dashboard de Kibana: cada gráfico es una agregación como esta."
}

step8() {
  title "Paso 8 — Kibana"
  say "http://localhost:5601 → menú ☰ → Management → Dev Tools: se pueden pegar las consultas de este guion"
  say "tal cual (las líneas verdes: método, ruta y cuerpo JSON)."
  say "Para Discover y gráficos: Stack Management → Data Views → Create → 'ventas', campo de tiempo 'fecha'."
}

closing() {
  title "Cierre"
  say "Elasticsearch es una base de documentos JSON hecha para BUSCAR: índice invertido, relevancia,"
  say "tolerancia a errores y agregaciones rápidas, distribuida en shards y réplicas."
  say "Se usa al lado de la base principal: buscadores de tiendas, logs y observabilidad (ELK), métricas."
}

FROM="${1:-0}"; TO="${2:-${1:-8}}"
for i in $(seq "$FROM" "$TO"); do
  "step$i"
  (( i < TO )) && pause
done
[[ "$TO" -ge 8 ]] && closing
exit 0
