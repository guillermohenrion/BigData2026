#!/usr/bin/env bash
# Guion de la demo MongoDB en vivo.
#   ./scripts/demo.sh          -> corre todos los pasos, pausando entre cada uno
#   ./scripts/demo.sh 5        -> corre solo el paso 5
#   ./scripts/demo.sh 2 6      -> corre del paso 2 al 6
#   AUTO=1 ./scripts/demo.sh   -> sin pausas (para ensayar)
#   VENTAS=1000000 ./scripts/demo.sh 4   -> carga más ventas (default 500.000)
set -uo pipefail

cd "$(dirname "$0")/.."
URI="mongodb://mongo1,mongo2,mongo3/tienda?replicaSet=rs0"
VENTAS="${VENTAS:-500000}"

BOLD=$'\e[1m'; CYAN=$'\e[36m'; GREEN=$'\e[32m'; YELLOW=$'\e[33m'; RED=$'\e[31m'; RESET=$'\e[0m'

title() { echo; echo "${BOLD}${CYAN}════ $* ════${RESET}"; }
say()   { echo "${YELLOW}💬 $*${RESET}"; }
run()   { echo "${GREEN}\$ $*${RESET}"; "$@"; }
pause() { [[ "${AUTO:-0}" == "1" ]] && return; read -rp $'\n[Enter para continuar] ' _; }

# Un nodo vivo desde donde correr mongosh (en el paso 7 apagamos uno).
nodo() {
  for n in mongo1 mongo2 mongo3; do
    docker ps --format '{{.Names}}' | grep -qx "$n" && { echo "$n"; return; }
  done
}

# m 'js'          -> muestra el comando como en mongosh y lo ejecuta contra el replica set
# m 'js' 'js2'    -> muestra 'js' pero ejecuta 'js2' (para resumir salidas largas)
m() {
  echo "${GREEN}tienda> $1${RESET}"
  docker exec "$(nodo)" mongosh --quiet "$URI" --eval "${2:-$1}" 2>&1 | sed 's/^/  /'
}
# Sin mostrar el comando (preparación).
m_q() { docker exec "$(nodo)" mongosh --quiet "$URI" --eval "$1" >/dev/null 2>&1; }

primario() {
  docker exec "$(nodo)" mongosh --quiet "$URI" --eval \
    'rs.status().members.filter(m => m.stateStr == "PRIMARY").map(m => m.name.split(":")[0]).join("")' 2>/dev/null
}

estado_rs() {
  echo "${GREEN}tienda> rs.status().members.map(m => [m.name, m.stateStr])${RESET}"
  docker exec "$(nodo)" mongosh --quiet --eval \
    'rs.status().members.forEach(m => print("  " + m.name.padEnd(14) + m.stateStr))' 2>/dev/null
}

step0() {
  title "Paso 0 — Levantar 3 nodos e iniciar el replica set"
  run docker compose up -d --wait mongo1 mongo2 mongo3
  if ! docker exec mongo1 mongosh --quiet --eval 'rs.status().ok' >/dev/null 2>&1; then
    echo "${GREEN}rs.initiate({ _id: 'rs0', members: [mongo1, mongo2, mongo3] })${RESET}"
    docker exec mongo1 mongosh --quiet --eval '
      rs.initiate({ _id: "rs0", settings: { electionTimeoutMillis: 5000 }, members: [
        { _id: 0, host: "mongo1:27017" },
        { _id: 1, host: "mongo2:27017" },
        { _id: 2, host: "mongo3:27017" } ] })' | sed 's/^/  /'
  fi
  echo "  esperando la elección del primer PRIMARY..."
  for _ in $(seq 1 30); do [[ -n "$(primario)" ]] && break; sleep 2; done
  estado_rs
  docker compose up -d mongo-express >/dev/null 2>&1
  m_q 'db.dropDatabase()'
  say "Un PRIMARY recibe las escrituras y 2 SECONDARY copian todo: es replicación, como en HDFS."
  say "UI: http://localhost:8083 (Mongo Express, base 'tienda')."
}

step1() {
  title "Paso 1 — Documentos JSON con esquema flexible"
  m 'db.productos.insertOne({ nombre: "Notebook Pro 14", categoria: "Notebook", precio: 1900000,
     specs: { ram_gb: 32, disco: "SSD 1 TB", peso_kg: 1.4 },
     tags: ["programar", "docker", "liviana"] })'
  m 'db.productos.insertMany([
  { nombre: "Celular Nova X", categoria: "Celular", precio: 650000, specs: { camara_mp: 108, bateria_mah: 5000 }, tags: ["foto"] },
  { nombre: "Auriculares Silence 700", categoria: "Auriculares", precio: 290000, inalambrico: true, tags: ["viajar", "ruido"] },
  { nombre: "Mouse Gamer Pro", categoria: "Mouse", precio: 65000, specs: { dpi: 26000, peso_g: 58 }, tags: ["gamer", "liviana"] },
  { nombre: "Notebook Oficina 15", categoria: "Notebook", precio: 780000, specs: { ram_gb: 8, disco: "SSD 256 GB", peso_kg: 2.1 } }
])' 'print(Object.keys(db.productos.insertMany([
  { nombre: "Celular Nova X", categoria: "Celular", precio: 650000, specs: { camara_mp: 108, bateria_mah: 5000 }, tags: ["foto"] },
  { nombre: "Auriculares Silence 700", categoria: "Auriculares", precio: 290000, inalambrico: true, tags: ["viajar", "ruido"] },
  { nombre: "Mouse Gamer Pro", categoria: "Mouse", precio: 65000, specs: { dpi: 26000, peso_g: 58 }, tags: ["gamer", "liviana"] },
  { nombre: "Notebook Oficina 15", categoria: "Notebook", precio: 780000, specs: { ram_gb: 8, disco: "SSD 256 GB", peso_kg: 2.1 } }
]).insertedIds).length + " documentos insertados")'
  m 'db.productos.findOne({ categoria: "Celular" })'
  say "Cada documento tiene los campos que necesita: el celular tiene cámara, el mouse DPI."
  say "No hubo CREATE TABLE ni ALTER TABLE: la colección se creó sola al insertar el primero."
  say "Y specs es un objeto adentro del documento, tags un array: nada de tablas auxiliares."
}

step2() {
  title "Paso 2 — Consultas: operadores, campos anidados y arrays"
  local fmt='.forEach(p => print(p.nombre.padEnd(26) + "$" + p.precio))'
  m 'db.productos.find({ precio: { $lt: 700000 } }, { nombre: 1, precio: 1, _id: 0 }).sort({ precio: -1 })' \
    "db.productos.find({ precio: { \$lt: 700000 } }).sort({ precio: -1 })$fmt"
  m 'db.productos.find({ "specs.ram_gb": { $gte: 16 } })' "db.productos.find({ 'specs.ram_gb': { \$gte: 16 } })$fmt"
  m 'db.productos.find({ tags: "liviana" })' "db.productos.find({ tags: 'liviana' })$fmt"
  m 'db.productos.find({ inalambrico: { $exists: true } })' "db.productos.find({ inalambrico: { \$exists: true } })$fmt"
  say "La consulta también es un documento JSON: { campo: { operador: valor } }."
  say "'specs.ram_gb' entra al objeto anidado; { tags: 'liviana' } busca adentro del array."
}

step3() {
  title "Paso 3 — Modelar embebiendo: un pedido con sus items adentro"
  m 'db.pedidos.insertOne({ _id: 1001, cliente: { nombre: "Ana", ciudad: "Rosario" }, fecha: new Date("2026-10-06"),
     items: [ { producto: "Notebook Pro 14", cantidad: 1, precio: 1900000 },
              { producto: "Mouse Gamer Pro",  cantidad: 2, precio: 65000 } ] })'
  m 'db.pedidos.updateOne({ _id: 1001 }, { $push: { items: { producto: "Auriculares Silence 700", cantidad: 1, precio: 290000 } } })' \
    'printjson(db.pedidos.updateOne({ _id: 1001 }, { $push: { items: { producto: "Auriculares Silence 700", cantidad: 1, precio: 290000 } } }))'
  m 'db.pedidos.updateOne({ _id: 1001 }, { $set: { estado: "pagado" } })' \
    'print("modificados: " + db.pedidos.updateOne({ _id: 1001 }, { $set: { estado: "pagado" } }).modifiedCount)'
  m 'db.pedidos.findOne({ _id: 1001 })'
  say "En SQL serían 3 tablas (pedidos, clientes, items) y un JOIN para leerlo."
  say "Acá el pedido se lee y se escribe entero en una operación: se modela según cómo se va a consultar."
}

step4() {
  title "Paso 4 — Escala: ${VENTAS} ventas con mongoimport"
  m_q 'db.ventas.drop()'
  echo "  generando ${VENTAS} ventas (mismo generador y semilla que hdfs-demo/scripts/gen_dataset.sh)..."
  echo "${GREEN}\$ ... | mongoimport --collection ventas${RESET}"
  local inicio=$SECONDS
  # Mismas filas que las primeras VENTAS líneas de dataset_demo.csv de la clase 2, en JSON.
  awk -v n="$VENTAS" 'BEGIN {
    srand(42)
    ns = split("Buenos Aires,Cordoba,Rosario,Mendoza,La Plata,Mar del Plata,Tucuman,Salta", suc, ",")
    np = split("Notebook,Celular,Tablet,Monitor,Teclado,Mouse,Auriculares,Impresora", prod, ",")
    split("850000,420000,310000,190000,35000,18000,45000,160000", precio, ",")
    for (i = 0; i < n; i++) {
      p = int(rand() * np) + 1
      cant = int(rand() * 5) + 1
      unit = int(precio[p] * (0.9 + rand() * 0.2))
      printf "{\"fecha\":{\"$date\":\"2026-%02d-%02dT00:00:00Z\"},", int(rand() * 12) + 1, int(rand() * 28) + 1
      printf "\"sucursal\":\"%s\",\"producto\":\"%s\",\"cantidad\":%d,\"precio_unitario\":%d,\"total\":%d}\n",
             suc[int(rand() * ns) + 1], prod[p], cant, unit, cant * unit
    }
  }' | docker exec -i "$(nodo)" mongoimport --quiet --uri "$URI" --collection ventas --numInsertionWorkers 4
  echo "  cargadas en $(( SECONDS - inicio )) s"
  m 'db.ventas.countDocuments()'
  m 'db.ventas.findOne({}, { _id: 0 })'
}

# Resumen de explain("executionStats"): etapas del plan, documentos revisados y tiempo.
EXPLAIN='const e = db.ventas.find({ sucursal: "Rosario", producto: "Notebook" }).explain("executionStats");
const etapas = []; let s = e.executionStats.executionStages;
while (s) { etapas.push(s.stage); s = s.inputStage; }
print("plan:           " + etapas.join(" ← "));
print("docs revisados: " + e.executionStats.totalDocsExamined);
print("docs devueltos: " + e.executionStats.nReturned);
print("tiempo:         " + e.executionStats.executionTimeMillis + " ms");'

step5() {
  title "Paso 5 — Índices: de recorrer todo a ir directo"
  m_q 'db.ventas.dropIndexes()'
  m 'db.ventas.find({ sucursal: "Rosario", producto: "Notebook" }).explain("executionStats")' "$EXPLAIN"
  say "COLLSCAN = leyó los ${VENTAS} documentos para devolver unos pocos miles."
  m 'db.ventas.createIndex({ sucursal: 1, producto: 1 })'
  m 'db.ventas.find({ sucursal: "Rosario", producto: "Notebook" }).explain("executionStats")' "$EXPLAIN"
  say "IXSCAN = usó el índice (un árbol B ordenado por sucursal+producto) y revisó solo lo que devuelve."
  say "Mismo concepto que el índice de un libro o de una base relacional. El precio: ocupa espacio y"
  say "hace más lenta cada escritura, así que se crean para las consultas que de verdad se usan."
}

step6() {
  title "Paso 6 — Aggregation pipeline: el GROUP BY de MongoDB"
  local fmt='.forEach(r => print(String(r._id).padEnd(15) + String(r.ventas).padStart(7) + " ventas   $" + Number(r.facturado).toLocaleString("es-AR")))'
  m 'db.ventas.aggregate([
  { $group: { _id: "$sucursal", ventas: { $sum: 1 }, facturado: { $sum: "$total" } } },
  { $sort:  { facturado: -1 } }
])' "const t = Date.now(); db.ventas.aggregate([
  { \$group: { _id: '\$sucursal', ventas: { \$sum: 1 }, facturado: { \$sum: '\$total' } } },
  { \$sort: { facturado: -1 } } ])$fmt; print('(' + (Date.now() - t) + ' ms)')"
  m 'db.ventas.aggregate([
  { $match: { producto: "Notebook", fecha: { $gte: ISODate("2026-12-01"), $lt: ISODate("2027-01-01") } } },
  { $group: { _id: "$sucursal", ventas: { $sum: 1 }, facturado: { $sum: "$total" } } },
  { $sort:  { facturado: -1 } },
  { $limit: 3 }
])' "db.ventas.aggregate([
  { \$match: { producto: 'Notebook', fecha: { \$gte: ISODate('2026-12-01'), \$lt: ISODate('2027-01-01') } } },
  { \$group: { _id: '\$sucursal', ventas: { \$sum: 1 }, facturado: { \$sum: '\$total' } } },
  { \$sort: { facturado: -1 } }, { \$limit: 3 } ])$fmt"
  say "Un pipeline: cada etapa recibe los documentos de la anterior. \$match = WHERE, \$group = GROUP BY."
  say "Son los mismos números que dio Elasticsearch en su paso 7: mismos datos, otro motor."
}

step7() {
  title "Paso 7 — Replica set: se cae el PRIMARY y el cluster elige otro"
  m_q 'db.alertas.drop()'
  estado_rs
  local p; p=$(primario)
  m 'db.alertas.insertOne({ msg: "escrito antes de la caída" })' \
    'print("insertado: " + db.alertas.insertOne({ msg: "escrito antes de la caída" }, { writeConcern: { w: "majority" } }).acknowledged)'
  say "La escritura fue al PRIMARY (${p}) y con w: majority esperó a que la copie al menos un SECONDARY."
  pause
  echo "${GREEN}\$ docker stop ${p}${RESET}"
  docker stop "$p" >/dev/null && echo "  ${RED}✘ ${p} apagado${RESET}"
  echo "  esperando la elección de un nuevo PRIMARY..."
  local inicio=$SECONDS nuevo=""
  for _ in $(seq 1 30); do
    nuevo=$(primario); [[ -n "$nuevo" && "$nuevo" != "$p" ]] && break; sleep 1
  done
  echo "  ✔ nuevo PRIMARY: ${BOLD}${nuevo}${RESET} (en ~$(( SECONDS - inicio )) s)"
  estado_rs
  m 'db.alertas.find({}, { _id: 0 })'
  m 'db.alertas.insertOne({ msg: "escrito con un nodo caído" })' \
    'print("insertado: " + db.alertas.insertOne({ msg: "escrito con un nodo caído" }).acknowledged)'
  say "Los 2 nodos que quedaron votaron un PRIMARY nuevo (2 de 3 = mayoría) y se sigue escribiendo."
  say "La aplicación usa la URI con los 3 nodos y el driver encuentra solo al nuevo PRIMARY."
  pause
  echo "${GREEN}\$ docker start ${p}${RESET}"
  docker start "$p" >/dev/null && echo "  ✔ ${p} de vuelta"
  sleep 8
  estado_rs
  echo "${GREEN}\$ docker exec ${p} mongosh   # leyendo directo de ${p}, que ahora es SECONDARY${RESET}"
  echo "${GREEN}tienda> db.alertas.find({}, { _id: 0 })${RESET}"
  docker exec "$p" mongosh --quiet "mongodb://localhost/tienda?directConnection=true&readPreference=secondaryPreferred"     --eval 'db.alertas.find({}, { _id: 0 }).forEach(d => print("  " + d.msg))'
  say "${p} volvió como SECONDARY y se puso al día solo: tiene hasta lo que se escribió mientras estaba caído."
}

closing() {
  title "Cierre"
  say "MongoDB es una base de documentos: JSON con esquema flexible, se modela embebiendo lo que se"
  say "lee junto, se consulta con documentos y se agrega con pipelines. Índices para ir rápido y"
  say "replica set para no depender de un solo nodo. Para crecer horizontalmente, además, sharding."
}

FROM="${1:-0}"; TO="${2:-${1:-7}}"
for i in $(seq "$FROM" "$TO"); do
  "step$i"
  (( i < TO )) && pause
done
[[ "$TO" -ge 7 ]] && closing
exit 0
