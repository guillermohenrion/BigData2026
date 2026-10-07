#!/usr/bin/env bash
# Guion de la demo Spark Structured Streaming en vivo (continúa la demo de ../kafka-demo).
#   ./scripts/demo.sh          -> corre todos los pasos, pausando entre cada uno
#   ./scripts/demo.sh 4        -> corre solo el paso 4
#   ./scripts/demo.sh 2 5      -> corre del paso 2 al 5
#   AUTO=1 ./scripts/demo.sh   -> sin pausas (para ensayar)
#   RATE=50 ./scripts/demo.sh  -> ventas por segundo que genera el productor (default 20)
#   ./scripts/demo.sh parar    -> detiene el productor de ventas
set -uo pipefail

# Git Bash convierte "/opt/..." en "C:/Program Files/Git/opt/..." al pasarlo a docker.
export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")/.."
BOOT="kafka1:19092,kafka2:19092,kafka3:19092"
PAQUETE="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
RATE="${RATE:-20}"

BOLD=$'\e[1m'; CYAN=$'\e[36m'; GREEN=$'\e[32m'; YELLOW=$'\e[33m'; RED=$'\e[31m'; RESET=$'\e[0m'

title() { echo; echo "${BOLD}${CYAN}════ $* ════${RESET}"; }
say()   { echo "${YELLOW}💬 $*${RESET}"; }
run()   { echo "${GREEN}\$ $*${RESET}"; "$@"; }
pause() { [[ "${AUTO:-0}" == "1" ]] && return; read -rp $'\n[Enter para continuar] ' _; }

kafka() { local cmd="$1"; shift; docker exec kafka1 "/opt/kafka/bin/${cmd}" --bootstrap-server "$BOOT" "$@" 2>/dev/null; }

# Lanza un job de streaming.py dentro del contenedor spark-streaming.
#   job MODO DURACION
job() {
  local modo="$1" dur="$2" jars
  jars=$(docker exec spark-streaming sh -c 'ls /root/.ivy2/jars/*.jar | paste -sd, -')
  echo "${GREEN}\$ spark-submit --packages ${PAQUETE} streaming.py ${modo}   (corre ${dur} s)${RESET}"
  docker exec -e DURACION="$dur" spark-streaming /opt/spark/bin/spark-submit --master "local[*]" --jars "$jars" \
    --driver-java-options "-Dlog4j2.configurationFile=file:/opt/demo/log4j2.properties" \
    /opt/demo/streaming.py "$modo" 2>&1 | grep -vE "^\s*$|WARN (ResolveWriteToStream|KafkaDataConsumer|OffsetSeqMetadata|HDFSBackedStateStoreProvider|NetworkClient)|WriteToDataSourceV2Exec|TaskKilled"
}

productor_corriendo() { docker exec kafka1 pgrep -f ConsoleProducer >/dev/null 2>&1; }

# Productor de ventas en tiempo real, adentro de kafka1: RATE ventas por segundo, con la hora actual.
iniciar_productor() {
  productor_corriendo && return
  docker exec -d kafka1 sh -c "
    while true; do
      ts=\$(date -u +%Y-%m-%dT%H:%M:%S)
      awk -v n=$RATE -v ts=\$ts -v seed=\$RANDOM 'BEGIN {
        srand(seed)
        ns = split(\"Buenos Aires,Cordoba,Rosario,Mendoza,La Plata,Mar del Plata,Tucuman,Salta\", suc, \",\")
        np = split(\"Notebook,Celular,Tablet,Monitor,Teclado,Mouse,Auriculares,Impresora\", prod, \",\")
        split(\"850000,420000,310000,190000,35000,18000,45000,160000\", precio, \",\")
        for (i = 0; i < n; i++) {
          p = int(rand() * np) + 1; cant = int(rand() * 5) + 1; s = suc[int(rand() * ns) + 1]
          printf \"%s|{\\\"ts\\\":\\\"%s\\\",\\\"sucursal\\\":\\\"%s\\\",\\\"producto\\\":\\\"%s\\\",\\\"cantidad\\\":%d,\\\"total\\\":%d}\\n\",
                 s, ts, s, prod[p], cant, cant * int(precio[p] * (0.9 + rand() * 0.2))
        }
      }'
      sleep 1
    done | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server $BOOT --topic ventas-stream \
             --property parse.key=true --property key.separator='|' >/dev/null 2>&1"
}

parar_productor() {
  docker exec kafka1 pkill -f ConsoleProducer >/dev/null 2>&1
  docker exec kafka1 pkill -f "while true" >/dev/null 2>&1
  echo "  ✔ productor detenido"
}

step0() {
  title "Paso 0 — Spark al lado del cluster Kafka"
  if ! docker network inspect kafka-demo_default >/dev/null 2>&1 \
     || ! docker ps --format '{{.Names}}' | grep -qx kafka1; then
    echo "${RED}✘ El cluster Kafka no está corriendo. Primero: cd ../kafka-demo && docker compose up -d${RESET}"
    exit 1
  fi
  run docker compose up -d
  parar_productor >/dev/null
  kafka kafka-topics.sh --delete --topic ventas-stream >/dev/null
  kafka kafka-topics.sh --delete --topic alertas >/dev/null
  docker exec spark-streaming rm -rf /tmp/checkpoints
  sleep 3
  kafka kafka-topics.sh --create --topic ventas-stream --partitions 3 --replication-factor 3
  kafka kafka-topics.sh --create --topic alertas --partitions 1 --replication-factor 3
  if ! docker exec spark-streaming sh -c 'ls /root/.ivy2/jars/*kafka*.jar' >/dev/null 2>&1; then
    echo "  bajando el conector de Kafka para Spark (solo la primera vez, ~1 min)..."
    docker exec spark-streaming /opt/spark/bin/spark-submit --master "local[1]" --packages "$PAQUETE" \
      /opt/demo/streaming.py preparar >/dev/null 2>&1
  fi
  docker exec spark-streaming sh -c 'ls /root/.ivy2/jars/' | sed 's/^/  ✔ /'
  say "Spark Structured Streaming lee Kafka con un conector: el mismo spark.read de siempre, en modo stream."
}

step1() {
  title "Paso 1 — Un productor genera ${RATE} ventas por segundo"
  iniciar_productor
  sleep 3
  echo "${GREEN}\$ kafka-console-consumer.sh --topic ventas-stream --max-messages 5${RESET}"
  kafka kafka-console-consumer.sh --topic ventas-stream --max-messages 5 --property print.key=true \
    | sed 's/\t/  /; s/^/  /'
  kafka kafka-get-offsets.sh --topic ventas-stream | sed 's/^/  /'
  say "Cada venta lleva su hora (ts): es el TIEMPO DE EVENTO, cuándo pasó, no cuándo llega a Spark."
  say "El productor queda corriendo en segundo plano. Mirá crecer los offsets en http://localhost:8084."
}

step2() {
  title "Paso 2 — Leer el stream: micro-batches"
  job crudo 22
  say "El Batch 0 sale vacío: el job arranca leyendo desde el final del topic (startingOffsets=latest)."
  say "Spark no procesa mensaje por mensaje: cada 5 s junta lo nuevo en un micro-batch y lo procesa"
  say "como un DataFrame común. El stream es una tabla que no para de crecer."
}

step3() {
  title "Paso 3 — Agregación acumulada: facturación por sucursal en vivo"
  job acumulado 27
  say "Mismo groupBy + sum que en batch. Spark guarda el estado (los totales) entre micro-batches"
  say "y en cada uno lo actualiza con lo nuevo: el resultado crece batch a batch."
}

step4() {
  title "Paso 4 — Ventanas de tiempo de evento (10 s) con watermark"
  job ventanas 42
  say "Cada fila es una ventana de 10 s de reloj de las ventas (ts), no de cuándo llegaron."
  say "Una ventana se sigue actualizando mientras lleguen datos suyos. El watermark (10 s) dice"
  say "cuánto esperar datos atrasados: pasado eso, la ventana se cierra y su estado se libera."
}

step5() {
  title "Paso 5 — De Kafka a Kafka: alertas de ventas grandes"
  say "Job: filtra ventas >= \$4.000.000 y las escribe al topic 'alertas'. Mientras, un consumidor las lee."
  job alertas 25 &
  local j=$!
  sleep 12
  echo "${GREEN}\$ kafka-console-consumer.sh --topic alertas --property print.key=true${RESET}"
  kafka kafka-console-consumer.sh --topic alertas --timeout-ms 15000 --property print.key=true \
    | sed 's/\t/  /; s/^/  🚨 /'
  wait "$j"
  say "Un pipeline de streaming completo: Kafka → Spark (filtro) → Kafka. Del otro lado puede haber"
  say "un servicio que mande un mail, otro que bloquee la tarjeta... cada uno con su consumer group."
}

step6() {
  title "Paso 6 — Checkpoint: se corta el job y retoma donde quedó"
  docker exec spark-streaming rm -rf /tmp/checkpoints/acumulado
  job checkpoint 22
  echo "  ${RED}✘ job detenido${RESET} — el productor sigue mandando ventas durante 15 s..."
  sleep 15
  echo "${GREEN}\$ ls /tmp/checkpoints/acumulado${RESET}"
  docker exec spark-streaming sh -c 'cd /tmp/checkpoints/acumulado; ls | tr "\n" " "; echo
    echo "offsets/ (batches empezados):   $(ls offsets | sort -n | tr "\n" " ")"
    echo "commits/ (batches terminados):  $(ls commits | sort -n | tr "\n" " ")"' | sed 's/^/  /'
  say "Si un batch figura en offsets/ y no en commits/, quedó a medias: al volver se reprocesa entero."
  job checkpoint 17
  say "El batchId siguió contando y los totales incluyen las ventas que llegaron con el job apagado"
  say "(el primer batch grande después de volver es el atraso acumulado):"
  say "en el checkpoint quedaron los offsets de Kafka ya procesados y el estado de la agregación."
  say "Sin checkpoint arrancaría de cero (o perdería lo del medio). Así se logra exactly-once."
}

closing() {
  title "Cierre"
  parar_productor
  say "Structured Streaming = el mismo DataFrame de Spark sobre una tabla infinita: micro-batches,"
  say "estado entre batches, ventanas por tiempo de evento y checkpoints para recuperarse."
  say "Kafka guarda y reparte los eventos; Spark los procesa; el resultado vuelve a Kafka o a una base."
}

if [[ "${1:-}" == "parar" ]]; then parar_productor; exit 0; fi
FROM="${1:-0}"; TO="${2:-${1:-6}}"
for i in $(seq "$FROM" "$TO"); do
  "step$i"
  (( i < TO )) && pause
done
[[ "$TO" -ge 6 ]] && closing
exit 0
