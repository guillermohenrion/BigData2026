#!/usr/bin/env bash
# Guion de la demo Kafka en vivo.
#   ./scripts/demo.sh          -> corre todos los pasos, pausando entre cada uno
#   ./scripts/demo.sh 4        -> corre solo el paso 4
#   ./scripts/demo.sh 2 5      -> corre del paso 2 al 5
#   AUTO=1 ./scripts/demo.sh   -> sin pausas (para ensayar)
set -uo pipefail

# Git Bash convierte "/opt/kafka/..." en "C:/Program Files/Git/opt/kafka/..." al pasarlo a docker.
export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")/.."
BOOT="kafka1:19092,kafka2:19092,kafka3:19092"
EVENTOS="${EVENTOS:-300000}"

BOLD=$'\e[1m'; CYAN=$'\e[36m'; GREEN=$'\e[32m'; YELLOW=$'\e[33m'; RED=$'\e[31m'; RESET=$'\e[0m'

title() { echo; echo "${BOLD}${CYAN}════ $* ════${RESET}"; }
say()   { echo "${YELLOW}💬 $*${RESET}"; }
run()   { echo "${GREEN}\$ $*${RESET}"; "$@"; }
pause() { [[ "${AUTO:-0}" == "1" ]] && return; read -rp $'\n[Enter para continuar] ' _; }

# Un broker vivo desde donde correr los CLIs (en el paso 6 apagamos uno).
nodo() {
  for n in kafka1 kafka2 kafka3; do
    docker ps --format '{{.Names}}' | grep -qx "$n" && { echo "$n"; return; }
  done
}

# k kafka-topics.sh --create ...  -> muestra el comando y lo corre dentro de un broker
k() {
  local cmd="$1"; shift
  echo "${GREEN}\$ ${cmd} $*${RESET}"
  docker exec "$(nodo)" "/opt/kafka/bin/${cmd}" --bootstrap-server "$BOOT" "$@" 2>&1 | grep -v "^\["
}
k_q() { local cmd="$1"; shift; docker exec "$(nodo)" "/opt/kafka/bin/${cmd}" --bootstrap-server "$BOOT" "$@" >/dev/null 2>&1; }

# Ventas en formato "clave|valor": la clave es la sucursal. Mismo generador y semilla que
# hdfs-demo/scripts/gen_dataset.sh; DESDE permite seguir donde quedó la tanda anterior.
ventas() {
  local n="$1" desde="${2:-0}"
  awk -v n="$n" -v desde="$desde" 'BEGIN {
    srand(42)
    ns = split("Buenos Aires,Cordoba,Rosario,Mendoza,La Plata,Mar del Plata,Tucuman,Salta", suc, ",")
    np = split("Notebook,Celular,Tablet,Monitor,Teclado,Mouse,Auriculares,Impresora", prod, ",")
    split("850000,420000,310000,190000,35000,18000,45000,160000", precio, ",")
    for (i = 0; i < desde + n; i++) {
      p = int(rand() * np) + 1; cant = int(rand() * 5) + 1
      unit = int(precio[p] * (0.9 + rand() * 0.2))
      mes = int(rand() * 12) + 1; dia = int(rand() * 28) + 1; s = suc[int(rand() * ns) + 1]
      if (i >= desde)
        printf "%s|{\"fecha\":\"2026-%02d-%02d\",\"producto\":\"%s\",\"cantidad\":%d,\"total\":%d}\n",
               s, mes, dia, prod[p], cant, cant * unit
    }
  }'
}

producir() {  # producir N [DESDE]
  echo "${GREEN}\$ ... | kafka-console-producer.sh --topic ventas --property parse.key=true --property key.separator='|'${RESET}"
  ventas "$@" | docker exec -i "$(nodo)" /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "$BOOT" \
    --topic ventas --producer-property acks=all --property parse.key=true --property "key.separator=|" 2>&1 | grep -v "^\["
}

# kafka-topics --describe resumido: una línea por partición, con nombres de broker.
describe() {
  echo "${GREEN}\$ kafka-topics.sh --describe --topic $1${RESET}"
  docker exec "$(nodo)" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOT" --describe --topic "$1" 2>/dev/null \
    | awk -F'\t' -v red="$RED" -v reset="$RESET" '
      function nombres(s) { gsub(/[0-9]+/, "kafka&", s); gsub(/,/, ", ", s); return s }
      /Partition:/ {
        delete f
        for (i = 1; i <= NF; i++) { split($i, kv, ": "); gsub(/^ +| +$/, "", kv[1]); f[kv[1]] = kv[2] }
        nr = split(f["Replicas"], a, ","); ni = split(f["Isr"], b, ",")
        printf "  partición %s   líder → %-7s  réplicas: %-24s  ISR: %s%s%s\n", f["Partition"],
               nombres(f["Leader"]), nombres(f["Replicas"]), (ni < nr ? red : ""), nombres(f["Isr"]), reset
      }'
}

brokers_vivos() {
  docker exec "$(nodo)" /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$BOOT" 2>/dev/null \
    | grep -c "(id:"
}

wait_brokers() {
  local want="$1" live=0
  echo "  esperando ${want} brokers..."
  for _ in $(seq 1 40); do
    live=$(brokers_vivos)
    [[ "${live:-0}" -ge "$want" ]] && { echo "  ✔ ${live} brokers vivos"; return 0; }
    sleep 3
  done
  echo "  ${RED}✘ solo ${live:-0} brokers (docker logs kafka1)${RESET}"; return 1
}

step0() {
  title "Paso 0 — Levantar un cluster de 3 brokers (KRaft, sin ZooKeeper)"
  run docker compose up -d
  wait_brokers 3
  k_q kafka-topics.sh --delete --topic ventas
  k_q kafka-topics.sh --delete --topic eventos
  for g in facturacion auditoria analitica; do k_q kafka-consumer-groups.sh --delete --group "$g"; done
  sleep 2
  echo "${GREEN}\$ kafka-metadata-quorum.sh describe --status${RESET}"
  docker exec "$(nodo)" /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server "$BOOT" describe --status 2>/dev/null \
    | awk '/^LeaderId/ { print "  controlador líder: kafka" $2 }
           /^CurrentVoters/ { s = ""; while (match($0, /"id": [0-9]+/)) {
                                s = s (s ? ", " : "") "kafka" substr($0, RSTART + 6, RLENGTH - 6)
                                $0 = substr($0, RSTART + RLENGTH) }
                              print "  votantes:          " s }'
  say "Los 3 brokers también votan entre ellos quién coordina el cluster (KRaft). Antes eso lo hacía ZooKeeper."
  say "Kafka UI: http://localhost:8084"
}

step1() {
  title "Paso 1 — Un topic partido en 3 particiones, cada una replicada en 3 brokers"
  k_q kafka-topics.sh --delete --topic ventas && sleep 3   # por si se repite el paso
  k kafka-topics.sh --create --topic ventas --partitions 3 --replication-factor 3
  sleep 1
  describe ventas
  say "Como los bloques de HDFS: el topic se parte (particiones) y cada parte se copia en 3 brokers."
  say "Cada partición tiene UN líder que recibe las escrituras; los otros copian. ISR = réplicas al día."
}

step2() {
  title "Paso 2 — Producir mensajes con clave"
  producir 24
  echo "${GREEN}\$ kafka-console-consumer.sh --topic ventas --from-beginning --property print.partition=true ...${RESET}"
  docker exec "$(nodo)" /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BOOT" --topic ventas \
    --from-beginning --timeout-ms 6000 --property print.partition=true --property print.offset=true \
    --property print.key=true 2>/dev/null | sort -t: -k2,2n -k3,3n \
    | awk -F'\t' '{ p = $1; if (++n[p] <= 3) { gsub(/\t/, "  "); print "  " $0 } else if (n[p] == 4) print "    ..." }'
  echo "${GREEN}\$ (qué sucursales cayeron en cada partición)${RESET}"
  docker exec "$(nodo)" /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BOOT" --topic ventas \
    --from-beginning --timeout-ms 6000 --property print.partition=true --property print.key=true 2>/dev/null \
    | awk -F'\t' '{ sub("Partition:", "", $1); n[$1]++; if (!(($1, $2) in s)) { s[$1, $2]; l[$1] = l[$1] (l[$1] ? ", " : "") $2 } }
                  END { for (p = 0; p < 3; p++) printf "  partición %d: %2d mensajes  ← %s\n", p, n[p], l[p] }'
  say "La clave decide la partición (hash de la clave): una sucursal SIEMPRE cae en la misma."
  say "El orden se garantiza dentro de una partición, no entre particiones. Offset = posición en el log."
}

step3() {
  title "Paso 3 — El log no se borra al leer: se puede releer"
  echo "${GREEN}\$ kafka-console-consumer.sh --topic ventas --partition 0 --offset 3 --max-messages 3${RESET}"
  docker exec "$(nodo)" /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BOOT" --topic ventas \
    --partition 0 --offset 3 --max-messages 3 --timeout-ms 6000 --property print.offset=true --property print.key=true \
    2>/dev/null | sed 's/\t/  /g; s/^/  /'
  k kafka-get-offsets.sh --topic ventas
  say "Leímos desde el offset 3 de la partición 0, después de haber leído todo en el paso 2."
  say "A diferencia de una cola (la lista de Redis: RPOP saca el mensaje), Kafka GUARDA el log"
  say "(7 días por defecto): cada consumidor lleva su propio offset y puede rebobinar."
}

step4() {
  title "Paso 4 — Consumer groups: dos consumidores se reparten las particiones"
  local tmp; tmp=$(mktemp -d)
  for g in facturacion auditoria; do k_q kafka-consumer-groups.sh --delete --group "$g"; done
  say "Arrancamos 2 consumidores en el grupo 'facturacion' y 1 en el grupo 'auditoria'."
  for c in 1 2; do
    docker exec "$(nodo)" /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BOOT" --topic ventas \
      --group facturacion --timeout-ms 12000 --property print.partition=true --property print.key=true \
      > "$tmp/facturacion_$c" 2>/dev/null &
  done
  docker exec "$(nodo)" /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BOOT" --topic ventas \
    --group auditoria --timeout-ms 12000 --property print.partition=true > "$tmp/auditoria" 2>/dev/null &
  sleep 8   # que se unan al grupo y se repartan las particiones
  producir 60 24
  wait
  for f in facturacion_1 facturacion_2 auditoria; do
    awk -F'\t' -v who="$f" '{ sub("Partition:", "", $1); n++; p[$1]++ }
      END { s = ""; for (k = 0; k < 3; k++) if (k in p) s = s " " k "(" p[k] ")"
            printf "  %-15s leyó %2d mensajes   particiones:%s\n", who, n, s }' "$tmp/$f"
  done
  rm -rf "$tmp"
  k kafka-consumer-groups.sh --describe --group facturacion
  say "Los consumidores ya terminaron ('no active members'), pero el grupo guardó hasta dónde leyó (CURRENT-OFFSET)."
  say "Dentro de un grupo cada partición la lee UN solo consumidor: 3 particiones = hasta 3 en paralelo."
  say "'auditoria' es otro grupo: recibe TODOS los mensajes, independiente de 'facturacion'."
  say "Así un mismo stream de ventas alimenta facturación, auditoría, un modelo de fraude... sin copiarlo."
}

step5() {
  title "Paso 5 — Rendimiento y lag: el productor va más rápido que el consumidor"
  # Arranca de cero para que el paso se pueda repetir
  k_q kafka-consumer-groups.sh --delete --group analitica
  k_q kafka-topics.sh --delete --topic eventos
  sleep 3
  k_q kafka-topics.sh --create --topic eventos --partitions 3 --replication-factor 3
  k_q kafka-consumer-groups.sh --group analitica --topic eventos --reset-offsets --to-earliest --execute
  echo "${GREEN}\$ kafka-producer-perf-test.sh --topic eventos --num-records ${EVENTOS} --record-size 200 --throughput -1${RESET}"
  docker exec "$(nodo)" /opt/kafka/bin/kafka-producer-perf-test.sh --topic eventos --num-records "$EVENTOS" \
    --record-size 200 --throughput -1 --producer-props bootstrap.servers="$BOOT" acks=all 2>/dev/null | tail -1 | sed 's/^/  /'
  k kafka-consumer-groups.sh --describe --group analitica
  say "LAG = mensajes que el grupo 'analitica' todavía no leyó. Kafka los guarda hasta que pueda."
  echo "${GREEN}\$ kafka-consumer-perf-test.sh --topic eventos --group analitica --messages ${EVENTOS}${RESET}"
  docker exec "$(nodo)" /opt/kafka/bin/kafka-consumer-perf-test.sh --bootstrap-server "$BOOT" --topic eventos \
    --group analitica --messages "$EVENTOS" --timeout 30000 2>/dev/null \
    | awk -F', *' 'NR == 2 { printf "  %d mensajes (%.0f MB) consumidos a %.0f mensajes/s (%.0f MB/s)\n", $5, $3, $6, $4 }'
  k kafka-consumer-groups.sh --describe --group analitica
  say "El consumidor se puso al día: LAG 0. El buffer entre quien produce y quien consume es el log."
}

step6() {
  title "Paso 6 — Se cae un broker: otro asume como líder"
  describe ventas
  local victima
  victima=$(docker exec "$(nodo)" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOT" --describe --topic ventas 2>/dev/null \
            | sed -n 's/.*Partition: 0.*Leader: \([0-9]*\).*/kafka\1/p')
  pause
  echo "${GREEN}\$ docker stop ${victima}${RESET}"
  docker stop "$victima" >/dev/null && echo "  ${RED}✘ ${victima} apagado (era líder de la partición 0)${RESET}"
  sleep 8
  describe ventas
  say "Las particiones que lideraba ${victima} eligieron otro líder entre las réplicas del ISR."
  say "El ISR quedó con 2 brokers (en rojo): falta una copia, pero no se perdió nada."
  producir 5 84
  echo "${GREEN}\$ (contamos todos los mensajes del topic)${RESET}"
  docker exec "$(nodo)" /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$BOOT" --topic ventas \
    --from-beginning --timeout-ms 8000 2>/dev/null | wc -l | sed 's/^ */  mensajes en ventas: /'
  say "24 + 60 + 5 = 89: se sigue escribiendo y leyendo con un broker menos (min.insync.replicas=2)."
  pause
  echo "${GREEN}\$ docker start ${victima}${RESET}"
  docker start "$victima" >/dev/null && echo "  ✔ ${victima} de vuelta"
  wait_brokers 3
  sleep 10
  describe ventas
  say "${victima} se puso al día copiando de los líderes y volvió al ISR. El liderazgo vuelve a"
  say "repartirse solo un rato después (preferred leader election, cada 5 min por defecto)."
}

closing() {
  title "Cierre"
  say "Kafka es un log distribuido: los productores agregan al final, los consumidores leen a su ritmo"
  say "con su propio offset, y el log se parte en particiones (paralelismo) replicadas (tolerancia a fallas)."
  say "Es la columna vertebral del streaming: el mismo evento alimenta a muchos sistemas sin acoplarlos."
}

FROM="${1:-0}"; TO="${2:-${1:-6}}"
for i in $(seq "$FROM" "$TO"); do
  "step$i"
  (( i < TO )) && pause
done
[[ "$TO" -ge 6 ]] && closing
exit 0
