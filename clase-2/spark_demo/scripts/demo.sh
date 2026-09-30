#!/usr/bin/env bash
# Guion de la demo Spark en vivo (continúa la demo de ../hdfs-demo).
#   ./scripts/demo.sh          -> corre todos los pasos, pausando entre cada uno
#   ./scripts/demo.sh 4        -> corre solo el paso 4
#   ./scripts/demo.sh 2 4      -> corre del paso 2 al 4
#   AUTO=1 ./scripts/demo.sh   -> sin pausas (para ensayar)
#
# Pasos 1-4 corren en UNA aplicación Spark (demo_spark.py), así la UI de
# http://localhost:4040 acumula todos los jobs y el RDD cacheado mientras dure.
set -uo pipefail

# Git Bash convierte "/opt/demo" en "C:/Program Files/Git/opt/demo" al pasarlo a docker.
export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")/.."
MASTER="spark://spark-master:7077"
# El paso 5 apaga este Worker a los KILL_AFTER segundos de arrancar el job.
VICTIMA="${VICTIMA:-spark-worker-1}"
KILL_AFTER="${KILL_AFTER:-12}"

BOLD=$'\e[1m'; CYAN=$'\e[36m'; GREEN=$'\e[32m'; YELLOW=$'\e[33m'; RED=$'\e[31m'; RESET=$'\e[0m'

title() { echo; echo "${BOLD}${CYAN}════ $* ════${RESET}"; }
say()   { echo "${YELLOW}💬 $*${RESET}"; }
run()   { echo "${GREEN}\$ $*${RESET}"; "$@"; }
pause() { [[ "${AUTO:-0}" == "1" ]] && return; read -rp $'\n[Enter para continuar] ' _; }

# spark-submit dentro del master. Con -it si hay terminal (pausas + barra de progreso).
submit() {
  local tty=()
  [[ -t 0 && -t 1 && "${AUTO:-0}" != "1" ]] && tty=(-it)
  docker exec "${tty[@]}" -e AUTO="${AUTO:-0}" spark-master \
    /opt/spark/bin/spark-submit --master "$MASTER" \
    --driver-java-options "-Dlog4j2.configurationFile=file:/opt/demo/log4j2.properties" \
    /opt/demo/demo_spark.py "$@"
}

workers_vivos() {
  curl -s localhost:8080/json/ | sed -n 's/.*"aliveworkers" : \([0-9]*\).*/\1/p'
}

wait_workers() {
  local want="$1" live=0
  echo "  esperando ${want} Workers registrados en el Master..."
  for _ in $(seq 1 30); do
    live=$(workers_vivos)
    [[ "${live:-0}" -ge "$want" ]] && { echo "  ✔ ${live} Workers vivos"; return 0; }
    sleep 2
  done
  echo "  ✘ solo ${live:-0} Workers (revisá docker logs spark-master)"; return 1
}

step0() {
  title "Paso 0 — Levantar el cluster Spark al lado del de HDFS"
  if ! docker network inspect hdfs-demo_default >/dev/null 2>&1 \
     || ! docker ps --format '{{.Names}}' | grep -qx namenode; then
    echo "${RED}✘ El cluster HDFS no está corriendo. Primero: cd ../hdfs-demo && ./scripts/demo.sh 0 1${RESET}"
    exit 1
  fi
  if ! docker exec namenode hdfs dfs -test -e /demo/dataset_demo.csv 2>/dev/null; then
    echo "${RED}✘ Falta /demo/dataset_demo.csv en HDFS. Subilo con: cd ../hdfs-demo && ./scripts/demo.sh 1${RESET}"
    exit 1
  fi
  run docker compose up -d
  wait_workers 2
  run docker ps --format 'table {{.Names}}\t{{.Status}}'
  say "UI del Master: http://localhost:8080 (2 Workers, cores y memoria de cada uno)."
  say "Dejala abierta al lado de la UI de HDFS: http://localhost:9870"
}

step5() {
  title "Paso 5 (opcional) — Matar un Worker a mitad de un job"
  [[ "$(workers_vivos)" -ge 2 ]] || { docker start "$VICTIMA" >/dev/null; wait_workers 2; }
  say "Lanzamos un job de 24 tareas y a los ${KILL_AFTER} s apagamos ${VICTIMA}."
  say "Mirá http://localhost:4040 → Executors mientras pasa."
  AUTO=1 submit 5 &
  local job=$!
  sleep "$KILL_AFTER"   # sin countdown: se mezclaría con la salida del job
  echo "${GREEN}\$ docker stop ${VICTIMA}${RESET}"
  docker stop -t 1 "$VICTIMA" >/dev/null && echo "  ${RED}✘ ${VICTIMA} apagado a mitad del job${RESET}"
  wait "$job"
  pause
  title "Revivimos ${VICTIMA}"
  run docker start "$VICTIMA"
  wait_workers 2
}

closing() {
  title "Cierre"
  say "Leímos directo de HDFS, repartimos el cómputo entre Workers y, sobre todo, vimos la"
  say "diferencia entre recalcular y cachear en memoria: por eso Spark reemplazó a MapReduce."
  say "HDFS sigue dando el storage confiable; lo que cambió es que el cómputo ya no pasa por disco en cada paso."
}

FROM="${1:-0}"; TO="${2:-${1:-5}}"
(( FROM <= 0 && TO >= 0 )) && { step0; (( TO > 0 )) && pause; }
# Pasos 1-4 en una sola app: demo_spark.py pausa entre ellos y al final (Enter para cerrar).
A=$(( FROM > 1 ? FROM : 1 )); B=$(( TO < 4 ? TO : 4 ))
(( A <= B )) && submit "$A" "$B"
(( TO >= 5 )) && { step5; }
(( TO >= 4 )) && closing
exit 0
