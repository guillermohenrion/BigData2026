#!/usr/bin/env bash
# Guion de la demo HDFS en vivo.
#   ./scripts/demo.sh          -> corre todos los pasos, pausando entre cada uno
#   ./scripts/demo.sh 4        -> corre solo el paso 4
#   ./scripts/demo.sh 3 6      -> corre del paso 3 al 6
#   AUTO=1 ./scripts/demo.sh   -> sin pausas (para ensayar)
set -uo pipefail

# Git Bash convierte "/demo" en "C:/Program Files/Git/demo" al pasarlo a docker.
export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")/.."
DATASET="dataset_demo.csv"
HDFS_PATH="/demo/${DATASET}"
# Se declara muerto a los ~50 s con la config de hadoop.env
DEAD_WAIT="${DEAD_WAIT:-60}"

BOLD=$'\e[1m'; CYAN=$'\e[36m'; GREEN=$'\e[32m'; YELLOW=$'\e[33m'; RESET=$'\e[0m'

title() { echo; echo "${BOLD}${CYAN}════ $* ════${RESET}"; }
say()   { echo "${YELLOW}💬 $*${RESET}"; }
run()   { echo "${GREEN}\$ $*${RESET}"; "$@"; }
pause() { [[ "${AUTO:-0}" == "1" ]] && return; read -rp $'\n[Enter para continuar] ' _; }
hdfs()  { docker exec namenode hdfs "$@"; }

countdown() {
  local s="$1"
  while (( s > 0 )); do printf "\r  esperando %3ds " "$s"; sleep 1; ((s--)); done; printf "\r%20s\r" ""
}

wait_datanodes() {
  local want="$1" live=0
  echo "  esperando ${want} DataNodes vivos..."
  for _ in $(seq 1 60); do
    live=$(hdfs dfsadmin -report 2>/dev/null | sed -n 's/^Live datanodes (\([0-9]*\)).*/\1/p')
    [[ "${live:-0}" -ge "$want" ]] && { echo "  ✔ ${live} DataNodes vivos"; return 0; }
    sleep 2
  done
  echo "  ✘ solo ${live:-0} DataNodes vivos (revisá docker logs)"; return 1
}

# fsck resumido: una línea por bloque, con nombres de DataNode en vez de IPs.
#   FULL=1 ./scripts/demo.sh 3   -> muestra la salida completa de fsck
fsck() {
  echo "${GREEN}\$ docker exec namenode hdfs fsck $HDFS_PATH -files -blocks -locations${RESET}"
  local out map
  out=$(hdfs fsck "$HDFS_PATH" -files -blocks -locations 2>/dev/null)
  if [[ "${FULL:-0}" == "1" ]]; then echo "$out"; return; fi
  # "Name: 172.20.0.3:9866 (datanode2.hdfs-demo_default)" -> s/172.20.0.3:9866/datanode2/g
  map=$(hdfs dfsadmin -report 2>/dev/null \
        | sed -n 's/^Name: \([0-9.:]*\) (\([a-z0-9]*\).*/s#\1#\2#g/p')
  echo "$out" | grep -E '^[0-9]+\. ' \
    | sed -E 's/^([0-9]+)\. [^ ]*:(blk_[0-9]+)_[0-9]+ len=([0-9]+) Live_repl=([0-9]+) +\[(.*)\]$/  bloque \1  \2  \3 bytes  réplicas=\4  → \5/' \
    | sed -E 's/DatanodeInfoWithStorage\[([0-9.:]+),[^]]*\]/\1/g' \
    | sed "$map"
  echo "$out" | grep -E 'Total blocks|Under-replicated blocks|Average block replication|Missing blocks|is (HEALTHY|CORRUPT)'
}

step0() {
  title "Paso 0 — Levantar el cluster"
  run docker compose up -d
  wait_datanodes 3
  # Tras un reinicio el NameNode arranca en safe mode (solo lectura) hasta que los
  # DataNodes reportan sus bloques; subir archivos antes falla.
  hdfs dfsadmin -safemode wait >/dev/null 2>&1 && echo "  ✔ NameNode fuera de safe mode"
  run docker ps --format 'table {{.Names}}\t{{.Status}}'
  say "UI web del NameNode: http://localhost:9870  (pestaña Datanodes)"
}

step1() {
  title "Paso 1 — Subir el archivo a HDFS"
  [[ -f "$DATASET" ]] || ./scripts/gen_dataset.sh
  run docker cp "$DATASET" "namenode:/${DATASET}"
  run docker exec namenode hdfs dfs -mkdir -p /demo
  run docker exec namenode hdfs dfs -put -f "/${DATASET}" "$HDFS_PATH"
}

step2() {
  title "Paso 2 — El archivo está en HDFS"
  run docker exec namenode hdfs dfs -ls -h /demo
  say "HDFS ya decidió automáticamente en cuántos bloques partirlo."
}

step3() {
  title "Paso 3 — Bloques y ubicación de cada réplica"
  fsck
  say "Cada bloque no está en un solo lugar: está en 3 DataNodes distintos. Eso es replicación."
  say "UI: Utilities → Browse the file system → /demo → ${DATASET}"
}

step4() {
  title "Paso 4 — Matar un DataNode y seguir leyendo"
  echo "Lectura con el cluster sano:"
  run bash -c "docker exec namenode hdfs dfs -cat $HDFS_PATH 2>/dev/null | wc -c"
  pause
  run docker stop datanode1
  say "Simulamos una falla de hardware: datanode1 está caído."
  countdown 20
  echo "Lectura con un nodo menos:"
  run bash -c "docker exec namenode hdfs dfs -cat $HDFS_PATH 2>/dev/null | wc -c"
  say "Perdimos un tercio del storage del cluster y la aplicación ni se enteró."
}

step5() {
  title "Paso 5 — El NameNode detecta la falla"
  say "Esperamos a que el NameNode declare muerto a datanode1 (~50 s)."
  countdown "$DEAD_WAIT"
  run bash -c "docker exec namenode hdfs dfsadmin -report | grep -E 'Live datanodes|Dead datanodes|Under replicated'"
  fsck
  say "Cada bloque tiene ahora 2 réplicas en vez de 3: 'Under-replicated blocks'."
  say "Con un nodo sano libre, HDFS las re-copiaría solo (acá no hay un 4º nodo)."
}

step6() {
  title "Paso 6 — Revivir el nodo"
  run docker start datanode1
  wait_datanodes 3
  countdown 10
  fsck
  say "El DataNode se reincorporó, reportó sus bloques y volvemos a 3 réplicas."
}

step7() {
  title "Paso 7 (opcional) — Escalar agregando datanode4"
  run docker compose --profile scale up -d datanode4
  wait_datanodes 4
  run bash -c "docker exec namenode hdfs dfsadmin -report | grep -E '^(Live datanodes|Name:)'"
  say "El nodo nuevo se sumó solo, sin reconfigurar el NameNode. Miralo en http://localhost:9870 → Datanodes"
  say "Bonus: ahora 'docker stop datanode2' + ~1 min + fsck muestra re-replicación real hacia datanode4."
}

closing() {
  title "Cierre"
  say "Un archivo partido en bloques, cada bloque replicado en 3 nodos, y el sistema sigue"
  say "funcionando aunque perdimos un nodo: partición + replicación en un cluster real."
  say "Esto es lo que, por debajo, permite a Spark procesar petabytes de forma confiable."
}

FROM="${1:-0}"; TO="${2:-${1:-7}}"
for i in $(seq "$FROM" "$TO"); do
  "step$i"
  (( i < TO )) && pause
done
[[ "$TO" -ge 6 ]] && closing
exit 0
