#!/usr/bin/env bash
# Consulta concreta sobre HDFS mostrando qué DataNode entrega cada bloque,
# antes y después de apagar un nodo.
#   ./scripts/consulta.sh                 -> apaga el nodo que sirvió el bloque 0
#   ./scripts/consulta.sh datanode2       -> apaga ese nodo
#   SUCURSAL=Salta PRODUCTO=Celular ./scripts/consulta.sh  -> cambia el filtro
#   AUTO=1 ./scripts/consulta.sh          -> sin pausas
set -uo pipefail

# Git Bash convierte "/demo" en "C:/Program Files/Git/demo" al pasarlo a docker.
export MSYS_NO_PATHCONV=1

HDFS_PATH="/demo/dataset_demo.csv"
SUCURSAL="${SUCURSAL:-Rosario}"
PRODUCTO="${PRODUCTO:-Notebook}"
# ¿Cuántas ventas de PRODUCTO hubo en SUCURSAL, cuántas unidades y cuánto se facturó?
# (columnas: fecha,sucursal,producto,cantidad,precio_unitario,total)
QUERY="awk -F, '\$2==\"$SUCURSAL\" && \$3==\"$PRODUCTO\" {n++; u+=\$4; t+=\$6} END {printf \"RESULTADO: %d ventas, %d unidades, \$%.0f facturados\n\", n, u, t}'"

BOLD=$'\e[1m'; CYAN=$'\e[36m'; GREEN=$'\e[32m'; YELLOW=$'\e[33m'; RED=$'\e[31m'; RESET=$'\e[0m'

title() { echo; echo "${BOLD}${CYAN}════ $* ════${RESET}"; }
say()   { echo "${YELLOW}💬 $*${RESET}"; }
pause() { [[ "${AUTO:-0}" == "1" ]] && return; read -rp $'\n[Enter para continuar] ' _; }

# Corre la consulta con logs DEBUG del cliente HDFS y muestra, bloque a bloque,
# de qué DataNode se leyó. Deja en LAST_NODES los nodos usados (uno por bloque).
consulta() {
  echo "${GREEN}\$ hdfs dfs -cat $HDFS_PATH | $QUERY${RESET}"
  local log out
  log=$(docker exec -e HADOOP_ROOT_LOGGER=DEBUG,console namenode \
        sh -c "hdfs dfs -cat $HDFS_PATH | $QUERY" 2>&1)
  LAST_RESULT=$(echo "$log" | sed -n 's/^RESULTADO: //p' | tail -1)

  # "Connecting to datanode datanode3:9866" = intento de leer el siguiente bloque.
  # Si le sigue un WARN de conexión, ese intento falló y se prueba otra réplica.
  # El cliente loguea IPs: las traducimos a nombres con NODE_MAP.
  out=$(echo "$log" | sed "$NODE_MAP" | awk -v red="$RED" -v green="$GREEN" -v reset="$RESET" '
    function ok() { if (p != "") { printf "  bloque %d  %s→ %s%s\n", b++, green, p, reset; u = u p " " } }
    /Connecting to datanode/ { ok(); p = $NF; sub(/:.*/, "", p); next }
    /WARN/ && /(Failed to connect|Connection refused|No route to host|Exception)/ && p != "" {
      printf "  bloque %d  %s✘ %s no responde, pruebo otra réplica%s\n", b, red, p, reset; p = ""
    }
    END { ok(); print "NODES:" u }')
  echo "$out" | grep -v '^NODES:'
  LAST_NODES=$(echo "$out" | sed -n 's/^NODES://p')
  echo "  ${BOLD}${PRODUCTO} en ${SUCURSAL}: ${LAST_RESULT:-?}${RESET}"
}

# "Name: 172.20.0.3:9866 (datanode2.hdfs-demo_default)" -> s#172.20.0.3:9866#datanode2#g
NODE_MAP=$(docker exec namenode hdfs dfsadmin -report 2>/dev/null \
           | sed -n 's/^Name: \([0-9.:]*\) (\([a-z0-9]*\).*/s#\1#\2#g/p')

title "1 — Consulta con el cluster sano"
consulta
ANTES="$LAST_RESULT"
VICTIMA="${1:-$(echo "$LAST_NODES" | awk '{print $1}')}"
say "Cada bloque se leyó de uno de sus 3 DataNodes. Ahora apagamos ${VICTIMA}."
pause

title "2 — Apagamos ${VICTIMA}"
echo "${GREEN}\$ docker stop ${VICTIMA}${RESET}"
docker stop "$VICTIMA" >/dev/null && echo "  ${RED}✘ ${VICTIMA} apagado${RESET}"
pause

title "3 — Repetimos la misma consulta"
consulta
if [[ " $LAST_NODES " == *" $VICTIMA "* ]]; then
  say "Atención: se volvió a leer de ${VICTIMA}; revisá que esté apagado (docker ps)."
elif [[ "$LAST_RESULT" == "$ANTES" ]]; then
  say "Mismo resultado (${ANTES}), y ningún bloque salió de ${VICTIMA}: HDFS leyó las réplicas de otros nodos."
else
  say "El resultado cambió (${ANTES} → ${LAST_RESULT}): revisá con ./scripts/demo.sh 3"
fi
pause

title "4 — Volvemos a prender ${VICTIMA}"
echo "${GREEN}\$ docker start ${VICTIMA}${RESET}"
docker start "$VICTIMA" >/dev/null && echo "  ✔ ${VICTIMA} de vuelta"
