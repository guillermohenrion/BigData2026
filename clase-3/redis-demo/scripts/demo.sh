#!/usr/bin/env bash
# Guion de la demo Redis en vivo.
#   ./scripts/demo.sh          -> corre todos los pasos, pausando entre cada uno
#   ./scripts/demo.sh 4        -> corre solo el paso 4
#   ./scripts/demo.sh 2 5      -> corre del paso 2 al 5
#   AUTO=1 ./scripts/demo.sh   -> sin pausas (para ensayar)
set -uo pipefail

cd "$(dirname "$0")/.."
TTL="${TTL:-10}"

BOLD=$'\e[1m'; CYAN=$'\e[36m'; GREEN=$'\e[32m'; YELLOW=$'\e[33m'; RESET=$'\e[0m'

title() { echo; echo "${BOLD}${CYAN}════ $* ════${RESET}"; }
say()   { echo "${YELLOW}💬 $*${RESET}"; }
run()   { echo "${GREEN}\$ $*${RESET}"; "$@"; }
pause() { [[ "${AUTO:-0}" == "1" ]] && return; read -rp $'\n[Enter para continuar] ' _; }

# Muestra el comando tal como se tipea en redis-cli y lo ejecuta.
r() {
  echo "${GREEN}redis> $*${RESET}"
  docker exec redis redis-cli "$@" | sed 's/^/  /'
}

# Igual que r, pero junta la salida de a pares (campo valor / miembro puntaje).
r2() {
  echo "${GREEN}redis> $*${RESET}"
  docker exec redis redis-cli "$@" | paste -d' ' - - | sed 's/^/  /'
}

countdown() {
  local s="$1"
  while (( s > 0 )); do printf "\r  esperando %3ds " "$s"; sleep 1; ((s--)); done; printf "\r%20s\r" ""
}

wait_redis() {
  for _ in $(seq 1 30); do
    [[ "$(docker exec redis redis-cli ping 2>/dev/null)" == "PONG" ]] && { echo "  ✔ Redis responde PONG"; return 0; }
    sleep 1
  done
  echo "  ✘ Redis no responde (docker logs redis)"; return 1
}

step0() {
  title "Paso 0 — Levantar Redis"
  run docker compose up -d
  wait_redis
  r FLUSHALL >/dev/null
  say "Base vacía. UI: http://localhost:5540 → Add Redis database → host 'redis', puerto 6379"
}

step1() {
  title "Paso 1 — Clave → valor: lo más simple que hay"
  r SET producto:1001:nombre "Notebook 14 pulgadas"
  r GET producto:1001:nombre
  r SET visitas:home 0
  r INCR visitas:home
  r INCR visitas:home
  r INCRBY visitas:home 10
  say "INCR es atómico: mil clientes sumando a la vez nunca pisan el contador. Sin SQL, sin tablas."
  say "La convención 'objeto:id:campo' en el nombre es lo único que organiza los datos."
}

step2() {
  title "Paso 2 — TTL: datos que se borran solos (cache y sesiones)"
  r SET sesion:abc123 "usuario=guille" EX "$TTL"
  r TTL sesion:abc123
  r GET sesion:abc123
  say "Le dimos ${TTL} segundos de vida. Esperemos..."
  countdown $(( TTL + 1 ))
  r TTL sesion:abc123
  r GET sesion:abc123
  say "-2 = la clave ya no existe. Así se implementa un cache: si no está, se recalcula y se guarda con TTL."
}

step3() {
  title "Paso 3 — Hash: un objeto con campos (feature store online)"
  r HSET cliente:42 edad 35 segmento premium compras_30d 7 ticket_promedio 182000 score_riesgo 0.12
  r2 HGETALL cliente:42
  r HGET cliente:42 score_riesgo
  r HINCRBY cliente:42 compras_30d 1
  say "Esto es el 'online store' de un Feature Store: el modelo pide las features de un cliente"
  say "por su clave y Redis las devuelve en menos de un milisegundo, en el momento de predecir."
}

step4() {
  title "Paso 4 — Sorted set: un ranking que se mantiene ordenado solo"
  r DEL ranking:productos >/dev/null
  r ZINCRBY ranking:productos 5 Notebook
  r ZINCRBY ranking:productos 12 Celular
  r ZINCRBY ranking:productos 3 Tablet
  r ZINCRBY ranking:productos 8 Auriculares
  r ZINCRBY ranking:productos 9 Notebook
  r2 ZREVRANGE ranking:productos 0 2 WITHSCORES
  say "Top 3 más vendidos, siempre ordenado: cada venta suma con ZINCRBY y el ranking se lee en O(log N)."
  say "Así se hacen leaderboards de juegos, trending topics, 'lo más visto'."
}

step5() {
  title "Paso 5 — Lista como cola de trabajo"
  r DEL cola:emails >/dev/null
  r LPUSH cola:emails "bienvenida:ana@mail.com" "factura:juan@mail.com" "promo:sol@mail.com"
  r LLEN cola:emails
  r RPOP cola:emails
  r RPOP cola:emails
  say "Un productor encola con LPUSH, varios workers sacan con RPOP (o BRPOP, que espera si está vacía)."
  say "Es el desacople productor/consumidor; a escala, el mismo patrón lo hace Kafka."
}

step6() {
  title "Paso 6 — ¿Qué tan rápido es?"
  say "100.000 SET y 100.000 GET, 50 clientes en paralelo:"
  run docker exec redis redis-benchmark -q -n 100000 -c 50 -t set,get
  say "Del orden de 100.000 operaciones por segundo, con latencia de décimas de milisegundo (p50),"
  say "en una notebook y pasando por Docker. Todo vive en memoria RAM."
  r INFO memory | grep -E "used_memory_human|maxmemory_human"
}

step7() {
  title "Paso 7 — Persistencia: ¿se pierde todo si se reinicia?"
  r SET persistente "sigo acá"
  r SET temporal "me voy" EX 30
  run docker restart redis
  wait_redis
  r GET persistente
  r GET visitas:home
  r TTL temporal
  say "Con appendonly=yes Redis escribe cada operación en disco y la reproduce al arrancar."
  say "Hasta el TTL se guarda: 'temporal' sigue descontando sus 30 s y se va a borrar igual."
  say "Es memoria primero, pero no volátil."
}

closing() {
  title "Cierre"
  say "Redis no es una base relacional: es un diccionario gigante en memoria con estructuras de datos."
  say "Se usa al lado de la base principal para lo que necesita ser instantáneo:"
  say "cache, sesiones, contadores, rankings, colas y features online para modelos de ML."
}

FROM="${1:-0}"; TO="${2:-${1:-7}}"
for i in $(seq "$FROM" "$TO"); do
  "step$i"
  (( i < TO )) && pause
done
[[ "$TO" -ge 7 ]] && closing
exit 0
