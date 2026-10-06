# Demo Redis — base clave-valor en memoria, en Docker

Demo en vivo de **Redis**: clave → valor, TTL, hashes (feature store online), rankings con sorted
sets, colas, rendimiento y persistencia. Incluye **RedisInsight** para ver las claves en una UI web.

```
redis-demo/
├─ docker-compose.yml      # redis (6379, con AOF) + redisinsight (5540)
└─ scripts/
   ├─ demo.sh              # guion paso a paso, con pausas
   └─ reset.sh             # docker compose down -v
```

## Requisitos

- Docker Desktop (Redis usa ~10 MB, RedisInsight ~110 MB).
- Git Bash (Windows) o terminal (Mac/Linux).

## Antes de la clase

```bash
cd redis-demo
docker compose pull          # redis:8.2 + redisinsight:2.70
AUTO=1 ./scripts/demo.sh     # ensayo completo sin pausas (~40 s)
```

## Durante la clase

```bash
./scripts/demo.sh           # todo el guion, Enter entre pasos
./scripts/demo.sh 4         # solo un paso
./scripts/demo.sh 2 5       # un rango de pasos
TTL=20 ./scripts/demo.sh 2  # el TTL del paso 2 (default 10 s)
AUTO=1 ./scripts/demo.sh    # sin pausas, para ensayar
```

**RedisInsight:** <http://localhost:5540> → *Add Redis database* → host `redis`, puerto `6379`
(es el nombre del contenedor en la red de Docker; `localhost` no funciona desde RedisInsight).

| Paso | Qué se muestra | Comandos clave |
|---|---|---|
| 0 | Levantar Redis y vaciar la base | `docker compose up -d`, `FLUSHALL` |
| 1 | Clave → valor y contadores atómicos | `SET`, `GET`, `INCR` |
| 2 | **TTL**: claves que se borran solas (cache, sesiones) | `SET ... EX 10`, `TTL` |
| 3 | **Hash**: las features de un cliente (*online store* de un feature store) | `HSET`, `HGETALL` |
| 4 | **Sorted set**: ranking de productos siempre ordenado | `ZINCRBY`, `ZREVRANGE` |
| 5 | **Lista** como cola productor/consumidor | `LPUSH`, `RPOP` |
| 6 | Rendimiento: ~90.000 ops/s y p50 ≈ 0,3 ms en una notebook | `redis-benchmark` |
| 7 | Persistencia: reinicio el contenedor y los datos (y los TTL) siguen | `docker restart redis` |

## Tipear a mano

```bash
docker exec -it redis redis-cli
```

```
SET saludo "hola"
GET saludo
KEYS *                        # ver todas las claves (solo en demos: en producción bloquea el servidor)
TYPE cliente:42
HGETALL cliente:42
ZREVRANGE ranking:productos 0 -1 WITHSCORES
EXPIRE saludo 30
TTL saludo
```

**Pub/Sub en vivo** (dos terminales):

```bash
# Terminal 1 — se queda escuchando
docker exec -it redis redis-cli SUBSCRIBE alertas
# Terminal 2 — publica
docker exec redis redis-cli PUBLISH alertas "stock bajo: Notebook"
```

**Ver todo lo que llega al servidor** (útil mientras corre el guion en otra terminal):

```bash
docker exec -it redis redis-cli MONITOR
```

## Desde Python

```python
import redis                                   # pip install redis
r = redis.Redis(host="localhost", port=6379, decode_responses=True)

r.hset("cliente:42", mapping={"edad": 35, "segmento": "premium", "score_riesgo": 0.12})
r.hgetall("cliente:42")                        # {'edad': '35', 'segmento': 'premium', ...}

# Patrón cache-aside: busco en Redis; si no está, calculo y guardo con TTL
def precio_dolar():
    if (v := r.get("cache:dolar")) is not None:
        return float(v)
    v = 1450.0                                 # acá iría la consulta lenta (API, base de datos)
    r.set("cache:dolar", v, ex=60)
    return v
```

## Troubleshooting

- **`port is already allocated` (6379)**: hay otro Redis corriendo en tu máquina. Cambiá el
  puerto del host en el compose (`"6380:6379"`).
- **RedisInsight no conecta**: usá host `redis`, no `localhost`.
- **`the input device is not a TTY`** con `docker exec -it`: usá la terminal de VS Code o
  Windows Terminal, o anteponé `winpty`.
- **Limpiar todo**: `./scripts/reset.sh` (borra los datos y las conexiones guardadas de RedisInsight).
