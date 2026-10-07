# Demo Kafka — cluster de 3 brokers (KRaft) en Docker

Demo en vivo de **Apache Kafka**: un topic partido en particiones replicadas, mensajes con
clave, el log que se puede releer, consumer groups repartiéndose particiones, lag y rendimiento,
y la caída de un broker con cambio de líder. Incluye **Kafka UI** para ver topics, mensajes y grupos.

```
kafka-demo/
├─ docker-compose.yml      # kafka1..3 (broker + controller KRaft, sin ZooKeeper) + kafka-ui (8084)
└─ scripts/
   ├─ demo.sh              # guion paso a paso, con pausas
   └─ reset.sh             # docker compose down -v
```

## Requisitos

- Docker Desktop con **~1,5 GB de RAM** libres (~300 MB por broker + ~370 MB Kafka UI).
- Git Bash (Windows) o terminal (Mac/Linux).

## Antes de la clase

```bash
cd kafka-demo
docker compose pull          # apache/kafka:4.1.0 + kafbat/kafka-ui
AUTO=1 ./scripts/demo.sh     # ensayo completo sin pausas (~2:40 min)
```

## Durante la clase

```bash
./scripts/demo.sh           # todo el guion, Enter entre pasos
./scripts/demo.sh 4         # solo un paso (cada paso se puede repetir)
./scripts/demo.sh 2 5       # un rango de pasos
EVENTOS=1000000 ./scripts/demo.sh 5   # más mensajes en la prueba de rendimiento
```

**Kafka UI:** <http://localhost:8084> (el 8080 lo usa el Master de Spark de la clase 2).

| Paso | Qué se muestra | Resultado de referencia |
|---|---|---|
| 0 | 3 brokers que también forman el quórum KRaft (reemplaza a ZooKeeper) | controlador líder + 3 votantes |
| 1 | Topic `ventas`: **3 particiones × 3 réplicas**, con un líder por partición e ISR | como el `fsck` de HDFS |
| 2 | 24 ventas con **clave = sucursal**: cada sucursal cae siempre en la misma partición; offsets | Rosario, La Plata, Bs As → partición 0 |
| 3 | Releer desde un offset: el log **no se borra al leer** (vs. la cola de Redis) | |
| 4 | **Consumer groups**: 2 consumidores de `facturacion` se reparten las 3 particiones; `auditoria` recibe todo | 2 + 1 particiones; 60 mensajes |
| 5 | `kafka-producer-perf-test` → **LAG** de 300.000 → `kafka-consumer-perf-test` → LAG 0 | ~105.000 msg/s producidos (acks=all), ~560.000 msg/s consumidos |
| 6 | `docker stop` del líder de la partición 0 → **nuevo líder**, ISR con 2 → se sigue produciendo → vuelve y se pone al día | 24 + 60 + 5 = 89 mensajes |

### Puntos para comentar en clase

- **Comparación con lo que ya vimos.** Las particiones son la partición de HDFS y los shards de
  Elasticsearch. El líder por partición es como el PRIMARY de MongoDB, pero hay uno **por partición**,
  así que las escrituras se reparten entre los 3 brokers. El log persistente es lo que lo diferencia
  de la cola de Redis: leer no consume el mensaje.
- **Paso 2, la clave importa.** Con 8 sucursales y 3 particiones, el hash reparte desparejo (Tucumán
  queda sola en la partición 1). Es un buen momento para hablar de *hot partitions*: si una clave
  concentra el tráfico, esa partición es el cuello de botella.
- **Paso 4, el límite del paralelismo.** Un tercer consumidor en `facturacion` tomaría la partición
  que sobra; un cuarto quedaría ocioso. Para escalar consumidores hay que tener particiones.
- **Paso 6, `min.insync.replicas=2`.** Con `acks=all`, un mensaje se confirma cuando lo tienen al
  menos 2 réplicas. Con un broker caído se sigue escribiendo; con dos caídos, el productor
  recibiría `NotEnoughReplicas` en vez de arriesgar perder datos.

## Tipear a mano

```bash
export MSYS_NO_PATHCONV=1     # Git Bash: si no, cambia /opt/kafka por C:/Program Files/Git/opt/kafka
B=kafka1:19092,kafka2:19092,kafka3:19092

docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server $B --list
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server $B --describe --topic ventas

# Terminal 1 — consumidor que se queda escuchando
docker exec -it kafka1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server $B \
  --topic ventas --property print.key=true --property print.partition=true

# Terminal 2 — productor: cada línea "sucursal|mensaje" es un evento
docker exec -it kafka1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server $B \
  --topic ventas --property parse.key=true --property "key.separator=|"
```

Un buen ejercicio en vivo: con el consumidor de la terminal 1 corriendo, abrir otro igual con
`--group demo` en una tercera terminal y ver cómo se reparten las particiones (y cómo una sola
recibe todo cuando cerrás la otra: *rebalance*).

## Desde Python

Desde tu máquina se usan los listeners externos (`localhost:9092/9094/9096`):

```python
import json
from kafka import KafkaProducer, KafkaConsumer       # pip install kafka-python

BOOT = ["localhost:9092", "localhost:9094", "localhost:9096"]

productor = KafkaProducer(bootstrap_servers=BOOT, acks="all",
                          key_serializer=str.encode,
                          value_serializer=lambda v: json.dumps(v).encode())
md = productor.send("ventas", key="Salta", value={"producto": "Mouse", "total": 18000}).get(timeout=10)
print("partición", md.partition, "offset", md.offset)     # Salta → partición 2, igual que en la consola

consumidor = KafkaConsumer("ventas", bootstrap_servers=BOOT, group_id="mi-app",
                           auto_offset_reset="earliest", consumer_timeout_ms=5000,
                           value_deserializer=json.loads)
for msg in consumidor:
    print(msg.partition, msg.offset, msg.key, msg.value)
```

## Por qué 3 listeners por broker

Es el error más común al poner Kafka en Docker. El cliente se conecta a un broker, recibe la
lista de brokers **con las direcciones que cada uno anuncia** y después habla directo con el
líder de cada partición. Dentro de Docker esas direcciones tienen que ser `kafkaN:19092`; desde tu
máquina, `localhost:909x`. Por eso cada broker anuncia las dos (`KAFKA_ADVERTISED_LISTENERS`).
El tercero (`CONTROLLER :9093`) es el quórum KRaft entre brokers.

## Troubleshooting

- **El cliente de Python se cuelga o dice `kafka1:19092` no resuelve**: estás usando el listener
  interno desde afuera de Docker. Desde tu máquina usá `localhost:9092,localhost:9094,localhost:9096`.
- **`Consumer group 'x' has no active members`**: es normal cuando los consumidores ya terminaron;
  el grupo conserva los offsets.
- **`port is already allocated` (8084, 9092...)**: cambiá el puerto del host en el compose.
- **Un broker no arranca después de `reset.sh` parcial**: los 3 deben compartir el mismo
  `CLUSTER_ID`; si borraste el volumen de uno solo, borrá todos con `./scripts/reset.sh`.
- **`the input device is not a TTY`** con `docker exec -it`: usá la terminal de VS Code o
  Windows Terminal, o anteponé `winpty`.
- **Limpiar todo**: `./scripts/reset.sh` (borra topics, mensajes y offsets).
