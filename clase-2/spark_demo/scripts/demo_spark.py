"""Demo Spark en vivo: corre dentro de spark-master con spark-submit (ver demo.sh).

    demo_spark.py 1 4    -> pasos 1 a 4 en una sola aplicación (la UI :4040 acumula todo)
    demo_spark.py 5      -> paso 5, el job largo para matar un Worker a mitad de camino

Pausa entre pasos solo si hay terminal (docker exec -it) y AUTO != 1.
"""
import json
import os
import socket
import sys
import time
import urllib.request
from operator import add

from pyspark import SparkConf, SparkContext

HDFS_PATH = "hdfs://namenode:9000/demo/dataset_demo.csv"
# columnas: fecha,sucursal,producto,cantidad,precio_unitario,total

BOLD, CYAN, GREEN, YELLOW, RED, RESET = "\033[1m", "\033[36m", "\033[32m", "\033[33m", "\033[31m", "\033[0m"
INTERACTIVE = sys.stdin.isatty() and os.environ.get("AUTO", "0") != "1"


def title(t):
    print("\n{}{}════ {} ════{}".format(BOLD, CYAN, t, RESET))


def say(t):
    print("{}💬 {}{}".format(YELLOW, t, RESET))


def code(t):
    print("{}>>> {}{}".format(GREEN, t, RESET))


def pause(msg="[Enter para continuar] "):
    if INTERACTIVE:
        input("\n" + msg)


def timed(label, fn):
    inicio = time.time()
    res = fn()
    print("  {}{:<42}{} {:6.2f} s".format(BOLD, label, RESET, time.time() - inicio))
    return res


# --- Qué Worker corrió cada tarea: se lo preguntamos a la API REST de la UI del driver ---

_hosts = {}


def worker_name(ip):
    """172.20.0.7 -> spark-worker-1 (DNS inverso de la red de Docker)."""
    if ip not in _hosts:
        try:
            _hosts[ip] = socket.gethostbyaddr(ip)[0].split(".")[0]
        except OSError:
            _hosts[ip] = ip
    return _hosts[ip]


def api(sc, path):
    url = "{}/api/v1/applications/{}/{}".format(sc.uiWebUrl, sc.applicationId, path)
    with urllib.request.urlopen(url) as r:
        return json.load(r)


def wait_executors(sc, n=2):
    """Espera a que se registren los executors y resuelve sus nombres ya, mientras están vivos
    (en el paso 5 el Worker caído deja de resolver por DNS)."""
    for _ in range(30):
        ejecutores = [e for e in api(sc, "executors") if e["id"] != "driver"]
        if len(ejecutores) >= n:
            break
        time.sleep(1)
    for e in ejecutores:
        e["worker"] = worker_name(e["hostPort"].split(":")[0])
    return sorted(ejecutores, key=lambda e: e["id"])


def show_tasks(sc, job_group):
    """Una línea por tarea de los jobs del grupo: stage, partición, Worker y duración."""
    time.sleep(1)  # la UI procesa los eventos del job de forma asíncrona
    jobs = [j for j in api(sc, "jobs") if j.get("jobGroup") == job_group]
    stage_ids = sorted(s for j in jobs for s in j["stageIds"])
    for sid in stage_ids:
        try:
            tasks = api(sc, "stages/{}/0/taskList?length=500".format(sid))
        except Exception:
            continue  # stage salteado (ya estaba calculado)
        for t in sorted(tasks, key=lambda t: t["index"]):
            ip = t["host"]
            color = RED if t["status"] != "SUCCESS" else GREEN
            print("  stage {}  tarea {:>2}  {}→ {:<15}{}  {:5.2f} s  {}".format(
                sid, t["index"], color, worker_name(ip), RESET,
                t.get("duration", 0) / 1000.0, "" if t["status"] == "SUCCESS" else t["status"]))


def run_group(sc, group, fn):
    sc.setJobGroup(group, group)
    try:
        return fn()
    finally:
        sc.setLocalProperty("spark.jobGroup.id", None)


# --- La transformación "cara": parsear cada línea del CSV ---

def parsear(linea):
    f = linea.split(",")
    if f[0] == "fecha":  # encabezado
        return None
    return (f[0], f[1], f[2], int(f[3]), int(f[4]), int(f[5]))


# ------------------------------------------------------------------------------------------

def step1(sc, ctx):
    title("Paso 1 — Un driver conectado al cluster")
    code("sc.master")
    print("  " + sc.master)
    for e in wait_executors(sc):
        print("  executor {} en {:<15} {} cores".format(e["id"], e["worker"], e["totalCores"]))
    say("El SparkContext (sc) es el driver: planifica. Los Workers de http://localhost:8080 ejecutan.")
    say("UI de esta aplicación (jobs, stages, tareas): http://localhost:4040")


def step2(sc, ctx):
    title("Paso 2 — Leer el archivo directamente desde HDFS")
    code('lines = sc.textFile("{}")'.format(HDFS_PATH))
    lines = sc.textFile(HDFS_PATH)
    ctx["lines"] = lines
    code("lines.take(3)")
    for l in lines.take(3):
        print("  " + l)
    code("lines.count()")
    n = run_group(sc, "paso2", lambda: timed("lines.count()", lines.count))
    print("  {:,} líneas".format(n).replace(",", "."))
    code("lines.getNumPartitions()")
    print("  {}".format(lines.getNumPartitions()))
    say("3 bloques de 128 MB en HDFS → 3 particiones → 3 tareas en paralelo. ¿Quién corrió cada una?")
    show_tasks(sc, "paso2")
    say("No copiamos nada: cada tarea le pidió su bloque a HDFS. Storage y cómputo son sistemas separados.")


def step3(sc, ctx):
    lines = ctx.get("lines") or sc.textFile(HDFS_PATH)
    title("Paso 3 — Map → Shuffle → Reduce: facturación por sucursal")
    code("ventas = lines.map(parsear).filter(lambda v: v is not None)")
    code("por_sucursal = ventas.map(lambda v: (v[1], v[5])).reduceByKey(add)")
    code("por_sucursal.collect()")
    ventas = lines.map(parsear).filter(lambda v: v is not None)
    por_sucursal = ventas.map(lambda v: (v[1], v[5])).reduceByKey(add)
    res = run_group(sc, "paso3", lambda: timed("por_sucursal.collect()", por_sucursal.collect))
    for suc, total in sorted(res, key=lambda x: -x[1]):
        print("  {:<14} ${:>18,}".format(suc, total).replace(",", "."))
    print("  {}{:<14} ${:>18,}{}".format(BOLD, "TOTAL", sum(t for _, t in res), RESET).replace(",", "."))
    say("map emite pares (sucursal, total); reduceByKey es el shuffle + reduce agrupando por clave.")
    say("Es MapReduce, pero en una sola cadena de transformaciones. Mirá las tareas: 2 stages separados por el shuffle.")
    show_tasks(sc, "paso3")
    say("En http://localhost:4040 → Jobs → el último job → DAG Visualization.")


def step4(sc, ctx):
    lines = ctx.get("lines") or sc.textFile(HDFS_PATH)
    title("Paso 4 — Recalcular vs. cachear en memoria")
    code("datos = lines.map(parsear)")
    datos = lines.map(parsear)
    t1 = timed("Sin cache, 1ra vez  datos.count()", datos.count)
    t2 = timed("Sin cache, 2da vez  datos.count()", datos.count)
    say("Casi lo mismo las dos veces: cada acción vuelve a leer de HDFS y a parsear todo (linaje completo).")
    pause()

    code("datos_cacheados = lines.map(parsear).cache()")
    datos_cacheados = lines.map(parsear).cache()
    ctx["cacheados"] = datos_cacheados
    timed("Con cache, 1ra vez (calcula y guarda)", datos_cacheados.count)
    timed("Con cache, 2da vez (desde memoria)", datos_cacheados.count)
    say("La 2da vez no toca HDFS ni vuelve a parsear: el RDD ya vive en la memoria de los Workers.")
    say("http://localhost:4040 → Storage: el RDD cacheado, cuánta memoria ocupa y en qué Workers.")
    pause()

    title("Paso 4b — Un algoritmo iterativo: 5 pasadas sobre el mismo dataset")
    productos = ["Notebook", "Celular", "Tablet", "Monitor", "Impresora"]
    code("for p in productos: datos.filter(lambda v: v and v[2] == p).map(lambda v: v[5]).sum()")

    def iterar(rdd):
        return [rdd.filter(lambda v, p=p: v is not None and v[2] == p).map(lambda v: v[5]).sum()
                for p in productos]

    timed("5 pasadas SIN cache (5 lecturas de HDFS)", lambda: iterar(datos))
    timed("5 pasadas CON cache (0 lecturas de HDFS)", lambda: iterar(datos_cacheados))
    say("Un algoritmo de ML que recorre 50 veces el dataset: en MapReduce son 50 lecturas de disco;")
    say("en Spark, una sola lectura y 50 pasadas en memoria.")


def step5(sc, ctx):
    title("Paso 5 — Tolerancia a fallas: el job sigue aunque se caiga un Worker")
    code("lines = sc.textFile(HDFS_PATH, minPartitions=24)   # más tareas, para que se vea el reparto")
    code("lines.mapPartitions(procesar_particion).sum()")
    lines = sc.textFile(HDFS_PATH, minPartitions=24)
    wait_executors(sc)

    def procesar_particion(it):
        time.sleep(3)  # simula trabajo pesado: cada tarea tarda unos segundos
        yield sum(v[5] for v in map(parsear, it) if v is not None)

    total = run_group(sc, "paso5", lambda: timed("sum() sobre 24 tareas", lines.mapPartitions(procesar_particion).sum))
    print("  {}Total facturado: ${:,}{}".format(BOLD, total, RESET).replace(",", "."))
    show_tasks(sc, "paso5")
    say("Las tareas que corrían en el Worker caído se reintentaron en el otro (líneas en rojo = intentos perdidos).")
    say("El total coincide con el del paso 3: Spark recalcula lo perdido a partir del linaje, sin réplicas.")


def main():
    args = [int(a) for a in sys.argv[1:]] or [1, 4]
    desde, hasta = args[0], args[-1]
    conf = (SparkConf().setAppName("demo-spark-paso{}".format(desde) if desde == hasta
                                   else "demo-spark-pasos{}-{}".format(desde, hasta))
            # la barra [Stage 3:====> (2 + 1) / 3] solo se ve bien en una terminal
            .set("spark.ui.showConsoleProgress", str(sys.stdout.isatty()).lower()))
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    ctx = {}
    for i in range(desde, hasta + 1):
        globals()["step{}".format(i)](sc, ctx)
        if i < hasta:
            pause()
    pause("[Enter para cerrar la aplicación — la UI de http://localhost:4040 se apaga con ella] ")
    sc.stop()


if __name__ == "__main__":
    main()
