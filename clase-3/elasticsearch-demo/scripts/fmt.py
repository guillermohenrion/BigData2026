"""Resume en pocas líneas una respuesta JSON de Elasticsearch, para leer en clase.

    curl ... | python scripts/fmt.py hits      -> una línea por documento, con _score
    curl ... | python scripts/fmt.py tokens    -> los términos que genera _analyze
    curl ... | python scripts/fmt.py aggs      -> los buckets de cada agregación
    curl ... | python scripts/fmt.py raw       -> el JSON indentado
"""
import json
import re
import sys

B, G, Y, D, R = "\033[1m", "\033[32m", "\033[33m", "\033[2m", "\033[0m"


def money(v):
    return "$" + "{:,.0f}".format(v).replace(",", ".")


def hits(r):
    total = r["hits"]["total"]["value"]
    print("  {}{} documentos{}  ({} ms)".format(B, total, R, r["took"]))
    for h in r["hits"]["hits"]:
        s = h["_source"]
        print("  {:6.2f}  {:<38} {:<12} {:>12}".format(
            h["_score"] or 0, s.get("nombre", "")[:38], s.get("categoria", ""), money(s.get("precio", 0))))
        for frags in h.get("highlight", {}).values():
            for f in frags:
                print("          {}…{}…{}".format(D, re.sub(r"<em>(.*?)</em>", Y + r"\1" + R + D, f), R))


def tokens(r):
    print("  " + "  ".join("[{}{}{}]".format(G, t["token"], R) for t in r["tokens"]))


def buckets(name, agg, indent="  "):
    for b in agg.get("buckets", []):
        extra = []
        for k, v in b.items():
            if isinstance(v, dict) and "value" in v:
                extra.append("{} {}".format(k, money(v["value"]) if k.startswith(("facturado", "total")) else v["value"]))
        print("{}{:<16} {:>9} docs   {}".format(indent, str(b.get("key_as_string", b["key"])), b["doc_count"], "  ".join(extra)))
        for k, v in b.items():
            if isinstance(v, dict) and "buckets" in v:
                buckets(k, v, indent + "    ↳ ")


def aggs(r):
    print("  {}{} documentos analizados en {} ms{}".format(B, r["hits"]["total"]["value"], r["took"], R))
    for name, agg in r.get("aggregations", {}).items():
        print("  {}{}{}".format(B, name, R))
        if "value" in agg:
            print("    " + money(agg["value"]))
        buckets(name, agg, "    ")


def main():
    data = sys.stdin.buffer.read().decode("utf-8")  # en Windows stdin no es UTF-8 por defecto
    try:
        r = json.loads(data)
    except ValueError:
        print(data)
        return
    if "error" in r:
        print(json.dumps(r["error"], indent=2, ensure_ascii=False))
        return
    {"hits": hits, "tokens": tokens, "aggs": aggs}.get(
        sys.argv[1] if len(sys.argv) > 1 else "raw",
        lambda r: print(json.dumps(r, indent=2, ensure_ascii=False)))(r)


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    main()
