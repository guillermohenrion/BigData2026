#!/usr/bin/env bash
# Genera un CSV de ventas para ver la partición en bloques de 128 MB y hacer consultas.
#   fecha,sucursal,producto,cantidad,precio_unitario,total
# Uso: ./scripts/gen_dataset.sh [MB]   (default 400 MB -> 3 bloques; 500 -> 4)
# Usa semilla fija: el mismo tamaño genera siempre el mismo archivo.
set -euo pipefail

SIZE_MB="${1:-400}"
OUT="$(dirname "$0")/../dataset_demo.csv"

echo "Generando ~${SIZE_MB} MB de ventas en ${OUT} ..."
awk -v target=$((SIZE_MB * 1000 * 1000)) 'BEGIN {
  srand(42)
  ns = split("Buenos Aires,Cordoba,Rosario,Mendoza,La Plata,Mar del Plata,Tucuman,Salta", suc, ",")
  np = split("Notebook,Celular,Tablet,Monitor,Teclado,Mouse,Auriculares,Impresora", prod, ",")
  split("850000,420000,310000,190000,35000,18000,45000,160000", precio, ",")
  line = "fecha,sucursal,producto,cantidad,precio_unitario,total"
  print line; bytes = length(line) + 1
  while (bytes < target) {
    p = int(rand() * np) + 1
    cant = int(rand() * 5) + 1
    unit = int(precio[p] * (0.9 + rand() * 0.2))
    line = sprintf("2026-%02d-%02d,%s,%s,%d,%d,%d", int(rand() * 12) + 1, int(rand() * 28) + 1,
                   suc[int(rand() * ns) + 1], prod[p], cant, unit, cant * unit)
    print line; bytes += length(line) + 1
  }
}' > "$OUT"

ls -lh "$OUT"
head -4 "$OUT"
SIZE=$(wc -c < "$OUT")
echo "Filas: $(( $(wc -l < "$OUT") - 1 ))   Bloques esperados con 128 MB: $(( (SIZE + 134217727) / 134217728 ))"
