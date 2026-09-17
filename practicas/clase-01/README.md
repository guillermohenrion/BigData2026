# Clase práctica 1 — Ingesta y capa Bronze

## Propósito

Construir la primera capa de un Lakehouse en Databricks a partir de fuentes CSV, JSON y Parquet. Al terminar, cada alumno tendrá un espacio aislado en Unity Catalog y cuatro tablas Bronze trazables.

## Duración estimada

| Bloque | Minutos |
|---|---:|
| Setup y recorrido del workspace | 25 |
| Generación de fuentes | 25 |
| Lectura, esquemas y calidad inicial | 35 |
| Pausa | 10 |
| Parquet, Delta y tablas Bronze | 40 |
| Desafío de evolución de esquema | 35 |
| Puesta en común y cierre | 10 |

## Antes de la clase

1. Completar la [guía paso a paso de Databricks Free Edition](../GUIA_SETUP_DATABRICKS_FREE.md).
2. Confirmar que se puede abrir `00_setup.ipynb` y ejecutar `SELECT current_catalog()`.
3. No instalar paquetes: la práctica usa únicamente Spark y Delta incluidos en la plataforma.

## Resultados esperados

Al finalizar deben existir:

```text
<catalogo>.bigdata_<alumno>.landing
<catalogo>.bigdata_<alumno>.bronze_customers
<catalogo>.bigdata_<alumno>.bronze_products
<catalogo>.bigdata_<alumno>.bronze_transactions
<catalogo>.bigdata_<alumno>.bronze_events
```

## Criterios del checkpoint

- El notebook se puede volver a ejecutar sin romperse.
- Bronze conserva el dato original y agrega metadatos de ingesta.
- Los esquemas no dependen ciegamente de inferencia.
- Se identifican duplicados e importes inválidos, pero no se corrigen todavía: esa tarea corresponde a Silver.
- El alumno puede explicar por qué Delta es más que un formato de archivos.

## Recuperación

Si una ejecución se interrumpe, volver a ejecutar desde el setup. Las escrituras de archivos usan `overwrite` y las tablas se reemplazan de forma controlada. Para limpiar todo, revisar primero el nombre mostrado por el notebook y ejecutar manualmente:

```sql
DROP SCHEMA IF EXISTS `<catalogo>`.`bigdata_<alumno>` CASCADE;
```

Nunca usar `DROP CATALOG`.
