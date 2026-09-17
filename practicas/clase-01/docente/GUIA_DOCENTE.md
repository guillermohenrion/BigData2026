# Guía docente — Clase práctica 1

## Resultado esperado

Al finalizar, el alumno debe poder explicar y demostrar:

- Por qué Spark trabaja con esquemas y evaluación lazy.
- Qué se conserva en una capa Bronze.
- Qué diferencias prácticas existen entre CSV/JSON, Parquet y Delta.
- Cómo Unity Catalog organiza catálogo, esquema, volumen y tabla.
- Por qué aceptar una evolución técnica de esquema no equivale a validarla semánticamente.

## Preparación docente

1. Probar los notebooks con una cuenta nueva de Free Edition.
2. Ejecutar primero en escala `test` y después en `small`.
3. Confirmar que el catálogo actual permite crear un esquema y un volumen administrado.
4. Mantener una copia exportada de los notebooks por si Git folders no está disponible para algún alumno.
5. No depender de Internet ni instalar paquetes durante la clase.

## Guion sugerido

### 0–25 min — Setup

- Mostrar workspace, Git folder, notebook, catálogo y esquema.
- Todos deben llegar al checkpoint de `00_setup`.
- Si alguien falla, usar `test` y verificar primero su `student_id`.

### 25–60 min — Fuentes

- Ejecutar el generador y recorrer el volumen `landing`.
- Preguntar qué formato usarían para intercambio y cuál para analítica.
- No explicar todavía todas las ventajas de Delta; dejar que aparezcan en la comparación.

### 60–85 min — Esquemas

- Comparar CSV con Parquet.
- Mostrar que `N/A` hace que `amount` sea problemático.
- Introducir la diferencia entre preservar, rechazar y corregir.

### 85–95 min — Pausa

### 95–135 min — Bronze y Delta

- Crear las cuatro tablas.
- Inspeccionar detalle, historia y plan físico.
- Vincular archivos físicos, metadatos y tabla lógica.

### 135–170 min — Desafío

- Permitir trabajo en parejas, pero exigir entrega individual.
- Dar primero pistas conceptuales: `unionByName`, columnas faltantes y deduplicación.
- Mostrar código solamente durante la puesta en común.

### 170–180 min — Cierre

- Recuperar las 5V en el caso.
- Conectar el problema de calidad con la futura capa Silver.

## Resultados e invariantes

- `bronze_customers`: exactamente la cantidad configurada de clientes.
- `bronze_products`: exactamente la cantidad configurada de productos.
- `bronze_transactions`: más filas que IDs distintos por los duplicados inyectados.
- `bronze_transactions.amount`: se conserva como texto y contiene valores `N/A`.
- `bronze_events_v2`: una fila por `event_id`, contiene `app_version` y al menos un `refund`.
- Al ejecutar nuevamente el desafío, `bronze_events_v2` mantiene el mismo total.

No fijar en la consigna números absolutos: dependen de la escala seleccionada.

## Errores frecuentes

| Síntoma | Causa probable | Resolución |
|---|---|---|
| No puede crear el esquema | Identificador inválido o catálogo sin privilegios | Normalizar `student_id` y revisar `current_catalog()` |
| `%run` no encuentra `common` | Se importó un notebook suelto | Importar la carpeta `practicas` completa |
| Se agota la cuota | Se usó `demo` o se repitieron acciones costosas | Cambiar a `test`; continuar cuando se restablezca la cuota |
| `amount` sigue siendo string | Comportamiento esperado de Bronze | Usar `try_cast` solo para medir; corregir en Silver |
| Falla la unión de eventos | Esquemas diferentes | Usar `unionByName(..., allowMissingColumns=True)` |
| Aparecen duplicados | Se anexó sin deduplicar | Aplicar una regla explícita por `event_id` |

## Rúbrica del checkpoint

| Criterio | Puntos |
|---|---:|
| Setup aislado y cuatro tablas Bronze | 3 |
| Diagnóstico correcto de tipos y calidad | 2 |
| Integración de evolución de esquema | 2 |
| Deduplificación y ejecución repetible | 2 |
| Explicación técnica y vínculo con las 5V | 1 |

