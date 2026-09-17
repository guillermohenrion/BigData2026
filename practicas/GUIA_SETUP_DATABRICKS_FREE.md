# Guía paso a paso — Databricks Free Edition

Esta guía parte desde cero y termina con el notebook de verificación ejecutado. Está preparada para Databricks Free Edition y fue revisada el **10 de septiembre de 2026**. Databricks puede cambiar nombres o posiciones menores de la interfaz.

## Objetivo

Al terminar deberías tener:

- Una cuenta personal de Databricks Free Edition.
- Un workspace activo.
- Este repositorio clonado como Git folder.
- Un notebook conectado a compute serverless.
- Un esquema personal y un volumen `landing` en Unity Catalog.

Tiempo estimado: 20–30 minutos.

## 1. Crear la cuenta

1. Abrí la página oficial [Sign up for Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition).
2. Seleccioná la opción para registrarte en **Free Edition**, no la prueba gratuita o *Free Trial*.
3. Ingresá con una cuenta personal de Google, Microsoft o mediante el método de correo que ofrezca la pantalla.
4. Completá la verificación solicitada.
5. Esperá a que Databricks cree el workspace y muestre su página principal.

No necesitás una cuenta de AWS, Azure o GCP, ni una tarjeta de crédito. Free Edition usa infraestructura serverless administrada por Databricks.

> Free Edition reemplazó a la antigua Community Edition. Si tenías una cuenta vieja, creá o migrá a Free Edition siguiendo el flujo actual.

## 2. Reconocer el workspace

Ubicá estas secciones en la barra lateral. Los nombres normalmente aparecen en inglés:

- **Workspace:** archivos, carpetas y Git folders.
- **Catalog:** catálogos, esquemas, tablas y volúmenes de Unity Catalog.
- **Compute:** recursos disponibles; en Free Edition se utiliza serverless.
- **Jobs & Pipelines:** automatizaciones, que usaremos en clases posteriores.
- **Experiments** o **Machine Learning:** experimentos de MLflow, que usaremos en la clase 4.

No crees un clúster. Free Edition es serverless y no ofrece configuración personalizada de clústeres.

## 3. Incorporar el repositorio — camino recomendado

El repositorio público es:

```text
https://github.com/guillermohenrion/BigData2026
```

Para clonarlo como Git folder:

1. En la barra lateral, seleccioná **Workspace**.
2. Entrá en tu carpeta de usuario. Suele estar bajo `Users/<tu correo>`.
3. Seleccioná **Create** → **Git folder**.
4. En **Git repository URL**, ingresá:

   ```text
   https://github.com/guillermohenrion/BigData2026
   ```

5. En **Git provider**, elegí **GitHub**.
6. Usá `BigData2026` como nombre del Git folder.
7. No actives *sparse checkout* para esta práctica.
8. Seleccioná **Create Git folder**.
9. Esperá a que aparezcan `README.md`, `clase-1`…`clase-4` y `practicas`.

Los repositorios públicos se pueden clonar para lectura sin configurar credenciales. Para hacer `commit` o `push`, vinculá después tu cuenta desde **Settings** → **Linked accounts** → **Add Git credential**. Databricks recomienda OAuth o su aplicación de GitHub; no compartas tokens con docentes ni compañeros.

Documentación oficial: [crear y administrar Git folders](https://docs.databricks.com/aws/en/repos/git-operations-with-repos) y [conectar GitHub](https://docs.databricks.com/aws/en/repos/get-access-tokens-from-git-provider).

## 4. Abrir la práctica

Dentro del Git folder, navegá hasta:

```text
practicas/clase-01/00_setup.ipynb
```

El notebook debe mostrar celdas Markdown y Python. No ejecutes todavía los notebooks de `docente`.

Si el archivo aparece como texto JSON en lugar de notebook:

1. Verificá que su nombre termine en `.ipynb`.
2. Actualizá el navegador.
3. Si persiste, usá la alternativa de importación manual descrita más abajo.

## 5. Conectar compute serverless

1. En la parte superior derecha del notebook, abrí el selector de compute.
2. Elegí **Serverless**.
3. Si no aparece ningún recurso seleccionado, ejecutá la primera celda: los notebooks nuevos suelen conectarse automáticamente a serverless.
4. Esperá hasta que el indicador muestre que el entorno está listo.

No agregues dependencias ni selecciones memoria alta. La práctica usa el entorno estándar incluido. Consultá [serverless compute para notebooks](https://docs.databricks.com/aws/en/compute/serverless/notebooks) si la interfaz difiere.

## 6. Configurar tu espacio personal

En la parte superior de `00_setup.ipynb` aparecen dos parámetros:

- **Identificador del alumno:** usá algo como `ana_perez` o `alumno17`. Debe comenzar con una letra y contener solamente letras minúsculas, números o `_`.
- **Escala:** seleccioná `test` durante el setup inicial.

No uses tu correo completo, DNI ni otra información sensible. El identificador se incorpora al nombre del esquema.

Los modos disponibles son:

| Modo | Uso |
|---|---|
| `test` | Verificar el entorno rápidamente |
| `small` | Realizar la práctica en clase |
| `demo` | Demostración docente; consume más cuota |

## 7. Ejecutar el notebook de setup

1. Seleccioná **Run all**.
2. Si Databricks solicita confirmar la ejecución, aceptá.
3. Esperá a que todas las celdas terminen con estado correcto.
4. La salida debe mostrar valores similares a:

   ```text
   Namespace: <catalogo>.bigdata_ana_perez
   Volumen:    /Volumes/<catalogo>/bigdata_ana_perez/landing
   Filas:      {'customers': 100, ...}
   ```

5. La última consulta debe listar un volumen llamado `landing`.

El notebook detecta el catálogo actual, crea solamente tu esquema personal y crea un volumen administrado. Puede volver a ejecutarse sin borrar lo existente.

## 8. Verificar desde Catalog Explorer

1. Abrí **Catalog** en la barra lateral.
2. Seleccioná el catálogo cuyo nombre apareció en el notebook.
3. Buscá el esquema `bigdata_<tu_identificador>`.
4. Abrilo y verificá la sección **Volumes**.
5. Debe existir `landing`.

En este punto el entorno está listo. Volvé a **Workspace** y seguí con:

```text
practicas/clase-01/01_ingesta_bronze.ipynb
```

Antes de ejecutarlo, cambiá la escala a `small` y conservá exactamente el mismo identificador.

## 9. Alternativa si Git folder no funciona

Este camino sirve para continuar la clase, pero no ofrece sincronización con GitHub.

1. Descargá el repositorio desde GitHub mediante **Code** → **Download ZIP**.
2. Extraé el ZIP en tu computadora.
3. Conservá completa la estructura de `practicas`; los notebooks usan rutas relativas hacia `common`.
4. En Databricks, abrí **Workspace** y tu carpeta de usuario.
5. Creá una carpeta llamada `BigData2026`.
6. Usá el menú de tres puntos de la carpeta y seleccioná **Import**.
7. Importá un ZIP que contenga la carpeta `practicas` completa. Si tu interfaz no acepta esa estructura, arrastrá las carpetas y archivos manteniendo estas rutas:

   ```text
   practicas/
   ├── common/
   │   ├── config.py
   │   └── generate_data.py
   └── clase-01/
       ├── 00_setup.ipynb
       ├── 01_ingesta_bronze.ipynb
       └── 02_desafio.ipynb
   ```

8. Abrí `00_setup.ipynb` y continuá desde la sección 5.

Importar solamente los tres `.ipynb` no alcanza: las sentencias `%run` necesitan los archivos de `common`. Databricks admite notebooks `.ipynb` y archivos Python cuyo primer renglón es `# Databricks notebook source`. Véase [importar notebooks](https://docs.databricks.com/aws/en/notebooks/notebook-export-import).

## 10. Solución de problemas

### No aparece Serverless

- Confirmá que la cuenta sea **Free Edition** y no Community Edition.
- Actualizá la página y volvé a abrir el notebook.
- Ejecutá una celda para activar la conexión automática.
- Cerrá sesión y volvé a ingresar si el workspace acaba de ser creado.

### `%run` no encuentra `../common/config`

- Confirmá que clonaste o importaste la carpeta `practicas` completa.
- Verificá que `common` y `clase-01` sean carpetas hermanas.
- No muevas los notebooks fuera de `clase-01`.
- En la solución docente, la ruta tiene dos niveles (`../../common`) porque el notebook está dentro de `docente`.

### El identificador es inválido

Usá entre 2 y 31 caracteres. Debe comenzar con letra y continuar con letras minúsculas, números o `_`. Por ejemplo: `maria_g7`.

### No se puede crear el esquema o el volumen

Ejecutá estas consultas en una celda nueva:

```sql
%sql
SELECT current_catalog(), current_schema();
SHOW CATALOGS;
```

Guardá el mensaje de error completo y entregáselo al docente. No pruebes crear o eliminar catálogos.

### Compute deshabilitado por cuota

Free Edition aplica límites de uso. Si se supera la cuota, el compute puede quedar suspendido hasta el siguiente reinicio diario —y excepcionalmente por más tiempo— sin borrar los datos. No repitas muchas veces `count()`, no uses `demo` y no dejes streams ejecutándose. Consultá las [limitaciones de Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations).

### El repositorio no se actualiza

Abrí el diálogo **Git** desde el nombre de la rama y seleccioná **Pull**. Si tenés cambios locales sin guardar, no los descartes: pedí ayuda antes de resolver el conflicto.

## 11. Reglas de seguridad y uso

- Usá únicamente datos ficticios o públicos.
- No subas contraseñas, tokens, claves de API ni información personal.
- Nunca pegues un token dentro de un notebook.
- No ejecutes `DROP CATALOG`.
- Antes de ejecutar `DROP SCHEMA ... CASCADE`, comprobá que el nombre contenga tu identificador.
- Usá `test` para ensayar y `small` durante la clase.
- Detené cualquier stream al terminar su ejercicio.

## Checklist final

Marcá cada punto antes de comenzar `01_ingesta_bronze.ipynb`:

- [ ] Ingresé a un workspace de Databricks Free Edition.
- [ ] Veo el repositorio completo dentro de un Git folder o carpeta importada.
- [ ] Abrí `practicas/clase-01/00_setup.ipynb` como notebook.
- [ ] El notebook está conectado a Serverless.
- [ ] Usé un identificador válido y escala `test`.
- [ ] `Run all` terminó sin errores.
- [ ] Veo mi esquema `bigdata_<identificador>`.
- [ ] Veo el volumen `landing` en Catalog Explorer.
- [ ] Sé dónde cambiar la escala a `small` para comenzar la práctica.

