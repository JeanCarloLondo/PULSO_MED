# Sprint 7 · BI con Apache Superset (informe + demo final)

> **Estado:** ✅ cerrado · 2026-05-16
> **Objetivo:** entregar un informe BI consumible en una herramienta
> profesional (no en un PDF ni en notebooks), que sirva como guion vivo
> para la demo final frente al jurado.

---

## Decisión: por qué Apache Superset

El profesor dejó libre la elección de la herramienta BI. Evaluamos cuatro
candidatas:

| Opción | Pros | Contras | Veredicto |
|--------|------|---------|-----------|
| **Apache Superset** ✅ | Open source, motor SQL completo, **driver Trino nativo** (lee Iceberg sin reexportar), corre en Docker en el mismo stack, dashboards profesionales | Setup inicial 2 min más largo que Metabase | **Elegida** |
| Metabase | UI más amigable, setup de 5 min | SQL Lab más limitado, menos potente para queries multi-tabla | descartada |
| Power BI Desktop | Aspecto "corporativo", familiar | No integra al stack Docker, requiere exportar Gold a CSV/Parquet, no es reproducible | descartada |
| Extender el Streamlit existente | Cero infraestructura nueva | Streamlit es app framework, no BI — incumple el espíritu del entregable | descartada |

La razón decisiva fue **Trino → Iceberg → Gold sin reexportar nada**:
Superset consulta las mismas tablas Iceberg que ya construyó el pipeline
batch, vía Trino (bonus +2pt del Sprint 5). Cero duplicación de datos.

Decisión firmada en `data/processed/bi/decisiones.csv`, fila `S7-BI`.

---

## Qué entrega este sprint

### 1. Servicio Superset integrado al `docker-compose.yml`

```yaml
superset:
  build:
    context: ./docker/superset
  image: pulsomed/superset:4.0
  ports:
    - "${SUPERSET_PORT:-8088}:8088"
  depends_on:
    trino: { condition: service_started }
  ...
```

Imagen custom (`docker/superset/Dockerfile`) que extiende
`apache/superset:4.0.2` con:

- `sqlalchemy-trino` — driver SQLAlchemy para Trino.
- `trino[sqlalchemy]` — cliente Python oficial.
- `pymongo` — opcional, por si un tablero futuro lee alertas streaming.

### 2. Metadatos del proyecto materializados (4 datasets BI)

`scripts/generar_metadatos_bi.py` genera 4 CSVs + 1 SQLite en
`data/processed/bi/`:

| Dataset | Filas | Qué contiene |
|---------|-------|--------------|
| `hallazgos.csv` | 12 | Preguntas batch B-1..B-4, streaming S-1..S-4, híbrida 4.3, ML, Grafo, demo Iceberg |
| `decisiones.csv` | 8 | ADRs 02/04/05/07 + 3 decisiones del Sprint 6 cumplimiento + decisión Sprint 7 BI |
| `herramientas.csv` | 16 | Cada herramienta del stack con su capa, razón de uso y alternativa descartada |
| `cumplimiento_rubrica.csv` | 13 | Checklist contra `docs/Proyecto_Final_ST1630.pdf` con sección, puntos, estado y evidencia |
| `pulsomed_bi.db` | — | SQLite con las 4 tablas anteriores, montado en Superset en `/app/bi-data/` |

Los datos vienen directamente de los `.md` del proyecto (ADRs + sprints +
README + propuesta) — son la **fuente única de verdad** del informe.

### 3. Bootstrap automatizado

`scripts/bootstrap_superset.sh`:

1. Espera al `/health` de Superset (hasta 60 reintentos).
2. Crea el usuario admin (`admin/admin`) — idempotente.
3. Hace login vía REST API y obtiene access token.
4. Registra dos datasources:
   - `trino_iceberg` → `trino://trino@trino:8080/iceberg` (lakehouse Gold)
   - `pulsomed_bi_meta` → `sqlite:////app/bi-data/pulsomed_bi.db` (metadatos)
5. Lista los datasources registrados.

### 4. Comandos Makefile

```bash
make bi-metadatos        # regenera los 4 CSVs + SQLite (host, sin Docker)
make bi-up               # build + up del servicio Superset
make bi-init             # bootstrap: admin + datasources
make bi-logs             # tail de logs
make bi-down             # stop (preserva metadata + datasources)
```

---

## Ruta corta — la demo en 4 comandos

Asumiendo el stack ya está levantado (`make all`):

```bash
make trino-up            # si no estaba arriba
make bi-up               # construye imagen Superset (1ª vez ~3 min) y arranca
# esperar a que Superset termine `superset init` (~60s)
make bi-init             # crea admin + registra Trino + SQLite
# abrir http://localhost:8088   ·   admin / admin
```

---

## Guion de la demo (15–20 min)

La presentación se estructura como un recorrido por 4 tableros, uno por
cada eje pedido por el profesor: **descubrimientos**, **decisiones**,
**herramientas usadas y la razón de su uso**, y **cumplimiento de la
rúbrica**. Cada tablero usa uno de los datasets en `pulsomed_bi_meta`,
salvo el último que cruza Trino y metadatos.

### Tablero 1 · Hallazgos de datos (4 min)

**Fuente:** `pulsomed_bi_meta.hallazgos` + `trino_iceberg.iceberg."pulsomed.gold".*`

Gráficos sugeridos:

- **Tabla** de hallazgos B-1..B-4 + S-1..S-4 (12 filas) — columna `pregunta` + `hallazgo`.
- **Big number** "12 preguntas respondidas" (count distinct id).
- **Bar chart** por `categoria` (Batch / Streaming / Híbrido / ML / Grafo).
- **SQL Lab live**: ejecutar la consulta del README sobre Trino:
  ```sql
  SELECT comuna,
         SUM(con_muertos)  AS total_muertos,
         SUM(con_heridos)  AS total_heridos,
         AVG(indice_severidad) AS severidad_media
  FROM iceberg."pulsomed.gold".accidentalidad_por_comuna
  GROUP BY comuna
  ORDER BY total_muertos DESC
  LIMIT 5;
  ```
- **Tabla** de ranking PageRank Metro (rúbrica gold.red_metro_pagerank).

**Lo que se dice:** "Estos son los descubrimientos concretos del proyecto.
12 preguntas — 4 batch, 4 streaming, 1 híbrida, 1 ML, 1 grafo, 1 demo de
features Iceberg. Cada fila apunta a la tabla Gold o colección Mongo que
materializa la respuesta. Y mostramos en vivo que Superset consulta vía
Trino las mismas tablas Iceberg que produjo Spark."

### Tablero 2 · Decisiones técnicas (4 min)

**Fuente:** `pulsomed_bi_meta.decisiones`

Gráficos:

- **Tabla** completa con `id`, `titulo`, `decision`, `alternativa_descartada`,
  `motivacion`.
- **Big number** "4 ADRs firmados + 4 decisiones de cumplimiento de rúbrica".
- **Pie chart** por `estado` (Aceptado vs revisado).
- **Hyperlinks** a los `.md` de cada ADR en `docs/decisiones/`.

**Lo que se dice:** "Cada decisión técnica grande tiene un ADR firmado.
Aquí están los 4: Lambda vs Kappa (módulo 02), Benchmark de formatos
(módulo 04), Delta vs Iceberg (módulo 05), Cloud + Gobernanza (módulo 07).
Además, el Sprint 6 firmó 3 decisiones nuevas para cerrar gaps con la
rúbrica oficial, y el Sprint 7 firmó la decisión de usar Superset como BI."

### Tablero 3 · Herramientas y por qué (4 min)

**Fuente:** `pulsomed_bi_meta.herramientas`

Gráficos:

- **Tabla** con `herramienta`, `capa`, `razon_uso`, `alternativa_descartada`.
- **Bar chart** por `capa` (Infraestructura, Lakehouse, Compute, Streaming,
  Serving, Consumo, ML).
- **Bubble chart** o **treemap** por `sprint_introducido` para mostrar
  cómo creció el stack sprint a sprint (0 → 7).
- **Timeline** mostrando la introducción cronológica.

**Lo que se dice:** "16 herramientas, una por una con la razón concreta
de uso. Stack en capas: storage es MinIO, table format Iceberg, compute
batch Spark + Trino, streaming Kafka + Flink + stream-runner Python,
serving operacional MongoDB + Streamlit, serving analítico Trino + esta
herramienta BI. Cada elección tiene una alternativa que se descartó —
documentamos el porqué para que la decisión sea auditable."

### Tablero 4 · Cumplimiento de la rúbrica (4 min)

**Fuente:** `pulsomed_bi_meta.cumplimiento_rubrica`

Gráficos:

- **Big numbers**: total de puntos cubiertos / 100 + bonus +5.
- **Tabla** completa con `seccion`, `titulo`, `puntos`, `estado`, `evidencia`.
- **Bar chart** apilada por `estado` (Cumplido vs Listo).
- **Bullet chart** por sección §4.1..§4.9 + Bonus.

**Lo que se dice:** "Cruzamos cada § de la rúbrica oficial contra el
proyecto. 100/100 cumplido + 5 puntos de bonus. La columna `evidencia`
apunta al archivo o comando exacto que materializa el cumplimiento.
Cualquier evaluador puede reproducirlo con `make` desde el README."

### Cierre (3 min)

- Abrir SQL Lab y ejecutar 1-2 queries libres sobre `iceberg.pulsomed.gold`
  para demostrar que **no es un mock**: el BI consulta los datos reales.
- Mostrar `make help` y los **5 nuevos targets** `bi-*`.
- Recordar que el bootstrap es idempotente — `make bi-init` puede correrse
  N veces sin romper nada.

---

## Tableros opcionales para el bonus

Si sobra tiempo en la demo, se pueden levantar 2 tableros más sobre Gold
real (vía Trino):

### Tablero 5 — Accidentalidad por comuna (B-2)

```sql
SELECT comuna, anio,
       SUM(con_muertos)   AS muertos,
       SUM(con_heridos)   AS heridos,
       AVG(indice_severidad) AS severidad_media
FROM iceberg."pulsomed.gold".accidentalidad_por_comuna
GROUP BY comuna, anio
ORDER BY anio, severidad_media DESC;
```

Visualizaciones: heatmap comuna × año, bar race, choropleth si hay
geometrías.

### Tablero 6 — Red Metro y PageRank (G-1)

```sql
SELECT ranking, nombre, linea, ROUND(pagerank, 6) AS pagerank
FROM iceberg."pulsomed.gold".red_metro_pagerank
ORDER BY ranking;
```

Visualizaciones: tabla ranking + bar chart por línea.

---

## Estructura de archivos del sprint

```
docker/superset/
  Dockerfile                              imagen custom (Superset 4.0 + Trino driver + pymongo)
  superset_config.py                      config: SECRET_KEY, feature flags, sqlite metastore

scripts/
  generar_metadatos_bi.py                 genera 4 CSVs + SQLite con metadatos del proyecto
  bootstrap_superset.sh                   crea admin + registra datasources (Trino + SQLite)

data/processed/bi/                        gitignored — output del generador
  hallazgos.csv
  decisiones.csv
  herramientas.csv
  cumplimiento_rubrica.csv
  pulsomed_bi.db

Makefile                                  +5 targets bi-*
docker-compose.yml                        +1 servicio (superset) + 1 volume (superset-home)
.env.example                              +2 vars (SUPERSET_PORT, SUPERSET_SECRET_KEY)
docs/sprints/sprint-7-bi-superset.md      este archivo
```

---

## Troubleshooting

| Síntoma | Causa probable | Solución |
|---------|----------------|----------|
| `make bi-up` falla con "build context error" | Docker Desktop no está arriba | Arrancar Docker Desktop y reintentar |
| Superset no responde en :8088 después de 2 min | `superset db upgrade` toma tiempo la 1ª vez | Esperar 1 min más y `make bi-logs` para confirmar progreso |
| `make bi-init` retorna "connection refused" | El contenedor todavía está inicializando | Esperar 30s más y reintentar — idempotente |
| Datasource Trino aparece pero no lista tablas | Trino no expone `iceberg.pulsomed.gold` aún | Verificar `make trino-demo` primero; el pipeline batch debe haber corrido |
| Datasource SQLite aparece vacío | `data/processed/bi/pulsomed_bi.db` no fue regenerado | `make bi-metadatos` antes de `make bi-init` |
| Quiero resetear todo | borrar volume `superset-home` | `docker compose down -v superset` + `make bi-up` |

---

## Conclusión

Sprint 7 cierra el proyecto entregando el informe BI en una herramienta
profesional, reproducible con un solo comando, sin reexportar datos.
Quedan listos los 4 tableros del guion (hallazgos, decisiones,
herramientas, cumplimiento) y el ground truth para construirlos vive en
`data/processed/bi/`. La demo puede correrse end-to-end desde un clon
limpio del repo con:

```bash
cp .env.example .env
make all && make bi-up && make bi-init
```
