# Sprint 3 · Streaming completo + integración batch↔streaming

> Estado: cerrado (entrega MVP). Las cuatro preguntas operacionales (S-1..S-4) corren con productor y job propio; el job híbrido materializa la sección 4.3 de la propuesta y un dashboard Streamlit muestra todo en vivo.

## Lo que hay que correr (en una máquina nueva)

```bash
# 0. Datos reales (idempotente — sólo descarga lo que falta)
make datos-reales

# 1. Stack arriba (Sprint 0 + streaming)
make up
make pipeline-streaming-completo   # = stream-up + exportar-referencias

# 2. Lanzar 9 procesos paralelos (una terminal cada uno)
#    Productores
make stream-producer            # SIATA  (S-2)
make stream-encicla-producer    # EnCicla (S-1)
make stream-simm-producer       # SIMM    (S-3)
make stream-metro-producer      # Metro   (S-4)

#    Jobs
make stream-alert-job           # PM2.5 alertas
make stream-encicla-job         # disponibilidad sliding 1m/30s
make stream-simm-job            # corredor riesgo tumbling 5m
make stream-metro-job           # afluencia tumbling 5m
make stream-hibrido             # batch↔streaming (sección 4.3)

# 3. Dashboard
make dashboard                  # http://localhost:8501
```

## Sprint 1.5 — Datos reales (rescate antes de Sprint 3)

Antes de Sprint 3 el equipo trabajaba con sintéticos para Metro, EnCicla y SIATA. Tres descargadores nuevos resuelven los bloqueos de origen:

| Fuente | Bloqueo previo | Cómo se resolvió | Resultado |
|--------|----------------|------------------|-----------|
| Metro afluencia (xlsx) | ArcGIS Hub devolvía 403 con curl | DCAT feed del portal trae item-IDs **frescos**, item API responde con UA de navegador | **240k filas reales** (línea×día×hora 2022-2024) |
| SIATA PM2.5/PM10 | descargador requería `jq` | descargador Python que consume API Dataverse + parse `.tab` largo→CSV | **1.25M PM2.5 + 481k PM10** (2018-2024) |
| EnCicla estaciones | API CKAN cambió endpoints; nombres "Estación EnCicla 1..N" eran sintéticos | Overpass API trae 80 nodos `amenity=bicycle_rental` con nombres y coords reales | **80 estaciones reales** (Ruta N, MAMM, Plaza Botero, …) |

EnCicla **préstamos históricos** sigue sintético hasta que llegue PQRS al AMVA. Meteorología SIATA (precipitación, temperatura) sigue sintética porque los datasets están dispersos en >100 DOIs por estación.

**El batch del Sprint 1 NO se re-corrió.** El refactor de schema (Metro pasa de estación a línea, SIATA de wide a long) requiere modificar Bronze/Silver/Gold; quedó como tarea explícita de Sprint 4. Los productores de Sprint 3 leen los CSV reales DIRECTAMENTE — no necesitan que estén ingestados a Iceberg.

## Arquitectura del Sprint 3

```
                   ┌───────────────────────┐
                   │  data/raw/* (real)    │
                   │  - metro_afluencia    │
                   │  - simm_aforos        │
                   │  - siata_historico    │
                   │  - encicla_estaciones │
                   └─────────┬─────────────┘
                             │
   ┌─────────────────────────┼──────────────────────────┐
   │                         │                          │
   ▼                         ▼                          ▼
siata_producer.py    encicla_producer.py     simm_producer.py     metro_producer.py
   │                         │                          │                  │
   ▼                         ▼                          ▼                  ▼
siata.lecturas       encicla.disponibilidad     simm.aforos       metro.validaciones
(Kafka)                (Kafka)                  (Kafka)           (Kafka)
   │                         │                          │                  │
   ▼                         ▼                          ▼                  ▼
siata_alert_job   encicla_disponibilidad_job   simm_aforo_job    metro_afluencia_job
   │                         │                          │                  │
   ▼                         ▼                          ▼                  ▼
mongo:                 mongo:                     mongo:                mongo:
alertas_aire           disponibilidad_encicla     aforos_corredor       afluencia_metro_rt

                  ┌─────────────────────────────────┐
                  │  data/processed/                │
                  │  - percentiles_metro.json       │   (de afluencia real CSV)
                  │  - corredores_alta_*.json       │   (de MEData real CSV)
                  └─────────┬───────────────────────┘
                            │ bootstrap
                            ▼
                       job_hibrido.py
                            │
            consume metro.validaciones + siata.lecturas
                            │
                            ▼
                  mongo: alertas_hibridas
                            │
                            ▼
                  Streamlit dashboard (app/dashboard.py)
```

## Decisiones técnicas

### 1. Productores leen archivos reales, no la pipeline batch

Cada productor lee directamente del CSV/JSON real en `data/raw/`. **Razón:** Sprint 1.5 actualizó los datos crudos pero no se re-ingestaron a Bronze (eso es Sprint 4). Los productores son el camino más corto entre los datos reales y el stream — no hace falta que la capa Iceberg esté actualizada para que el dashboard muestre datos reales en vivo.

### 2. EnCicla disponibilidad: simulación honesta sobre estaciones reales

El AMVA no expone API pública de disponibilidad (la app móvil usa backend privado autenticado). El productor parte de las **80 estaciones reales con coordenadas reales** (OSM) y aplica un modelo Poisson + perfil horario para simular dinámica. Documentado explícitamente — no se afirma que la disponibilidad sea real.

### 3. Productores Python siguen siendo la elección

Sprint 2 dejó pendiente decidir si migrar a PyFlink. Con 4 jobs corriendo en paralelo se confirma que Python en `stream-runner` es suficiente:
- Cada job ocupa <50 MB de RAM
- Throughput: cada uno procesa cientos de eventos/segundo sin saturar
- Trade-offs aceptados: at-least-once, ventanas en buffer en memoria (las pierde en restart), no exactly-once con offsets transaccionales

ADR de cierre en `docs/decisiones/02-lambda-vs-kappa.md` (Sprint 4): justifica la decisión definitiva.

### 4. Job híbrido lee referencias de JSON, no de Iceberg en vivo

La sección 4.3 de la propuesta pide que el job consulte el percentil histórico desde Gold. Implementaciones evaluadas:
- **Live PyIceberg desde stream-runner**: requiere configurar S3 endpoint, REST URI, credenciales boto3. Funcional pero introduce dependencia compleja.
- **JSON precomputado**: `scripts/exportar_referencias_streaming.py` calcula p50/p75/p90/p95 por (línea, franja_horaria) directamente sobre los CSV reales del Metro. El job carga el JSON al arranque.

**Decisión:** JSON precomputado para el MVP del Sprint 3. La ventaja del Iceberg en vivo (p ej., snapshots auditable, time travel) la queremos en Sprint 4 cuando se haga el refactor de Bronze/Silver/Gold con la afluencia real. Mientras tanto el JSON aporta el mismo valor demostrativo y mantiene el job ligero.

### 5. Identificar corredores de alta siniestralidad desde MEData real

El job SIMM (S-3) emite alerta sólo si el corredor está en la lista de "alta siniestralidad". La lista se deriva en tiempo de bootstrap del CSV REAL de MEData (`incidentes_viales.csv`):
1. Score por comuna = 5 × muertos + 1 × heridos + 0.1 × daños (estándar OMS-like)
2. Top-8 comunas por score
3. Vías de mayor recurrencia en cada comuna → patrón regex extrae "Carrera N", "Calle N", "Avenida X"
4. Unión con corredores canónicos del SIMM (Carrera 70, Calle Colombia, Av. Las Vegas, etc.)

Resultado: **51 corredores** marcados como alta siniestralidad, sobre **257k incidentes reales analizados**.

### 6. Decisión de granularidad temporal en cada job

| Job | Tipo ventana | Tamaño | Paso | Justificación |
|-----|--------------|--------|------|---------------|
| EnCicla S-1 | sliding | 1 min | 30 s | Disponibilidad cambia rápido, dashboard refresca cada 5 s |
| SIATA S-2 | tumbling | 10 min | — | Sensores SIATA reportan cada 10 min real; matchea cadencia natural |
| SIMM S-3 | tumbling | 5 min | — | Conteos vehiculares se interpretan mejor agregados; aforo de minuto-a-minuto es ruidoso |
| Metro S-4 | tumbling | 5 min | — | Demanda Metro varía suavemente; 5 min es punto medio entre granularidad y suavizado |

## Esquemas

### Tópicos Kafka (todos JSON, key = entidad)

```jsonc
// siata.lecturas (Sprint 2)
{ "estacion_id": "MED-UNNV", "zona": "...", "pm25": 45.3, ... }

// encicla.disponibilidad
{ "estacion_id": "ENC042", "nombre": "Estación EnCicla Ruta N",
  "bicicletas_disponibles": 14, "anclajes_libres": 12, ... }

// simm.aforos
{ "corredor": "Carrera 70", "intensidad": 11, "velocidad_kmh": 24.0,
  "ocupacion_pct": 13, ... }

// metro.validaciones
{ "linea": "LÍNEA A", "pasajeros_periodo": 124, "hora": 7, ... }
```

### Colecciones Mongo

```
mongodb.pulsomed.alertas_aire             (S-2)
mongodb.pulsomed.disponibilidad_encicla   (S-1)
mongodb.pulsomed.aforos_corredor          (S-3)
mongodb.pulsomed.afluencia_metro_rt       (S-4)
mongodb.pulsomed.alertas_hibridas         (4.3, batch↔stream)
```

Todas con índice único `{entidad, ventana_inicio}` que protege contra reproceso.

## Verificación rápida

Tras lanzar el stack y los productores/jobs durante 2 min:

```bash
docker compose exec mongodb mongosh -u admin -p admin12345 --authenticationDatabase admin pulsomed --eval '
  printjson({
    aire:    db.alertas_aire.countDocuments({}),
    encicla: db.disponibilidad_encicla.countDocuments({}),
    simm:    db.aforos_corredor.countDocuments({}),
    metro:   db.afluencia_metro_rt.countDocuments({}),
    hibrida: db.alertas_hibridas.countDocuments({}),
  })
'
```

Esperado (con productores a INTERVALO_S=0.5 y dejando correr 60 s):

```
{
  aire:     ≥ 5,
  encicla: ≥ 60   (80 estaciones × varios snapshots),
  simm:     ≥ 30,
  metro:    ≥ 12,
  hibrida:  ≥ 1   (cuando una lectura SIATA con precip>0.3 cruza con afluencia baja)
}
```

## Pendiente para Sprint 4

- [ ] **ADR Lambda vs Kappa** (Módulo 02): documentar formalmente que el sistema es Lambda. Ya tenemos los datos para sostener el argumento.
- [ ] **ADR Delta vs Iceberg** (Módulo 05): comparar interoperabilidad multi-motor.
- [ ] **MapReduce legacy** (Módulo 01): job Hadoop sobre los CSV de incidentes pre/post-2017.
- [ ] **Refactor Bronze/Silver/Gold** para incorporar los datasets reales de Sprint 1.5: cambio de schema Metro (estación→línea), SIATA (wide→long con pivot interno), reorganización de meteorología.
- [ ] **Job híbrido sobre Iceberg en vivo**: migrar de JSON precomputado a consulta `pyiceberg.catalog.load_catalog(...).load_table(...)` para demostrar el valor del REST Catalog.

## Archivos nuevos en este sprint

```
scripts/
  descargar_metro_afluencia_real.py     # ArcGIS DCAT + xlsx → CSV largo
  descargar_siata_real.py               # Dataverse Python (sin jq)
  descargar_encicla_estaciones.py       # Overpass API → JSON
  exportar_referencias_streaming.py     # CSV reales → JSONs de referencia

src/streaming/producers/
  encicla_producer.py                   # 80 estaciones reales + simulación
  simm_producer.py                      # replay de simm_traffic_data.csv real
  metro_producer.py                     # micro-eventos desde afluencia hora×línea

src/streaming/flink_jobs/
  encicla_disponibilidad_job.py         # sliding 1m/30s + alerta bicis ≤ 2
  simm_aforo_job.py                     # tumbling 5m + alerta corredor riesgo
  metro_afluencia_job.py                # tumbling 5m
  job_hibrido.py                        # 4.3: batch↔stream, percentiles + lluvia

app/
  dashboard.py                          # Streamlit con 5 paneles, refresh 5s

docs/sprints/
  sprint-3-streaming-completo.md        # este archivo

data/processed/
  percentiles_metro.json                # generado, real-data derived
  corredores_alta_siniestralidad.json   # generado, MEData real
```
