# Sprint 2 · Streaming MVP — Pregunta S-2 (Alerta PM2.5)

> Estado: cerrado. MVP `make pipeline-streaming` levanta el stack streaming. Una corrida de productor + alert-job genera alertas en `mongodb.pulsomed.alertas_aire` consultables vía CLI.

## Lo que hay que correr

```bash
# 1. Levantar Zookeeper + Kafka + stream-runner
make stream-up

# 2. Job de alertas (terminal A — bloquea, escucha el tópico)
make stream-alert-job VENTANA_MINUTOS=1 UMBRAL_PM25=75

# 3. Productor SIATA (terminal B — emite ~120 eventos en 6 segundos
#    con un pico de PM2.5 cada 5 eventos, suficiente para una demo)
INTERVALO_S=0.05 INYECTAR_PICO_CADA=5 LIMITE_EVENTOS=120 \
  make stream-producer

# 4. Consultar alertas (terminal C)
make stream-alertas ULTIMAS=10min
```

Salida esperada del CLI:

```
ventana                 zona                          gravedad    pm25_avg  lect.
--------------------------------------------------------------------------------
2026-05-08 03:32        valle_aburra_centro           moderada      95.0     10
2026-05-08 03:32        valle_aburra_nororiental      moderada      95.0     10
```

## Arquitectura del Sprint 2

```
SIATA CSV histórico (Bronze sintético compartido con Sprint 1)
        ↓
siata_producer.py (Python, kafka-python)
        ↓ JSON, key=estacion_id
Kafka tópico `siata.lecturas`  (1 partición, retention default)
        ↓
siata_alert_job.py (consumidor + ventana tumbling en Python)
        ↓ Mongo insertOne
mongodb.pulsomed.alertas_aire   (índice único {zona, ventana_inicio})
        ↓
scripts/consultar_alertas.py    (CLI)
```

## Decisión técnica clave: Python en vez de PyFlink

**Decisión:** el job de procesamiento del Sprint 2 es un consumidor Kafka en Python que implementa la ventana tumbling manualmente, no PyFlink ni Flink Java/Scala.

**Por qué:**
- PyFlink requiere `flink-sql-connector-kafka` compatible con la versión exacta de Flink, descargar jars a `/opt/flink/lib`, y un build de la imagen. Para una sola pregunta (S-2) y una sola ventana es desproporcionado.
- El primitivo (ventana tumbling sobre clave + agregación + sink) es trivial en 100 líneas de Python sin perder semántica.
- Migrar a Flink real está planeado para Sprint 3, cuando haya 4 jobs paralelos (S-1, S-2, S-3, S-4) compitiendo por throughput y haya argumento real de elegir un engine streaming.

**Trade-off aceptado:**
- Sin gestión de estado distribuido. Las ventanas viven en memoria del proceso Python.
- Sin exactly-once. Es at-least-once por construcción (acks=1, auto-commit).
- Si el proceso muere, las ventanas en buffer se pierden.

Esto se documenta como ADR pendiente de aprobar al cierre del Sprint 3 (cuando se decida si todos los jobs siguen en Python o se migran a Flink).

## Esquemas

### Tópico `siata.lecturas`

Mensaje JSON, key = `estacion_id`:

```json
{
  "estacion_id": "ESP_AEROPUERTO",
  "estacion_nombre": "Aeropuerto",
  "zona": "valle_aburra_norte",
  "latitud": 6.22,
  "longitud": -75.59,
  "timestamp": "2023-05-15T14:30:00",
  "pm25": 45.3,
  "pm10": 78.2,
  "temperatura_c": 24.5,
  "humedad_pct": 65,
  "precipitacion_mm": 0.0,
  "viento_kmh": 5.2
}
```

### Colección `mongodb.pulsomed.alertas_aire`

```json
{
  "_id": ObjectId("..."),
  "zona": "valle_aburra_centro",
  "ventana_inicio": ISODate("2026-05-08T03:32:00Z"),
  "ventana_fin":    ISODate("2026-05-08T03:33:00Z"),
  "pm25_promedio": 95.0,
  "lecturas_en_ventana": 10,
  "tipo": "ALERTA_PM25",
  "gravedad": "moderada",   // leve | moderada | critica
  "umbral": 75.0,
  "emitido_en": ISODate("2026-05-08T03:33:05Z")
}
```

Índice único: `{zona: 1, ventana_inicio: 1}` — protege contra duplicados en re-procesos.

## Cómo se cierran las ventanas

El consumidor mantiene un buffer `{(zona, ventana_inicio): [pm25, ...]}`. Tras cada lote del consumer (y tras cada timeout de 2s sin mensajes nuevos):

```
cierre = max(event_time_max, wall_clock_now) - VENTANA_MINUTOS
```

Toda ventana con `ventana_inicio < cierre` se cierra: si `avg(pm25) > UMBRAL_PM25`, se inserta una alerta en Mongo.

El uso de `max(event_time, wall_clock)` evita que el productor histórico (que avanza event-time) y el productor en pausa (que no avanza nada) dejen ventanas huérfanas para siempre.

## Asignación de zona

El productor lee la columna `zona` del CSV de SIATA. Las 10 estaciones sintéticas se reparten en 5 zonas:

- `valle_aburra_centro` (Belén, Centro, Laureles)
- `valle_aburra_norte` (Aeropuerto, Bello)
- `valle_aburra_sur` (El Poblado, Itagüí, Caldas)
- `valle_aburra_nororiental` (Manrique, Aranjuez)

(Las estaciones reales de SIATA son ~40; cuando se reemplace por el dataset oficial, hay que recalcular esta asignación con un join espacial contra `bronze.geomedellin_comunas`.)

## Qué archivos nuevos hay

```
docker-compose.yml           # zookeeper, kafka, stream-runner descomentados
src/streaming/producers/siata_producer.py    # productor con pico inyectable
src/streaming/flink_jobs/siata_alert_job.py  # consumidor + ventana tumbling
scripts/consultar_alertas.py # CLI de Mongo
docs/sprints/sprint-2-streaming.md            # este documento
```

`stream-runner` es un contenedor `python:3.11-slim` que en boot hace `pip install kafka-python pymongo`. No carga PyFlink — eso es Sprint 3.

## Pendiente para Sprint 3

- [ ] 3 productores más: Metro (validaciones), EnCicla (disponibilidad), SIMM (aforos)
- [ ] 3 jobs equivalentes en otras 3 colecciones Mongo
- [ ] **Job híbrido S-3/S-4:** lee al arranque el percentil 90 histórico desde Gold (Iceberg), cachea, y emite alerta cuando RT cae por debajo del umbral histórico para esa franja con esa lluvia
- [ ] ADR final: ¿migramos a Flink real o nos quedamos con Python? (Decisión informada por el rendimiento de los 4 jobs corriendo en paralelo)
