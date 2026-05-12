# ----------------------------------------------------------------------
# Makefile · Pulso Medellín (v0.2)
#
# Comandos disponibles:
#   make up           - Levantar todos los servicios (Sprint 0)
#   make down         - Apagar servicios (conserva volúmenes)
#   make clean        - Apagar Y borrar volúmenes (¡pierde datos!)
#   make ps           - Estado de los contenedores
#   make logs         - Ver logs (todos)  ó  make logs SERVICE=minio
#   make smoke        - Correr smoke tests del Sprint 0
#   make shell        - Shell dentro del contenedor de Spark
#   make pyspark      - Abrir pyspark interactivo
#   make jupyter      - Mostrar URL de Jupyter
#   make help         - Esta ayuda
# ----------------------------------------------------------------------

.DEFAULT_GOAL := help
SHELL := /bin/bash
COMPOSE := docker compose
COMPOSE_EXEC    := MSYS_NO_PATHCONV=1 $(COMPOSE) exec
SERVICE ?=
# Python en el host (puede ser python3 en Linux sin alias)
PYTHON_HOST := $(shell command -v python3 2>/dev/null || command -v python 2>/dev/null || echo python3)

ENV_CHECK := @if [ ! -f .env ]; then \
	echo "ERROR: archivo .env no encontrado. Copia .env.example: cp .env.example .env"; \
	exit 1; \
fi

.PHONY: help up down clean ps logs smoke shell pyspark jupyter \
        smoke-minio smoke-iceberg smoke-mongo \
        env-check rebuild wait-stack \
        ml-fatalidad grafo-metro trino-up trino-sql trino-demo \
        pipeline-sprint5 all

help: ## Mostrar este mensaje de ayuda
	@echo "Pulso Medellín — comandos disponibles:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""

env-check: ## Verificar que .env existe
	$(ENV_CHECK)

up: env-check ## Levantar todos los servicios y esperar healthchecks
	$(COMPOSE) up -d
	@echo ""
	@echo "Estado actual de los contenedores:"
	@$(COMPOSE) ps
	@echo ""
	@echo "URLs útiles:"
	@echo "  MinIO Console : http://localhost:9001"
	@echo "  Iceberg REST  : http://localhost:8181/v1/config"
	@echo "  Spark UI      : http://localhost:8080  (visible cuando hay un job corriendo)"
	@echo "  Jupyter Lab   : http://localhost:8888"
	@echo "  MongoDB       : mongodb://localhost:27017"
	@echo ""
	@echo "Si algún contenedor aparece como 'Restarting' o 'unhealthy',"
	@echo "espera ~30s y corre 'make ps' otra vez. Spark suele tardar."
	@echo ""
	@echo "Cuando todo esté Up, valida con:  make smoke"

down: ## Apagar todos los servicios (conserva volúmenes)
	$(COMPOSE) down

clean: ## Apagar Y borrar volúmenes (¡destructivo!)
	@echo "⚠️  Esto borrará todos los datos persistidos en MinIO y MongoDB."
	@read -p "¿Continuar? [y/N] " ans && [ "$$ans" = "y" ]
	$(COMPOSE) down -v
	@echo "Limpieza completa."

ps: ## Mostrar estado de los contenedores
	$(COMPOSE) ps

logs: ## Ver logs (use SERVICE=<nombre> para uno específico)
	@if [ -z "$(SERVICE)" ]; then \
		$(COMPOSE) logs -f --tail=100; \
	else \
		$(COMPOSE) logs -f --tail=100 $(SERVICE); \
	fi

rebuild: ## Reconstruir imágenes locales
	$(COMPOSE) build --no-cache

# ---------- Smoke tests --------------------------------------

smoke: smoke-minio smoke-iceberg smoke-mongo ## Correr los 3 smoke tests del Sprint 0
	@echo ""
	@echo "✅ Sprint 0 smoke tests pasaron. El stack está sano."

smoke-minio: env-check ## Smoke MinIO + REST Catalog
	@echo "→ smoke MinIO + REST Catalog..."
	@$(COMPOSE_EXEC) -T spark-iceberg python /workspace/tests/smoke/test_minio.py

smoke-iceberg: env-check ## Smoke Iceberg (crear/leer/dropear tabla)
	@echo "→ smoke Iceberg..."
	@$(COMPOSE_EXEC) -T spark-iceberg python /workspace/tests/smoke/test_iceberg.py

smoke-mongo: env-check ## Smoke MongoDB
	@echo "→ smoke MongoDB..."
	@$(COMPOSE_EXEC) -T mongodb sh -lc 'mongosh --quiet -u "$$MONGO_INITDB_ROOT_USERNAME" -p "$$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin /workspace/tests/smoke/test_mongodb.js'

# ---------- Conveniencia --------------------------------------

shell: ## Abrir bash dentro del contenedor de Spark
	$(COMPOSE) exec spark-iceberg /bin/bash

pyspark: ## Abrir pyspark interactivo
	$(COMPOSE) exec spark-iceberg pyspark

jupyter: ## Mostrar URL de Jupyter
	@echo "Jupyter Lab: http://localhost:8888 (sin token en la imagen tabulario/spark-iceberg)"

# ---------- Sprint 1 — datos ----------------------------------

download-data: ## [Sprint 1] Descargar los 6 datasets crudos a data/raw/
	@bash scripts/download_datasets.sh

# ---------- Sprint 1 — setup ----------------------------------

init-namespaces: env-check ## [Sprint 1] Crear namespaces pulsomed.bronze/silver/gold en Iceberg
	@echo "→ Creando namespaces Medallion en el catálogo Iceberg..."
	@$(COMPOSE_EXEC) -T spark-iceberg python /workspace/scripts/init_iceberg_namespaces.py

# ---------- Sprint 1 — Bronze ---------------------------------

ingest-bronze-geomedellin: env-check ## [Sprint 1] Ingestar GeoMedellín → Bronze
	@$(COMPOSE_EXEC) -T spark-iceberg python /workspace/src/batch/bronze/ingest_geomedellin.py

ingest-bronze-simm: env-check ## [Sprint 1] Ingestar SIMM → Bronze
	@$(COMPOSE_EXEC) -T spark-iceberg python /workspace/src/batch/bronze/ingest_simm.py

ingest-bronze-siata: env-check ## [Sprint 1] Ingestar SIATA histórico → Bronze
	@$(COMPOSE_EXEC) -T spark-iceberg python /workspace/src/batch/bronze/ingest_siata.py

ingest-bronze-medata: env-check ## [Sprint 1] Ingestar MEData incidentes → Bronze
	@$(COMPOSE_EXEC) -T spark-iceberg python /workspace/src/batch/bronze/ingest_medata.py

ingest-bronze-metro: env-check ## [Sprint 1] Ingestar Metro afluencia → Bronze
	@$(COMPOSE_EXEC) -T spark-iceberg python /workspace/src/batch/bronze/ingest_metro.py

ingest-bronze-encicla: env-check ## [Sprint 1] Ingestar EnCicla → Bronze (aplica HMAC)
	@$(COMPOSE_EXEC) -T spark-iceberg python /workspace/src/batch/bronze/ingest_encicla.py

ingest-bronze-all: env-check ## [Sprint 1] Ingestar todas las fuentes → Bronze en orden
	$(MAKE) ingest-bronze-geomedellin
	$(MAKE) ingest-bronze-simm
	$(MAKE) ingest-bronze-siata
	$(MAKE) ingest-bronze-medata
	$(MAKE) ingest-bronze-metro
	$(MAKE) ingest-bronze-encicla

# ---------- Sprint 1 — Silver ---------------------------------

transform-silver: env-check ## [Sprint 1] Transformar Bronze → Silver (todas las fuentes)
	@$(COMPOSE_EXEC) -T spark-iceberg python /workspace/src/batch/silver/transform_all.py

# ---------- Sprint 1 — Gold -----------------------------------

build-gold: env-check ## [Sprint 1] Construir capa Gold (preguntas B-1..B-4)
	@$(COMPOSE_EXEC) -T spark-iceberg python /workspace/src/batch/gold/build_all.py

# ---------- Sprint 1 — Pipeline completo ----------------------

pipeline-batch: init-namespaces ingest-bronze-all transform-silver build-gold ## [Sprint 1] Pipeline completo Bronze → Silver → Gold
	@echo ""
	@echo "✅ Pipeline batch completo. Capa Gold lista para consultas."

# ---------- Sprint 1 — Datos sintéticos (cuando descarga real falla) -----

generate-samples: ## [Sprint 1] Generar muestras sintéticas para Metro/SIATA/EnCicla
	@PYTHONIOENCODING=utf-8 $(PYTHON_HOST) scripts/generar_muestras_sinteticas.py

# ---------- Sprint 2 — Streaming MVP --------------------------

stream-up: ## [Sprint 2] Levantar Zookeeper + Kafka + stream-runner
	$(COMPOSE) up -d zookeeper kafka stream-runner
	@echo "→ Esperando a que stream-runner instale dependencias..."
	@until $(COMPOSE) exec -T stream-runner python -c "import kafka, pymongo" 2>/dev/null; do sleep 2; done
	@echo "✅ Streaming stack listo."

stream-alert-job: env-check ## [Sprint 2] Correr el job Flink-like de alertas (foreground)
	$(COMPOSE_EXEC) -e VENTANA_MINUTOS=$${VENTANA_MINUTOS:-10} -e UMBRAL_PM25=$${UMBRAL_PM25:-75} \
		stream-runner python -u /workspace/src/streaming/flink_jobs/siata_alert_job.py

stream-producer: env-check ## [Sprint 2] Correr el productor SIATA (foreground)
	$(COMPOSE_EXEC) -e INTERVALO_S=$${INTERVALO_S:-1} -e INYECTAR_PICO_CADA=$${INYECTAR_PICO_CADA:-30} \
		stream-runner python -u /workspace/src/streaming/producers/siata_producer.py

stream-alertas: env-check ## [Sprint 2] Consultar últimas alertas en MongoDB
	$(COMPOSE_EXEC) stream-runner python /workspace/scripts/consultar_alertas.py --ultimas $${ULTIMAS:-1h}

pipeline-streaming: stream-up ## [Sprint 2] Stack streaming completo arriba
	@echo ""
	@echo "Para correr el demo S-2 (alerta PM2.5):"
	@echo "  Terminal 1:  make stream-alert-job"
	@echo "  Terminal 2:  make stream-producer"
	@echo "  Terminal 3:  make stream-alertas ULTIMAS=10min"

# ---------- Sprint 1.5 — datos reales -------------------------

datos-reales: ## [Sprint 1.5] Descargar datos reales: Metro xlsx + SIATA Dataverse + EnCicla OSM
	@echo "→ Metro afluencia (xlsx oficial Metro de Medellín)..."
	@PYTHONIOENCODING=utf-8 $(PYTHON_HOST) scripts/descargar_metro_afluencia_real.py
	@echo ""
	@echo "→ SIATA PM2.5 + PM10 (Dataverse oficial)..."
	@PYTHONIOENCODING=utf-8 $(PYTHON_HOST) scripts/descargar_siata_real.py
	@echo ""
	@echo "→ EnCicla estaciones (OpenStreetMap Overpass)..."
	@PYTHONIOENCODING=utf-8 $(PYTHON_HOST) scripts/descargar_encicla_estaciones.py
	@echo ""
	@echo "✅ Datos reales en data/raw/. Próximo paso: make exportar-referencias"

exportar-referencias: ## [Sprint 3] Pre-computar percentiles Metro + corredores riesgo desde CSV reales
	@PYTHONIOENCODING=utf-8 $(PYTHON_HOST) scripts/exportar_referencias_streaming.py

# ---------- Sprint 3 — Streaming completo ---------------------

stream-encicla-producer: env-check ## [Sprint 3] Productor EnCicla disponibilidad (S-1)
	$(COMPOSE_EXEC) -e INTERVALO_S=$${INTERVALO_S:-1.0} -e INYECTAR_PICO_CADA=$${INYECTAR_PICO_CADA:-20} \
		stream-runner python -u /workspace/src/streaming/producers/encicla_producer.py

stream-encicla-job: env-check ## [Sprint 3] Job ventana sliding S-1
	$(COMPOSE_EXEC) -e VENTANA_MINUTOS=$${VENTANA_MINUTOS:-1} -e PASO_SEGUNDOS=$${PASO_SEGUNDOS:-30} \
		-e UMBRAL_BICIS=$${UMBRAL_BICIS:-2} \
		stream-runner python -u /workspace/src/streaming/flink_jobs/encicla_disponibilidad_job.py

stream-simm-producer: env-check ## [Sprint 3] Productor SIMM aforos (S-3)
	$(COMPOSE_EXEC) -e INTERVALO_S=$${INTERVALO_S:-1.0} -e INYECTAR_PICO_CADA=$${INYECTAR_PICO_CADA:-30} \
		stream-runner python -u /workspace/src/streaming/producers/simm_producer.py

stream-simm-job: env-check ## [Sprint 3] Job tumbling S-3 + alerta corredor activo
	$(COMPOSE_EXEC) -e VENTANA_MINUTOS=$${VENTANA_MINUTOS:-5} -e UMBRAL_INTENSIDAD=$${UMBRAL_INTENSIDAD:-80} \
		stream-runner python -u /workspace/src/streaming/flink_jobs/simm_aforo_job.py

stream-metro-producer: env-check ## [Sprint 3] Productor Metro afluencia (S-4)
	$(COMPOSE_EXEC) -e INTERVALO_S=$${INTERVALO_S:-0.3} -e EVENTOS_POR_HORA=$${EVENTOS_POR_HORA:-12} \
		stream-runner python -u /workspace/src/streaming/producers/metro_producer.py

stream-metro-job: env-check ## [Sprint 3] Job tumbling S-4 (afluencia Metro RT)
	$(COMPOSE_EXEC) -e VENTANA_MINUTOS=$${VENTANA_MINUTOS:-5} \
		stream-runner python -u /workspace/src/streaming/flink_jobs/metro_afluencia_job.py

stream-hibrido: env-check ## [Sprint 3] Job híbrido batch↔streaming (sección 4.3 propuesta)
	$(COMPOSE_EXEC) -e FACTOR_AFLUENCIA=$${FACTOR_AFLUENCIA:-0.7} -e UMBRAL_LLUVIA_MM=$${UMBRAL_LLUVIA_MM:-0.3} \
		stream-runner python -u /workspace/src/streaming/flink_jobs/job_hibrido.py

dashboard: ## [Sprint 3] Levantar dashboard Streamlit (host)
	@which streamlit > /dev/null || pip3 install streamlit pymongo pandas pydeck
	@MONGO_HOST=localhost streamlit run app/dashboard.py

pipeline-streaming-completo: stream-up exportar-referencias ## [Sprint 3] Stack streaming + referencias listas
	@echo ""
	@echo "✅ Stack streaming arriba y referencias generadas."
	@echo ""
	@echo "Lanzar productores y jobs en paralelo (cada uno en una terminal):"
	@echo "  Terminal 1:  make stream-producer            # SIATA (S-2)"
	@echo "  Terminal 2:  make stream-alert-job"
	@echo "  Terminal 3:  make stream-encicla-producer    # EnCicla (S-1)"
	@echo "  Terminal 4:  make stream-encicla-job"
	@echo "  Terminal 5:  make stream-simm-producer       # SIMM (S-3)"
	@echo "  Terminal 6:  make stream-simm-job"
	@echo "  Terminal 7:  make stream-metro-producer      # Metro (S-4)"
	@echo "  Terminal 8:  make stream-metro-job"
	@echo "  Terminal 9:  make stream-hibrido             # 4.3 batch↔stream"
	@echo "  Terminal 10: make dashboard                  # Streamlit"

# ---------- Sprint 4 — ADRs + Benchmark + MapReduce legacy ---------

benchmark-formatos: env-check ## [Sprint 4] Benchmark CSV vs Parquet vs Parquet+ZSTD (ADR 04)
	@$(COMPOSE_EXEC) -T spark-iceberg python /workspace/scripts/benchmark_formatos.py

legacy-generar: ## [Sprint 4] Generar CSV legacy pre/post-2017 (módulo 01)
	@PYTHONIOENCODING=utf-8 $(PYTHON_HOST) src/legacy/generar_dataset_legacy.py

legacy-mapreduce: ## [Sprint 4] Correr el job mrjob (host, modo inline)
	@which mrjob >/dev/null 2>&1 || pip3 install --quiet mrjob
	@rm -rf data/processed/incidentes_normalizados
	@PYTHONIOENCODING=utf-8 $(PYTHON_HOST) src/legacy/mapreduce_incidentes.py \
		data/raw/medata_legacy/incidentes_pre2017.csv \
		data/raw/medata_legacy/incidentes_post2017.csv \
		--output-dir data/processed/incidentes_normalizados

legacy-ingest: env-check ## [Sprint 4] Ingestar salida del MR → Bronze Iceberg
	@$(COMPOSE_EXEC) -T spark-iceberg python /workspace/src/batch/bronze/ingest_legacy_mr.py

pipeline-legacy: legacy-generar legacy-mapreduce legacy-ingest ## [Sprint 4] MR end-to-end
	@echo ""
	@echo "✅ Pipeline MapReduce completo. Tabla:"
	@echo "    demo.pulsomed.bronze.medata_incidentes_legacy_mr"

# ---------- Sprint 5 — ML, Grafo, Trino, EDA cruzado ----------

ml-fatalidad: env-check ## [Sprint 5] Módulo 06a · MLlib: entrenar modelo de gravedad (multiclase)
	@$(COMPOSE_EXEC) -T spark-iceberg python /workspace/src/batch/ml/train_fatalidad.py

grafo-metro: env-check ## [Sprint 5] Módulo 06b · PageRank + rutas óptimas red Metro
	@$(COMPOSE_EXEC) -T spark-iceberg python /workspace/src/batch/graph/red_metro.py

trino-up: env-check ## [Sprint 5] Levantar Trino (tercer motor SQL sobre Gold)
	$(COMPOSE) up -d trino
	@echo ""
	@echo "Trino UI: http://localhost:$${TRINO_PORT:-8084}/ui"
	@echo "Conectar con:  make trino-sql"

trino-sql: env-check ## [Sprint 5] Abrir CLI de Trino (consulta interactiva)
	$(COMPOSE) exec trino trino

trino-demo: env-check ## [Sprint 5] Ejecutar consultas de demo sobre Gold vía Trino
	@$(COMPOSE) exec -T trino trino --execute \
		"SELECT nombre, linea, pagerank, ranking FROM demo.pulsomed.gold.red_metro_pagerank ORDER BY ranking LIMIT 10;"
	@echo ""
	@$(COMPOSE) exec -T trino trino --execute \
		"SELECT anio, comuna, incidentes_total, indice_severidad FROM demo.pulsomed.gold.accidentalidad_por_comuna ORDER BY indice_severidad DESC LIMIT 10;"

pipeline-sprint5: ml-fatalidad grafo-metro ## [Sprint 5] Pipeline ML + Grafo end-to-end
	@echo ""
	@echo "✅ Sprint 5 pipeline completado."
	@echo "   Tablas Gold creadas:"
	@echo "     demo.pulsomed.gold.ml_fatalidad_evaluacion"
	@echo "     demo.pulsomed.gold.red_metro_pagerank"
	@echo "     demo.pulsomed.gold.red_metro_rutas_optimas"
	@echo ""
	@echo "Para Trino (bonus):  make trino-up && make trino-demo"
	@echo "Para notebooks:      make jupyter"

# ---------- make all — pipeline completo Sprint 0..5 ----------

all: env-check up init-namespaces pipeline-batch pipeline-legacy pipeline-sprint5 ## Orquesta el pipeline completo Sprint 0→5 end-to-end
	@echo ""
	@echo "╔══════════════════════════════════════════════════════╗"
	@echo "║      Pulso Medellín — pipeline completo listo        ║"
	@echo "║                                                      ║"
	@echo "║  Batch Gold:      make build-gold                    ║"
	@echo "║  Streaming demo:  make pipeline-streaming-completo   ║"
	@echo "║  ML + Grafo:      make pipeline-sprint5              ║"
	@echo "║  Trino SQL:       make trino-up && make trino-sql    ║"
	@echo "║  Dashboard:       make dashboard                     ║"
	@echo "║  Jupyter:         make jupyter                       ║"
	@echo "╚══════════════════════════════════════════════════════╝"
