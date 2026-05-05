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
COMPOSE_EXEC := MSYS_NO_PATHCONV=1 $(COMPOSE) exec
SERVICE ?=

ENV_CHECK := @if [ ! -f .env ]; then \
	echo "ERROR: archivo .env no encontrado. Copia .env.example: cp .env.example .env"; \
	exit 1; \
fi

.PHONY: help up down clean ps logs smoke shell pyspark jupyter \
        smoke-minio smoke-iceberg smoke-mongo \
        env-check rebuild wait-stack

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

# ---------- Sprint 1+ (placeholders) --------------------------

pipeline-batch: ## [Sprint 1] Pipeline Bronze → Silver → Gold
	@echo "TODO: implementado en Sprint 1"
	@exit 1

pipeline-streaming: ## [Sprint 2+] Productores Kafka + jobs Flink
	@echo "TODO: implementado en Sprint 2"
	@exit 1
