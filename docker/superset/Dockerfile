# ----------------------------------------------------------------------
# Pulso Medellín — Imagen custom de Apache Superset (Sprint 7 · BI)
# ----------------------------------------------------------------------
# Base: apache/superset (Python 3.10).
# Añadimos:
#   - sqlalchemy-trino     · driver SQLAlchemy para Trino (hace que Superset
#                            "vea" iceberg.pulsomed.gold y consulte vía Trino).
#   - trino                · cliente Python oficial (dependencia del dialect).
#   - pymongo              · permite agregar Mongo como datasource para
#                            tableros operacionales (alertas streaming S-1..S-4).
# Todo se instala como usuario root (USER 0) tal como pide la imagen oficial,
# y luego se vuelve al usuario superset.
# ----------------------------------------------------------------------

FROM apache/superset:4.0.2

USER root

RUN pip install --no-cache-dir \
        "sqlalchemy-trino==0.5.0" \
        "trino[sqlalchemy]==0.330.0" \
        "pymongo==4.6.3"

# Volvemos al usuario por defecto de Superset (uid 1000)
USER superset
