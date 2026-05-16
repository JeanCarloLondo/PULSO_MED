# ----------------------------------------------------------------------
# superset_config.py · Pulso Medellín (Sprint 7 · BI)
# ----------------------------------------------------------------------
# Configuración mínima de Apache Superset para el entorno del proyecto.
# Sólo sobreescribe lo estrictamente necesario; el resto usa defaults.
# ----------------------------------------------------------------------

import os

# Clave secreta para firmar sesiones. En dev se inyecta vía env var, con
# fallback determinista para que el contenedor arranque sin .env editado.
SECRET_KEY = os.environ.get(
    "SUPERSET_SECRET_KEY",
    "pulsomed-superset-demo-secret-cambiar-en-produccion",
)

# Habilitamos algunas features útiles para la demo BI.
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "ALERT_REPORTS": False,  # no se usa, evita warnings
}

# Subida de CSV / Excel activa por defecto (lo usamos para los metadatos BI).
CSV_EXTENSIONS = {"csv", "tsv"}
EXCEL_EXTENSIONS = {"xlsx", "xls"}

# Branding mínimo
APP_NAME = "Pulso Medellín · BI"
APP_ICON = "/static/assets/images/superset-logo-horiz.png"

# Timezone para los gráficos temporales.
DEFAULT_FEATURE_FLAGS = FEATURE_FLAGS
ROW_LIMIT = 50000

# El metastore de Superset por defecto es SQLite en /app/superset_home.
# Lo dejamos persistente vía el volumen `superset-home` del docker-compose.
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

# Permitir que Superset cargue queries largas (los joins Iceberg pueden serlo).
SQLLAB_TIMEOUT = 300
SUPERSET_WEBSERVER_TIMEOUT = 300
