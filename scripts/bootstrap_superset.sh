#!/usr/bin/env bash
# ----------------------------------------------------------------------
# bootstrap_superset.sh · Sprint 7 (BI)
# ----------------------------------------------------------------------
# Inicializa el servicio Superset:
#   1. Crea el usuario admin (admin / admin).
#   2. Espera a que el API REST esté arriba.
#   3. Registra dos datasources:
#        - trino_iceberg     · Trino → Iceberg → capa Gold (lakehouse)
#        - pulsomed_bi_meta  · SQLite con metadatos del proyecto
#   4. Lista los datasources registrados.
#
# Idempotente: si el usuario admin ya existe o el datasource ya está
# registrado, lo informa y sigue.
#
# Uso (desde el host, NO desde dentro del contenedor):
#   bash scripts/bootstrap_superset.sh
# ----------------------------------------------------------------------

set -euo pipefail

# Configuración -------------------------------------------------------
SUPERSET_URL="${SUPERSET_URL:-http://localhost:${SUPERSET_PORT:-8088}}"
ADMIN_USER="${SUPERSET_ADMIN_USER:-admin}"
ADMIN_PASS="${SUPERSET_ADMIN_PASS:-admin}"
ADMIN_EMAIL="${SUPERSET_ADMIN_EMAIL:-admin@pulsomed.local}"
ADMIN_FIRSTNAME="${SUPERSET_ADMIN_FIRSTNAME:-Pulso}"
ADMIN_LASTNAME="${SUPERSET_ADMIN_LASTNAME:-Medellín}"
SUPERSET_CONTAINER="${SUPERSET_CONTAINER:-pulsomed-superset}"

DS_TRINO_NAME="trino_iceberg"
DS_TRINO_URI="trino://trino@trino:8080/iceberg"

DS_META_NAME="pulsomed_bi_meta"
DS_META_URI="sqlite:////app/bi-data/pulsomed_bi.db"

# Helpers -------------------------------------------------------------
log() { echo -e "[bootstrap-superset] $*"; }
log_ok() { echo -e "  \033[32m✓\033[0m $*"; }
log_skip() { echo -e "  \033[33m∼\033[0m $*"; }

# 1) Esperar a que Superset responda ----------------------------------
log "Esperando a que Superset responda en $SUPERSET_URL ..."
for i in $(seq 1 60); do
  if curl -fsS "$SUPERSET_URL/health" >/dev/null 2>&1; then
    log_ok "Superset listo (intento $i)"
    break
  fi
  if [ "$i" = "60" ]; then
    log "ERROR: Superset no respondió en 60 intentos. Revisar 'make logs SERVICE=superset'."
    exit 1
  fi
  sleep 2
done

# 2) Crear admin (idempotente) ----------------------------------------
log "Creando usuario admin '$ADMIN_USER' ..."
if docker exec "$SUPERSET_CONTAINER" superset fab create-admin \
     --username "$ADMIN_USER" \
     --firstname "$ADMIN_FIRSTNAME" \
     --lastname "$ADMIN_LASTNAME" \
     --email "$ADMIN_EMAIL" \
     --password "$ADMIN_PASS" 2>&1 | tee /tmp/superset_admin.log | grep -qi "already exists\|added Permission\|Recognized Database\|Admin User"; then
  if grep -qi "already exists" /tmp/superset_admin.log; then
    log_skip "Usuario admin ya existe — sin cambios"
  else
    log_ok "Usuario admin creado"
  fi
else
  log_ok "Usuario admin creado (sin coincidencia de patrón)"
fi

# 3) Login para obtener access token ----------------------------------
log "Login para obtener access token ..."
LOGIN_RESPONSE=$(curl -fsS -X POST "$SUPERSET_URL/api/v1/security/login" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"$ADMIN_USER\",\"password\":\"$ADMIN_PASS\",\"provider\":\"db\",\"refresh\":true}")
TOKEN=$(echo "$LOGIN_RESPONSE" | python -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
if [ -z "$TOKEN" ]; then
  log "ERROR: no se obtuvo access token"
  echo "$LOGIN_RESPONSE"
  exit 1
fi
log_ok "Access token obtenido (len=${#TOKEN})"

# CSRF token (requerido para POST /database/) -------------------------
CSRF_RESPONSE=$(curl -fsS "$SUPERSET_URL/api/v1/security/csrf_token/" \
  -H "Authorization: Bearer $TOKEN" \
  -c /tmp/superset_cookies.txt)
CSRF=$(echo "$CSRF_RESPONSE" | python -c "import sys,json; print(json.load(sys.stdin)['result'])")

# 4) Registrar datasource si no existe --------------------------------
registrar_datasource() {
  local nombre="$1"
  local uri="$2"
  local extra_json="${3:-{}}"

  # ¿Ya existe?
  local existente
  existente=$(curl -fsS "$SUPERSET_URL/api/v1/database/?q=(filters:!((col:database_name,opr:eq,value:'$nombre')))" \
    -H "Authorization: Bearer $TOKEN" \
    | python -c "import sys,json; d=json.load(sys.stdin); print(d.get('count', 0))")

  if [ "$existente" -gt "0" ]; then
    log_skip "Datasource '$nombre' ya está registrado"
    return 0
  fi

  log "Registrando datasource '$nombre' → $uri"
  local payload
  payload=$(python -c "
import json
print(json.dumps({
  'database_name': '$nombre',
  'sqlalchemy_uri': '$uri',
  'expose_in_sqllab': True,
  'allow_ctas': False,
  'allow_cvas': False,
  'allow_dml': False,
  'extra': '$extra_json',
}))
")

  curl -fsS -X POST "$SUPERSET_URL/api/v1/database/" \
    -H "Authorization: Bearer $TOKEN" \
    -H "X-CSRFToken: $CSRF" \
    -H "Referer: $SUPERSET_URL/" \
    -H "Content-Type: application/json" \
    -b /tmp/superset_cookies.txt \
    -d "$payload" \
    | python -c "import sys,json; r=json.load(sys.stdin); print('  id:', r.get('id'), '· result:', r.get('result',{}).get('database_name'))"
  log_ok "Datasource '$nombre' registrado"
}

registrar_datasource "$DS_TRINO_NAME" "$DS_TRINO_URI"
registrar_datasource "$DS_META_NAME" "$DS_META_URI"

# 5) Listar lo registrado ---------------------------------------------
log "Datasources actuales:"
curl -fsS "$SUPERSET_URL/api/v1/database/" \
  -H "Authorization: Bearer $TOKEN" \
  | python -c "
import sys, json
data = json.load(sys.stdin)
for db in data.get('result', []):
    print('  ·', db['database_name'], '→', db.get('backend','?'))
"

echo
log_ok "Bootstrap completo. Abre $SUPERSET_URL  · usuario: $ADMIN_USER / $ADMIN_PASS"
echo
echo "Próximos pasos para la demo:"
echo "  1. En SQL Lab elige 'trino_iceberg' → catálogo iceberg → esquema pulsomed.gold."
echo "  2. Sube los CSVs de data/processed/bi/ como datasets o usa 'pulsomed_bi_meta'."
echo "  3. Crea los 4 tableros: Hallazgos, Decisiones, Herramientas, Rúbrica."
echo "  4. Consulta docs/sprints/sprint-7-bi-superset.md para el guion de la demo."
