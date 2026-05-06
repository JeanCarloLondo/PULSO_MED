#!/usr/bin/env bash
# download_datasets.sh — Descarga los 6 datasets de Pulso Medellín a data/raw/
#
# Uso (desde la raíz del repositorio):
#   bash scripts/download_datasets.sh
#
# Requiere : curl, unzip
# Opcional : jq (descarga automática de SIATA via Dataverse API)
#
# Idempotente: omite archivos que ya existen y tienen tamaño > 0.

set -uo pipefail

# ── Verificar directorio raíz ─────────────────────────────────────────────────
if [[ ! -d "data/raw" ]]; then
    echo "ERROR: ejecutar desde la raíz del repositorio (donde está data/raw/)."
    exit 1
fi

# ── Colores ───────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; BOLD='\033[1m'; NC='\033[0m'
ok()      { echo -e "${GREEN}  ✓${NC} $*"; }
warn()    { echo -e "${YELLOW}  ⚠${NC} $*"; }
err()     { echo -e "${RED}  ✗${NC} $*"; }
section() { echo -e "\n${BOLD}━━━ $* ━━━${NC}"; }

FALLOS=0

# ── Función de descarga ───────────────────────────────────────────────────────
# Uso: descargar <destino> <url> [descripcion]
descargar() {
    local destino="$1"
    local url="$2"
    local desc="${3:-$(basename "$destino")}"

    if [[ -f "$destino" && -s "$destino" ]]; then
        warn "Ya existe: $desc — omitiendo."
        return 0
    fi

    echo "  → $desc"
    if curl -fsSL -L --retry 3 --retry-delay 5 -o "$destino" "$url" 2>/dev/null; then
        if [[ -s "$destino" ]]; then
            ok "$desc  ($(du -sh "$destino" 2>/dev/null | cut -f1))"
        else
            rm -f "$destino"
            err "$desc — respuesta vacía. Verificar URL: $url"
            FALLOS=$((FALLOS + 1))
        fi
    else
        rm -f "$destino"
        err "$desc — falló la descarga. URL: $url"
        FALLOS=$((FALLOS + 1))
    fi
}

# ─────────────────────────────────────────────────────────────────────────────
section "1/6  MEData — Incidentes viales  (medata.gov.co)"
# ─────────────────────────────────────────────────────────────────────────────
mkdir -p data/raw/medata_incidentes

descargar \
    "data/raw/medata_incidentes/incidentes_viales.csv" \
    "https://medata.gov.co/sites/default/files/distribution/1-023-25-000094/incidentes_viales.csv" \
    "incidentes_viales.csv  (2014–2024, >150 k registros)"

# ─────────────────────────────────────────────────────────────────────────────
section "2/6  Metro de Medellín — Afluencia anual + GTFS  (ArcGIS Hub)"
# ─────────────────────────────────────────────────────────────────────────────
# Los ítems del Hub son tipo Document (normalmente Excel).
# URL patrón: https://www.arcgis.com/sharing/rest/content/items/<id>/data
mkdir -p data/raw/metro_afluencia data/raw/metro_gtfs/2024

for anio in 2022 2023 2024; do
    case "$anio" in
        2022) item_id="4c66112ec6d045f29f7ad2cbffe06cc2" ;;
        2023) item_id="569c4b4c1ad54c3da95aa5f195637db2" ;;
        2024) item_id="666bac2214f445a18227f16cf8426faf" ;;
    esac
    descargar \
        "data/raw/metro_afluencia/afluencia_metro_${anio}.xlsx" \
        "https://www.arcgis.com/sharing/rest/content/items/${item_id}/data" \
        "Afluencia Metro ${anio}"
done

descargar \
    "data/raw/metro_gtfs/gtfs_metro_2024.zip" \
    "https://www.arcgis.com/sharing/rest/content/items/1717b6bff6c54623835c51be6969738f/data" \
    "GTFS Metro 2024"

if [[ -f "data/raw/metro_gtfs/gtfs_metro_2024.zip" ]]; then
    if unzip -t "data/raw/metro_gtfs/gtfs_metro_2024.zip" &>/dev/null; then
        unzip -q -o "data/raw/metro_gtfs/gtfs_metro_2024.zip" \
            -d "data/raw/metro_gtfs/2024/" \
        && ok "GTFS descomprimido en data/raw/metro_gtfs/2024/" \
        || warn "Error al descomprimir GTFS."
    else
        warn "El archivo GTFS descargado no es un ZIP — puede ser HTML de redirección."
        warn "Descargar manualmente: https://datosabiertos-metrodemedellin.opendata.arcgis.com/items/1717b6bff6c54623835c51be6969738f"
    fi
fi

# ─────────────────────────────────────────────────────────────────────────────
section "3/6  EnCicla — Estaciones  (préstamos históricos: solicitud formal)"
# ─────────────────────────────────────────────────────────────────────────────
mkdir -p data/raw/encicla_estaciones data/raw/encicla_prestamos

descargar \
    "data/raw/encicla_estaciones/estaciones_encicla.json" \
    "https://datosabiertos.metropol.gov.co/api/3/action/datastore_search?resource_id=4f309281-b47e-4d30-800d-93bba7189e39&limit=500" \
    "Estaciones EnCicla (API CKAN)"

echo ""
warn "Los datos de PRÉSTAMOS históricos EnCicla NO están publicados en el portal abierto."
warn "Para obtenerlos:"
warn "  1. Ir a https://metropol.gov.co/tramites"
warn "  2. Radicar PQRS con asunto: 'Solicitud dataset préstamos EnCicla — investigación académica'"
warn "  3. Una vez recibidos, colocar los CSV en: data/raw/encicla_prestamos/"
echo ""
warn "Para el Sprint 1, la demanda de EnCicla (pregunta B-3) puede trabajarse"
warn "con los datos de estaciones + simulación sintética basada en el histograma publicado."

# ─────────────────────────────────────────────────────────────────────────────
section "4/6  SIATA — Calidad del aire PM2.5  (datos.siata.gov.co · Dataverse)"
# ─────────────────────────────────────────────────────────────────────────────
# DOI oficial PM2.5: doi:10.83041/AUWZWT
# Contiene ~155 archivos .tab con lecturas por estación, desde 2013.
mkdir -p data/raw/siata_historico

SIATA_DOI="doi:10.83041/AUWZWT"
SIATA_META="data/raw/siata_historico/_pm25_metadata.json"

if ! command -v jq &>/dev/null; then
    warn "jq no está instalado — necesario para la descarga automática de SIATA."
    warn "Instalar: sudo apt install jq   ó   brew install jq"
    warn "Luego volver a ejecutar este script."
    warn "Descarga manual: https://datos.siata.gov.co/dataverse/calidadaire"
    warn "DOI PM2.5: ${SIATA_DOI}"
    FALLOS=$((FALLOS + 1))
else
    echo "  → Consultando metadatos SIATA PM2.5 en Dataverse..."

    if curl -fsSL --retry 3 \
        "https://datos.siata.gov.co/api/datasets/:persistentId/?persistentId=${SIATA_DOI}" \
        -o "$SIATA_META" 2>/dev/null && [[ -s "$SIATA_META" ]]; then

        ok "Metadatos obtenidos ($(du -sh "$SIATA_META" | cut -f1))"
        total=$(jq '.data.latestVersion.files | length' "$SIATA_META" 2>/dev/null || echo "?")
        echo "  → ${total} archivos de lectura PM2.5 encontrados. Descargando..."

        jq -r '.data.latestVersion.files[] | "\(.dataFile.id)\t\(.dataFile.filename)"' \
            "$SIATA_META" 2>/dev/null | \
        while IFS=$'\t' read -r file_id filename; do
            destino="data/raw/siata_historico/${filename}"
            [[ -f "$destino" && -s "$destino" ]] && continue
            curl -fsSL --retry 2 \
                "https://datos.siata.gov.co/api/access/datafile/${file_id}" \
                -o "$destino" 2>/dev/null \
            || warn "Falló: $filename (id=$file_id)"
        done

        archivos_ok=$(find data/raw/siata_historico -type f -not -name "_*" | wc -l | tr -d ' ')
        ok "SIATA PM2.5: ${archivos_ok} archivos en data/raw/siata_historico/"
    else
        rm -f "$SIATA_META"
        err "No se pudo conectar con datos.siata.gov.co"
        warn "Alternativa (parcial, 2018-2019): https://www.kaggle.com/datasets/sjessies/datos-siata-calidad-del-aire"
        FALLOS=$((FALLOS + 1))
    fi
fi

# ─────────────────────────────────────────────────────────────────────────────
section "5/6  GeoMedellín — Comunas y corregimientos  (OpenStreetMap / Overpass)"
# ─────────────────────────────────────────────────────────────────────────────
# ArcGIS Hub requiere autenticación para descarga bulk → usamos Overpass API
# (OSM, licencia ODbL — compatible con uso académico).
mkdir -p data/raw/geomedellin

if [[ -f "data/raw/geomedellin/comunas_corregimientos.geojson" && \
      -s "data/raw/geomedellin/comunas_corregimientos.geojson" ]]; then
    warn "Ya existe: comunas_corregimientos.geojson — omitiendo."
else
    echo "  → Construyendo GeoJSON de comunas desde OpenStreetMap..."
    if python3 scripts/overpass_a_geojson.py \
        --out "data/raw/geomedellin/comunas_corregimientos.geojson"; then
        ok "comunas_corregimientos.geojson  ($(du -sh data/raw/geomedellin/comunas_corregimientos.geojson | cut -f1))"
    else
        err "Falló descarga OSM de comunas."
        warn "Descarga manual: https://geomedellin-m-medellin.opendata.arcgis.com/datasets/7a8ad9f85799453e9dab4dc0c8c80bb3_3"
        FALLOS=$((FALLOS + 1))
    fi
fi

# ─────────────────────────────────────────────────────────────────────────────
section "6/6  SIMM — Aforos vehiculares  (medata.gov.co)"
# ─────────────────────────────────────────────────────────────────────────────
mkdir -p data/raw/simm_aforos

descargar \
    "data/raw/simm_aforos/aforos_vehiculares.csv" \
    "https://medata.gov.co/sites/default/files/distribution/1-023-25-000301/Aforos_Vehiculares.csv" \
    "Aforos vehiculares (consolidado de corredores)"

descargar \
    "data/raw/simm_aforos/simm_traffic_data.csv" \
    "https://medata.gov.co/sites/default/files/distribution/1-023-25-000300/simmtrafficdata.csv" \
    "SIMM traffic data (cámaras ARS)"

# ─────────────────────────────────────────────────────────────────────────────
# Resumen final
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo "Archivos en data/raw/:"
echo ""
find data/raw -type f -not -name ".gitkeep" | sort | while read -r f; do
    printf "  %-62s %s\n" "$f" "$(du -sh "$f" 2>/dev/null | cut -f1)"
done
echo ""

if [[ $FALLOS -gt 0 ]]; then
    err "Completado con ${FALLOS} problema(s). Revisar mensajes ⚠ arriba."
    echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    exit 1
else
    ok "Descarga completada sin errores."
    warn "Pendiente: EnCicla préstamos → PQRS a metropol.gov.co/tramites"
    echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
fi
