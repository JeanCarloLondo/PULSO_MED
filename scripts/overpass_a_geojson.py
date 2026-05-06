"""
overpass_a_geojson.py — Descarga las comunas de Medellín desde OpenStreetMap
(Overpass API) y las convierte a GeoJSON.

Uso:
    python3 scripts/overpass_a_geojson.py \
        --out data/raw/geomedellin/comunas_corregimientos.geojson

Solo requiere Python 3 stdlib (json, urllib). Sin dependencias externas.
"""

import argparse
import json
import re
import sys
import urllib.parse
import urllib.request
from pathlib import Path

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# Nombres oficiales de los 5 corregimientos rurales de Medellín
CORREGIMIENTOS_OFICIALES = {
    "Altavista",
    "San Sebastián de Palmitas",
    "San Cristóbal",
    "San Antonio de Prado",
    "Santa Elena",
}

# Patrón oficial de las 16 comunas urbanas (ej. "Comuna 1 - Popular")
_PATRON_COMUNA = re.compile(r"^Comuna\s+\d+", re.IGNORECASE)


def _es_division_oficial(tags: dict) -> bool:
    nombre = tags.get("name", "")
    nivel = tags.get("admin_level", "")
    if nivel == "8":
        return bool(_PATRON_COMUNA.match(nombre)) or nombre in CORREGIMIENTOS_OFICIALES
    if nivel == "7":
        return nombre in CORREGIMIENTOS_OFICIALES
    return False

# Comunas = admin_level 8 | Corregimientos = admin_level 7 (perímetro rural)
QUERY = """
[out:json][timeout:90];
area["ISO3166-1"="CO"]["admin_level"="2"]->.colombia;
area["admin_level"="4"]["name"="Antioquia"](area.colombia)->.antioquia;
area["admin_level"="6"]["name"="Medellín"](area.antioquia)->.medellin;
(
  relation(area.medellin)["boundary"="administrative"]["admin_level"="8"];
  relation(area.medellin)["boundary"="administrative"]["admin_level"="7"]
    ["name"!="Perímetro Urbano Medellín"];
);
out geom;
"""


def _coords_iguales(a: dict, b: dict, tol: float = 1e-7) -> bool:
    return abs(a["lat"] - b["lat"]) < tol and abs(a["lon"] - b["lon"]) < tol


def _construir_anillo(ways: list) -> list:
    """
    Encadena una lista de ways (cada una: lista de {lat, lon}) en un anillo cerrado.
    Devuelve lista de [lon, lat] (orden GeoJSON).
    """
    if not ways:
        return []

    ring = list(ways[0])
    restantes = list(ways[1:])

    iteraciones = 0
    while restantes and iteraciones < len(restantes) * len(restantes) + 1:
        iteraciones += 1
        fin = ring[-1]
        enganchado = False
        for i, way in enumerate(restantes):
            if _coords_iguales(way[0], fin):
                ring.extend(way[1:])
                restantes.pop(i)
                enganchado = True
                break
            if _coords_iguales(way[-1], fin):
                ring.extend(reversed(way[:-1]))
                restantes.pop(i)
                enganchado = True
                break
        if not enganchado:
            break

    if restantes:
        for w in restantes:
            ring.extend(w)

    if ring and not _coords_iguales(ring[0], ring[-1]):
        ring.append(ring[0])

    return [[p["lon"], p["lat"]] for p in ring]


def _relacion_a_feature(element: dict) -> dict | None:
    tags = element.get("tags", {})
    miembros = element.get("members", [])

    outer_ways = [
        m.get("geometry", [])
        for m in miembros
        if m.get("type") == "way" and m.get("role") == "outer"
    ]
    inner_ways = [
        m.get("geometry", [])
        for m in miembros
        if m.get("type") == "way" and m.get("role") == "inner"
    ]

    if not outer_ways:
        return None

    outer_ring = _construir_anillo(outer_ways)
    if len(outer_ring) < 4:
        return None

    coords = [outer_ring]
    if inner_ways:
        inner_ring = _construir_anillo(inner_ways)
        if len(inner_ring) >= 4:
            coords.append(inner_ring)

    # Inferir si es comuna o corregimiento
    nivel = tags.get("admin_level", "?")
    tipo = "corregimiento" if nivel == "7" else "comuna"

    return {
        "type": "Feature",
        "geometry": {"type": "Polygon", "coordinates": coords},
        "properties": {
            "osm_id":       element.get("id"),
            "nombre":       tags.get("name", ""),
            "tipo":         tipo,
            "admin_level":  nivel,
            "nombre_alt":   tags.get("alt_name", ""),
            "wikipedia":    tags.get("wikipedia", ""),
        },
    }


def descargar_overpass(query: str) -> dict:
    datos = urllib.parse.urlencode({"data": query}).encode()
    req = urllib.request.Request(OVERPASS_URL, data=datos, method="POST")
    req.add_header("User-Agent", "PulsoMedellin/1.0 (proyecto academico EAFIT)")
    print("  → Consultando Overpass API (puede tardar ~20 s)...")
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out",
        default="data/raw/geomedellin/comunas_corregimientos.geojson",
        help="Ruta de salida del GeoJSON",
    )
    args = parser.parse_args()

    destino = Path(args.out)
    if destino.exists() and destino.stat().st_size > 0:
        print(f"  ⚠ Ya existe: {destino} — omitiendo.")
        return 0

    destino.parent.mkdir(parents=True, exist_ok=True)

    datos_osm = descargar_overpass(QUERY)
    elementos = datos_osm.get("elements", [])
    print(f"  → {len(elementos)} relaciones recibidas.")

    features = []
    omitidos = []
    for el in elementos:
        tags = el.get("tags", {})
        if not _es_division_oficial(tags):
            omitidos.append(tags.get("name", f"id={el.get('id')}"))
            continue
        feat = _relacion_a_feature(el)
        if feat:
            features.append(feat)
        else:
            nombre = tags.get("name", f"id={el.get('id')}")
            print(f"  ⚠ Sin geometría outer válida: {nombre}")

    if omitidos:
        print(f"  → Omitidos {len(omitidos)} sectores no oficiales: {', '.join(omitidos)}")

    geojson = {
        "type": "FeatureCollection",
        "crs": {"type": "name", "properties": {"name": "EPSG:4326"}},
        "features": features,
    }

    with open(destino, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)

    print(f"  ✓ {len(features)} polígonos → {destino}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
