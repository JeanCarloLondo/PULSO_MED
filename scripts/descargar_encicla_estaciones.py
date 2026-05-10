"""
descargar_encicla_estaciones.py — Recupera la red real de estaciones EnCicla
desde OpenStreetMap (Overpass API) y guarda un JSON consumible por nuestros
pipelines.

Por qué OpenStreetMap:
    EnCicla no expone una API pública de disponibilidad (su app móvil usa un
    backend privado autenticado). Los datos abiertos del Área Metropolitana
    actualmente sólo entregan préstamos históricos vía PQRS. La capa OSM tiene
    103 nodos `amenity=bicycle_rental` con `network=EnCicla` cubriendo todo el
    Valle de Aburrá, con nombres oficiales (Ruta N, Plaza Botero, MAMM, etc.) y
    coordenadas reales. Esto reemplaza el archivo sintético previo en el que
    los nombres eran "Estación EnCicla 1..N".

Salida:
    data/raw/encicla_estaciones/estaciones_encicla.json
    Esquema: {result: {records: [{estacion_id, nombre, latitud, longitud,
              capacidad_anclajes, estado}]}}
    Compatible con el ingest_encicla.py existente (no hay que tocar Bronze).

Uso:
    pip install requests
    python scripts/descargar_encicla_estaciones.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: pip install requests", file=sys.stderr)
    sys.exit(1)

OVERPASS = "https://overpass-api.de/api/interpreter"
DESTINO = Path("data/raw/encicla_estaciones/estaciones_encicla.json")
# Bounding box centrada en el Valle de Aburrá (radio 30 km desde Medellín)
QUERY = """[out:json][timeout:90];
(
  node["amenity"="bicycle_rental"](around:30000,6.2442,-75.5812);
);
out body;
"""
CAPACIDAD_DEFAULT = 12  # cuando OSM no la trae, asumimos un anclaje promedio


def main() -> int:
    print("→ Consultando Overpass API…")
    r = requests.post(
        OVERPASS,
        data={"data": QUERY},
        headers={"User-Agent": "PulsoMedellin/1.0 (academico)"},
        timeout=180,
    )
    r.raise_for_status()
    data = r.json()
    elementos = data.get("elements", [])

    estaciones = []
    next_id = 1
    for el in elementos:
        tags = el.get("tags", {})
        nombre = tags.get("name", "").strip()
        if not nombre:
            continue
        red = tags.get("network", "").lower()
        # Si tiene network y NO es EnCicla, lo excluimos (filtramos otras redes)
        if red and "encicla" not in red:
            continue
        # Si no tiene network pero el nombre incluye "EnCicla", lo aceptamos
        if not red and "encicla" not in nombre.lower():
            continue
        try:
            cap = int(tags["capacity"])
        except (KeyError, ValueError):
            cap = CAPACIDAD_DEFAULT
        estaciones.append({
            "_id": next_id,
            "estacion_id": f"ENC{next_id:03d}",
            "nombre": nombre,
            "latitud": float(el["lat"]),
            "longitud": float(el["lon"]),
            "capacidad_anclajes": cap,
            "estado": "activa",
            "osm_id": el.get("id"),
        })
        next_id += 1

    if not estaciones:
        print("ERROR: Overpass no devolvió estaciones EnCicla.", file=sys.stderr)
        return 2

    DESTINO.parent.mkdir(parents=True, exist_ok=True)
    salida = {
        "result": {
            "records": estaciones,
            "total": len(estaciones),
            "fuente": "openstreetmap_overpass_api",
        }
    }
    DESTINO.write_text(
        json.dumps(salida, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"✅ {len(estaciones)} estaciones reales en {DESTINO}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
