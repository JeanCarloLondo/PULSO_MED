"""
generar_dataset_legacy.py — Simula el dataset "histórico legacy" de MEData
con DOS esquemas distintos (pre/post 2017) y SIN encabezados.

Por qué este script existe:
    MEData publica HOY un CSV unificado (data/raw/medata_incidentes/
    incidentes_viales.csv) que ya pasó por una limpieza institucional. Pero
    la realidad histórica fue caótica: hasta 2016 el dataset venía con un
    esquema (BARRIO_ACCIDENTE como nombre del barrio, FECHA en formato
    dd/mm/yyyy, sin LOCATION), y desde 2017 cambió (BARRIO, FECHA ISO8601,
    LOCATION como [lon, lat] string). El job MapReduce (mapreduce_incidentes.py)
    debe ser capaz de procesar AMBOS y emitir un schema único — esa es la
    razón pedagógica del módulo 01 (Arqueología de datos).

    Este script reproduce esa heterogeneidad partiendo del CSV unificado,
    así el job MapReduce tiene insumos realistas para demostrar el patrón.

Salida:
    data/raw/medata_legacy/incidentes_pre2017.csv   (sin encabezado, esquema viejo)
    data/raw/medata_legacy/incidentes_post2017.csv  (sin encabezado, esquema nuevo)

Uso:
    python src/legacy/generar_dataset_legacy.py
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

FUENTE = Path("data/raw/medata_incidentes/incidentes_viales.csv")
DESTINO = Path("data/raw/medata_legacy")

# Esquema VIEJO (pre-2017), 7 columnas, sin encabezado:
#   nro_radicado, fecha_dd_mm_yyyy, clase, gravedad, barrio_accidente, direccion, lon_lat_juntos
ESQUEMA_VIEJO = ["nro_radicado", "fecha", "clase", "gravedad", "barrio_accidente", "direccion", "ubicacion"]

# Esquema NUEVO (post-2017), 8 columnas, sin encabezado:
#   nro_radicado, fecha_iso8601, clase, gravedad, barrio, comuna, direccion, location
ESQUEMA_NUEVO = ["nro_radicado", "fecha", "clase", "gravedad", "barrio", "comuna", "direccion", "location"]


def _convertir_fecha_viejo(iso: str) -> str:
    """ISO 8601 → dd/mm/yyyy hh:mm:ss (formato pre-2017)."""
    if not iso or "T" not in iso:
        return iso
    fecha, resto = iso.split("T", 1)
    resto = resto.replace("Z", "").split(".")[0]
    anio, mes, dia = fecha.split("-")
    return f"{dia}/{mes}/{anio} {resto}"


def _ubicacion_juntos(location: str) -> str:
    """'[-75.5688, 6.2431]' → '-75.5688|6.2431' (formato pre-2017 sin corchetes)."""
    if not location:
        return ""
    s = location.replace("[", "").replace("]", "").replace(" ", "")
    return s.replace(",", "|")


def main() -> int:
    if not FUENTE.exists():
        print(f"ERROR: no existe {FUENTE}", file=sys.stderr)
        return 1

    DESTINO.mkdir(parents=True, exist_ok=True)
    pre = DESTINO / "incidentes_pre2017.csv"
    post = DESTINO / "incidentes_post2017.csv"

    n_pre, n_post = 0, 0
    with FUENTE.open(encoding="utf-8", errors="replace") as fin, \
         pre.open("w", newline="", encoding="utf-8") as fpre, \
         post.open("w", newline="", encoding="utf-8") as fpost:
        reader = csv.DictReader(fin)
        wpre = csv.writer(fpre)
        wpost = csv.writer(fpost)
        for row in reader:
            anio_str = (row.get("AÑO") or "").strip()
            try:
                anio = int(anio_str)
            except ValueError:
                continue
            if anio < 2017:
                wpre.writerow([
                    row.get("NRO_RADICADO", ""),
                    _convertir_fecha_viejo(row.get("FECHA_ACCIDENTES", "")),
                    row.get("CLASE_ACCIDENTE", ""),
                    row.get("GRAVEDAD_ACCIDENTE", ""),
                    row.get("BARRIO", ""),  # se llamaba BARRIO_ACCIDENTE en el legacy
                    row.get("DIRECCION", ""),
                    _ubicacion_juntos(row.get("LOCATION", "")),
                ])
                n_pre += 1
            else:
                wpost.writerow([
                    row.get("NRO_RADICADO", ""),
                    row.get("FECHA_ACCIDENTES", ""),
                    row.get("CLASE_ACCIDENTE", ""),
                    row.get("GRAVEDAD_ACCIDENTE", ""),
                    row.get("BARRIO", ""),
                    row.get("COMUNA", ""),
                    row.get("DIRECCION", ""),
                    row.get("LOCATION", ""),
                ])
                n_post += 1

    print(f"✓ {pre}     {n_pre:,} filas  (esquema viejo, sin encabezado)")
    print(f"✓ {post}    {n_post:,} filas  (esquema nuevo, sin encabezado)")
    print()
    print("Próximo paso:  python src/legacy/mapreduce_incidentes.py")
    print("                  data/raw/medata_legacy/incidentes_pre2017.csv")
    print("                  data/raw/medata_legacy/incidentes_post2017.csv")
    print("                  > data/processed/incidentes_normalizados.tsv")
    return 0


if __name__ == "__main__":
    sys.exit(main())
