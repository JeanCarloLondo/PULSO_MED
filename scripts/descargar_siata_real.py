"""
descargar_siata_real.py — Descarga el histórico real de PM2.5 (y opcionalmente
PM10) desde el Dataverse del SIATA, sin depender de jq.

Lo que hace:
  1. Resuelve cada DOI de Dataverse (PM2.5, PM10) consultando la API JSON.
  2. Lista los archivos `.tab` mensuales y los descarga en paralelo lógico (uno
     por uno, idempotente — omite si ya existen y no son nulos).
  3. Convierte cada `.tab` (formato ancho: timestamp + columna por estación) a
     CSV largo: timestamp, estacion_id, valor.
  4. Une todos los meses por contaminante en un único CSV final consumible por
     Bronze: `siata_pm25_horario.csv` y `siata_pm10_horario.csv`.

Decisiones:
  - Solo PM2.5 y PM10 son DOIs únicos. Las variables meteorológicas
    (precipitación, temperatura, humedad) en el Dataverse están dispersas en
    cientos de DOIs por estación; obtenerlas todas requeriría otro script
    dedicado (Sprint 4+). Por ahora se mantienen sintéticas para cumplir el
    esquema de Bronze; se documenta la limitación en el doc del Sprint 1.5.
  - Tab files con valores vacíos se interpretan como NULL.

Uso:
    pip install requests
    python scripts/descargar_siata_real.py
    python scripts/descargar_siata_real.py --solo-pm25
    python scripts/descargar_siata_real.py --desde 2020 --hasta 2024
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from datetime import datetime
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: instalar requests:  pip install requests", file=sys.stderr)
    sys.exit(1)

DV_BASE = "https://datos.siata.gov.co/api"
DOIS = {
    "pm25": "doi:10.83041/AUWZWT",
    "pm10": "doi:10.83041/VX4GC2",
}
# Cómo aparece el nombre del contaminante en los archivos .tab del Dataverse
TAGS_ARCHIVO = {
    "pm25": "PM2.5",
    "pm10": "PM10",
}
# Metadatos de la red (lat/lon por estación)
DOI_RED = "doi:10.83041/XTI3FH"
ARCHIVO_ESTACIONES = "Estaciones_calidad_aire.tab"
DESTINO_DIR = Path("data/raw/siata_historico")
TMP_DIR = DESTINO_DIR / "_raw_tabs"
SESION = requests.Session()
SESION.headers.update({"User-Agent": "Mozilla/5.0 PulsoMedellin/1.0"})


def _listar_archivos(doi: str) -> list[tuple[int, str]]:
    """Devuelve [(file_id, filename), ...] del último versionado del dataset."""
    r = SESION.get(
        f"{DV_BASE}/datasets/:persistentId/",
        params={"persistentId": doi},
        timeout=30,
    )
    r.raise_for_status()
    files = r.json()["data"]["latestVersion"]["files"]
    out: list[tuple[int, str]] = []
    for f in files:
        df = f.get("dataFile", {})
        out.append((int(df["id"]), df.get("filename", f"file_{df.get('id')}.tab")))
    return out


def _descargar_archivo(file_id: int, dst: Path) -> bool:
    if dst.exists() and dst.stat().st_size > 0:
        return True
    r = SESION.get(f"{DV_BASE}/access/datafile/{file_id}", timeout=60, stream=True)
    if r.status_code != 200:
        print(f"    ✗ id={file_id}: HTTP {r.status_code}", file=sys.stderr)
        return False
    with dst.open("wb") as f:
        for chunk in r.iter_content(64 * 1024):
            if chunk:
                f.write(chunk)
    return dst.stat().st_size > 0


def _ano_de_archivo(nombre: str) -> int | None:
    m = re.search(r"(\d{4})_\d{2}", nombre)
    return int(m.group(1)) if m else None


def _tab_a_filas_largas(
    path: Path, contaminante: str
) -> list[tuple[str, str, str, float]]:
    out: list[tuple[str, str, str, float]] = []
    with path.open(encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")
        try:
            cabeceras = next(reader)
        except StopIteration:
            return out
        # primera columna = fecha_hora; resto = estaciones
        estaciones = cabeceras[1:]
        for row in reader:
            if not row or not row[0]:
                continue
            ts = row[0].strip()
            for i, est in enumerate(estaciones, start=1):
                if i >= len(row):
                    break
                v = row[i].strip()
                if not v:
                    continue
                try:
                    valor = float(v)
                except ValueError:
                    continue
                out.append((ts, est.strip(), contaminante, valor))
    return out


def _consolidar_a_csv(
    contaminante: str, tabs_dir: Path, salida_csv: Path, anios: range
) -> int:
    tag = TAGS_ARCHIVO[contaminante]
    escritas = 0
    with salida_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["timestamp", "estacion_id", "variable", "valor"])
        for tab in sorted(tabs_dir.glob(f"*{tag}_*.tab")):
            anio = _ano_de_archivo(tab.name)
            if anio is None or anio not in anios:
                continue
            for fila in _tab_a_filas_largas(tab, contaminante):
                w.writerow(fila)
                escritas += 1
    return escritas


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--solo-pm25", action="store_true")
    parser.add_argument("--desde", type=int, default=2018, help="año mínimo")
    parser.add_argument("--hasta", type=int, default=datetime.now().year)
    args = parser.parse_args()

    DESTINO_DIR.mkdir(parents=True, exist_ok=True)
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    anios_validos = range(args.desde, args.hasta + 1)
    print(f"→ Años: {args.desde}..{args.hasta}")

    # Metadatos de estaciones (lat/lon, municipio)
    print("\n=== SIATA red de estaciones (metadatos) ===")
    try:
        archivos_red = _listar_archivos(DOI_RED)
        for fid, fname in archivos_red:
            if fname == ARCHIVO_ESTACIONES:
                dst = DESTINO_DIR / "siata_estaciones.tab"
                if _descargar_archivo(fid, dst):
                    print(f"  ✓ {dst.name}")
                break
    except Exception as exc:
        print(f"  ⚠ no se pudo obtener metadatos de la red: {exc}")

    contaminantes = ["pm25"] if args.solo_pm25 else ["pm25", "pm10"]
    for cont in contaminantes:
        doi = DOIS[cont]
        print(f"\n=== SIATA {cont.upper()} ({doi}) ===")
        try:
            archivos = _listar_archivos(doi)
        except Exception as exc:
            print(f"  ✗ no se pudo listar el dataset: {exc}", file=sys.stderr)
            return 2
        archivos_filtrados = [
            (fid, fname)
            for fid, fname in archivos
            if (a := _ano_de_archivo(fname)) is not None and a in anios_validos
        ]
        print(f"  Archivos a descargar (en rango): {len(archivos_filtrados)}")

        ok = 0
        for i, (fid, fname) in enumerate(archivos_filtrados, 1):
            dst = TMP_DIR / fname
            if _descargar_archivo(fid, dst):
                ok += 1
            if i % 20 == 0:
                print(f"    descargados {i}/{len(archivos_filtrados)}")
        print(f"  ✓ archivos descargados: {ok}/{len(archivos_filtrados)}")

        salida = DESTINO_DIR / f"siata_{cont}_horario.csv"
        n = _consolidar_a_csv(cont, TMP_DIR, salida, anios_validos)
        size_mb = salida.stat().st_size / (1024 * 1024)
        print(f"  ✓ {salida.name}: {n:,} filas largas ({size_mb:.1f} MB)")

    print("\n✅ SIATA real consolidado en data/raw/siata_historico/")
    print(
        "   Nota: la meteorología (T, RH, precipitación) sigue como sintético,"
        " viene en datasets per-estación que requerirán otro script en Sprint 4+."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
