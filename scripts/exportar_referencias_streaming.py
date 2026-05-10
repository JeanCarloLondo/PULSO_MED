"""
exportar_referencias_streaming.py — Genera JSONs de referencia que los jobs
streaming (especialmente el job híbrido) consumen al arranque.

Salidas:
  data/processed/percentiles_metro.json
      Por (linea, hora_franja) → {p50, p75, p90, p95} de pasajeros/hora
      Computado sobre data/raw/metro_afluencia/afluencia_metro_*.csv (real)

  data/processed/corredores_alta_siniestralidad.json
      Top-N comunas (y derivado: corredores) por índice de severidad
      OMS-like (5×fallecidos + 1×heridos + 0.1×daños) sobre MEData real.

Por qué Python puro y no Spark:
    Los archivos son moderados (240k filas Metro, 270k filas MEData) — pandas
    o csv puro corren en segundos. Esto evita el overhead de Spark y permite
    que los jobs streaming se autocontengan (no dependen de que la pipeline
    batch del Sprint 1 esté actualizada con los datos reales).

Uso (desde el host):
    python scripts/exportar_referencias_streaming.py
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path
from statistics import quantiles, median


SALIDA_DIR = Path("data/processed")
METRO_GLOB = "data/raw/metro_afluencia/afluencia_metro_*.csv"
MEDATA_CSV = Path("data/raw/medata_incidentes/incidentes_viales.csv")
TOP_COMUNAS = 8


def _franja(hora: int) -> str:
    """Franjas horarias de operación del Metro."""
    if 5 <= hora <= 8:
        return "punta_am"      # 5..8
    if 9 <= hora <= 11:
        return "valle_am"
    if 12 <= hora <= 13:
        return "almuerzo"
    if 14 <= hora <= 16:
        return "valle_pm"
    if 17 <= hora <= 20:
        return "punta_pm"
    return "nocturno"


def percentiles_metro() -> dict:
    """Calcula p50/p75/p90/p95 de pasajeros por (linea, franja) sobre el CSV real."""
    valores: dict[tuple[str, str], list[int]] = defaultdict(list)
    archivos = sorted(Path().glob(METRO_GLOB))
    if not archivos:
        return {}
    n_total = 0
    for arch in archivos:
        with arch.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    pax = int(row["pasajeros"])
                    hora = int(row["hora"])
                except (KeyError, ValueError):
                    continue
                linea = (row.get("linea") or "").strip()
                if not linea or pax <= 0:
                    continue
                valores[(linea, _franja(hora))].append(pax)
                n_total += 1

    salida: dict[str, dict] = {}
    for (linea, franja), serie in valores.items():
        if len(serie) < 10:
            continue
        # quantiles devuelve los cuartiles por defecto; pedimos n=20 para p5..p95
        q = quantiles(serie, n=20)
        # q[i] es el percentil ((i+1)/20)*100
        salida.setdefault(linea, {})[franja] = {
            "p50": int(median(serie)),
            "p75": int(q[14]),  # 75% → idx 14 (15/20)
            "p90": int(q[17]),  # 90% → idx 17 (18/20)
            "p95": int(q[18]),
            "muestras": len(serie),
        }
    print(f"  → Metro percentiles: {len(salida)} líneas, {n_total:,} obs base")
    return {
        "fuente": "data/raw/metro_afluencia/*.csv (afluencia real Metro de Medellín)",
        "granularidad": "linea × hora_franja",
        "franjas": ["punta_am", "valle_am", "almuerzo", "valle_pm", "punta_pm", "nocturno"],
        "valores": salida,
    }


def corredores_riesgo() -> dict:
    """Top-N comunas por índice OMS-like + lista de corredores asociados."""
    if not MEDATA_CSV.exists():
        print(f"  ⚠ {MEDATA_CSV} no existe — output vacío")
        return {"corredores": [], "comunas": []}

    sev_por_comuna: dict[str, dict] = defaultdict(lambda: {"muertos": 0, "heridos": 0, "danos": 0, "vias": defaultdict(int)})
    n = 0
    with MEDATA_CSV.open(encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            comuna = (row.get("COMUNA") or row.get("comuna") or "").strip()
            if not comuna or comuna.upper() in ("SD", "NO_DEFINIDO", "NULL", ""):
                continue
            grav = (row.get("GRAVEDAD_ACCIDENTE") or row.get("gravedad_accidente") or "").strip()
            if grav == "Con muertos":
                sev_por_comuna[comuna]["muertos"] += 1
            elif grav == "Con heridos":
                sev_por_comuna[comuna]["heridos"] += 1
            else:
                sev_por_comuna[comuna]["danos"] += 1
            via = (row.get("DIRECCION") or row.get("direccion") or "").strip().upper()
            if via:
                sev_por_comuna[comuna]["vias"][via] += 1
            n += 1
    if not sev_por_comuna:
        print(f"  ⚠ MEData no produjo comunas — verificar headers")
        return {"corredores": [], "comunas": []}

    # Score severidad ponderado
    ranking = []
    for comuna, d in sev_por_comuna.items():
        score = d["muertos"] * 5.0 + d["heridos"] * 1.0 + d["danos"] * 0.1
        ranking.append({
            "comuna": comuna,
            "score": round(score, 1),
            "muertos": d["muertos"],
            "heridos": d["heridos"],
            "danos": d["danos"],
        })
    ranking.sort(key=lambda r: r["score"], reverse=True)
    top_comunas = ranking[:TOP_COMUNAS]
    top_nombres = [r["comuna"] for r in top_comunas]

    # Inferir corredores: tomar las direcciones más frecuentes del top de comunas;
    # extraer la "vía base" (calle/carrera/avenida) para que se cruce con el campo
    # CORREDOR del SIMM streaming.
    import re
    corredores = set()
    for comuna in top_nombres:
        vias = sev_por_comuna[comuna]["vias"]
        for direccion, cuenta in sorted(vias.items(), key=lambda x: -x[1])[:25]:
            # Extraer "AVENIDA NN", "CALLE NN", "CARRERA NN" del inicio
            m = re.match(r"^(AVENIDA|AV|CALLE|CL|CARRERA|CR|TRANSVERSAL|TV|DIAGONAL|DG)\s*([0-9A-Z]+)", direccion)
            if m:
                base = f"{m.group(1).title()} {m.group(2)}"
                corredores.add(base)
            elif "AUTOPISTA" in direccion:
                corredores.add("Autopista")
            elif "BOLIVARIANA" in direccion:
                corredores.add("Avenida Bolivariana")
            elif "ORIENTAL" in direccion:
                corredores.add("Avenida Oriental")
    # Añadir corredores conocidos que aparecen en el SIMM
    canon = {"Carrera 70", "Calle Colombia", "Avenida Las Vegas", "Carrera 80",
             "Avenida Oriental", "Carrera 65", "Calle 33", "Avenida 33"}
    corredores.update(canon)
    print(f"  → Comunas top: {len(top_nombres)} | corredores: {len(corredores)} | obs base: {n:,}")
    return {
        "fuente": "data/raw/medata_incidentes/incidentes_viales.csv (MEData real)",
        "metodologia": "score = 5×muertos + 1×heridos + 0.1×daños; top-N comunas; vías de mayor recurrencia",
        "comunas": top_comunas,
        "corredores": sorted(corredores),
    }


def main() -> int:
    SALIDA_DIR.mkdir(parents=True, exist_ok=True)
    print("→ Calculando percentiles Metro (real)…")
    pm = percentiles_metro()
    if pm:
        (SALIDA_DIR / "percentiles_metro.json").write_text(
            json.dumps(pm, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        print(f"  ✓ {SALIDA_DIR / 'percentiles_metro.json'}")
    else:
        print("  ⚠ Sin datos Metro reales (descargar primero con scripts/descargar_metro_afluencia_real.py)")

    print("\n→ Calculando corredores de alta siniestralidad (MEData real)…")
    cs = corredores_riesgo()
    (SALIDA_DIR / "corredores_alta_siniestralidad.json").write_text(
        json.dumps(cs, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"  ✓ {SALIDA_DIR / 'corredores_alta_siniestralidad.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
