"""
consultar_alertas.py — CLI para consultar las alertas PM2.5 generadas por
el job de streaming en MongoDB (`alertas_aire`).

Ejecutar (desde el contenedor stream-runner o desde host con pymongo):
    docker compose exec stream-runner python /workspace/scripts/consultar_alertas.py
    docker compose exec stream-runner python /workspace/scripts/consultar_alertas.py --zona valle_aburra_centro --ultimas 1h
    docker compose exec stream-runner python /workspace/scripts/consultar_alertas.py --gravedad critica

Argumentos:
    --zona         filtrar por una zona (string exacto)
    --gravedad     leve | moderada | critica
    --ultimas      ventana de tiempo: 10min | 1h | 24h | 7d
    --limite       máximo a mostrar (default 30)
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime, timedelta

sys.path.insert(0, "/workspace/src")

try:
    from pymongo import MongoClient
except ImportError:
    print("ERROR: falta pymongo. Instalar: pip install pymongo")
    sys.exit(1)

from shared.config import COL_ALERTAS_AIRE, MONGO_DB, MONGO_URI

_ULTIMAS = re.compile(r"^(\d+)(min|h|d)$", re.IGNORECASE)


def _parse_ultimas(s: str) -> timedelta:
    m = _ULTIMAS.match(s.strip().lower())
    if not m:
        raise ValueError(f"--ultimas debe ser como 10min, 1h, 24h, 7d (recibido: {s})")
    n = int(m.group(1))
    unidad = m.group(2)
    if unidad == "min":
        return timedelta(minutes=n)
    if unidad == "h":
        return timedelta(hours=n)
    return timedelta(days=n)


def main() -> int:
    p = argparse.ArgumentParser(description="Consulta alertas PM2.5 desde MongoDB")
    p.add_argument("--zona", default=None)
    p.add_argument("--gravedad", choices=["leve", "moderada", "critica"], default=None)
    p.add_argument("--ultimas", default=None, help="ej. 10min, 1h, 24h, 7d")
    p.add_argument("--limite", type=int, default=30)
    args = p.parse_args()

    cli = MongoClient(MONGO_URI)
    coll = cli[MONGO_DB][COL_ALERTAS_AIRE]

    flt: dict = {}
    if args.zona:
        flt["zona"] = args.zona
    if args.gravedad:
        flt["gravedad"] = args.gravedad
    if args.ultimas:
        delta = _parse_ultimas(args.ultimas)
        flt["emitido_en"] = {"$gte": datetime.utcnow() - delta}

    cur = coll.find(flt).sort("ventana_inicio", -1).limit(args.limite)
    docs = list(cur)

    if not docs:
        print("Sin alertas para los filtros dados.")
        cli.close()
        return 0

    print(f"{'ventana':<22}  {'zona':<28}  {'gravedad':<10}  pm25_avg  lect.")
    print("-" * 80)
    for d in docs:
        v = d["ventana_inicio"].strftime("%Y-%m-%d %H:%M")
        print(
            f"{v:<22}  {d['zona']:<28}  {d['gravedad']:<10}  "
            f"{d['pm25_promedio']:>6.1f}    {d['lecturas_en_ventana']:>3}"
        )
    print(f"\nTotal: {len(docs)} alertas (filtros: {flt or 'sin filtros'})")
    cli.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
