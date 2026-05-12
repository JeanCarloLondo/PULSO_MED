"""
generar_muestras_sinteticas.py — Genera muestras sintéticas para las fuentes
cuya descarga real falla por restricciones de los portales (ArcGIS Hub para
Metro, API CKAN cambiada de Metropol para EnCicla, Dataverse SIATA que
requiere jq).

Genera CSV consistentes con los esquemas documentados en docs/01-arquitectura.md
para que el pipeline Bronze→Silver→Gold del Sprint 1 corra end-to-end.

Sustitución por datos reales: cuando se obtengan los archivos oficiales,
basta con dejarlos en `data/raw/<fuente>/` con el mismo nombre y los scripts
Bronze los preferirán.

Uso:
    python scripts/generar_muestras_sinteticas.py

Sin dependencias externas (solo stdlib).
"""

from __future__ import annotations

import csv
import json
import math
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw"


# ── 1. Metro Afluencia ─────────────────────────────────────────────────────
ESTACIONES_METRO = [
    # (id, nombre, linea, lat, lon)
    ("E01", "Niquía",     "A", 6.3404, -75.5443),
    ("E02", "Bello",      "A", 6.3325, -75.5562),
    ("E03", "Madera",     "A", 6.3197, -75.5605),
    ("E04", "Acevedo",    "A", 6.2997, -75.5582),
    ("E05", "Tricentenario","A", 6.2898, -75.5641),
    ("E06", "Caribe",     "A", 6.2780, -75.5683),
    ("E07", "Universidad","A", 6.2691, -75.5651),
    ("E08", "Hospital",   "A", 6.2610, -75.5631),
    ("E09", "Prado",      "A", 6.2538, -75.5662),
    ("E10", "Parque Berrío","A", 6.2491, -75.5687),
    ("E11", "San Antonio","A", 6.2473, -75.5707),
    ("E12", "Alpujarra",  "A", 6.2434, -75.5713),
    ("E13", "Exposiciones","A", 6.2389, -75.5728),
    ("E14", "Industriales","A", 6.2324, -75.5749),
    ("E15", "Poblado",    "A", 6.2128, -75.5778),
    ("E16", "Aguacatala", "A", 6.1934, -75.5817),
    ("E17", "Ayurá",      "A", 6.1822, -75.5868),
    ("E18", "Envigado",   "A", 6.1758, -75.5916),
    ("E19", "Itagüí",     "A", 6.1647, -75.6020),
    ("E20", "Sabaneta",   "A", 6.1525, -75.6168),
    ("E21", "La Estrella","A", 6.1454, -75.6362),
    # Línea B
    ("E22", "Cisneros",   "B", 6.2484, -75.5757),
    ("E23", "Suramericana","B", 6.2557, -75.5832),
    ("E24", "Estadio",    "B", 6.2570, -75.5897),
    ("E25", "Floresta",   "B", 6.2594, -75.6050),
    ("E26", "Santa Lucía","B", 6.2606, -75.6132),
    ("E27", "San Javier", "B", 6.2570, -75.6135),
]

def gen_metro_afluencia():
    """3 años de afluencia horaria por línea (esquema real post-Sprint 4).

    El Metro publica por línea, NO por estación. Columnas: fecha, linea, hora, pasajeros.
    """
    out = RAW / "metro_afluencia"
    out.mkdir(parents=True, exist_ok=True)

    # Perfil horario típico (hora → factor relativo al promedio diario)
    PERFIL_HORA = {
        4: 0.05, 5: 0.12, 6: 0.35, 7: 0.85, 8: 1.00, 9: 0.70,
        10: 0.55, 11: 0.50, 12: 0.60, 13: 0.65, 14: 0.55, 15: 0.50,
        16: 0.58, 17: 0.80, 18: 0.95, 19: 0.85, 20: 0.65, 21: 0.45,
        22: 0.30, 23: 0.15,
    }
    LINEAS = {
        "A": 35_000,   # pasajeros diarios base Línea A
        "B": 14_000,   # pasajeros diarios base Línea B
        "J": 4_500,    # cable J
        "K": 5_000,    # cable K
    }

    for anio in (2022, 2023, 2024):
        ruta = out / f"afluencia_metro_{anio}.csv"
        if ruta.exists() and ruta.stat().st_size > 0:
            print(f"  ⚠ Existe: {ruta.name}")
            continue
        with open(ruta, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["fecha", "linea", "hora", "pasajeros"])
            d = datetime(anio, 1, 1)
            fin = datetime(anio, 12, 31)
            while d <= fin:
                factor_dia = 1.0 if d.weekday() < 5 else 0.55
                factor_mes = 1.0 + 0.08 * math.sin(2 * math.pi * (d.month - 3) / 12)
                for linea, base_diario in LINEAS.items():
                    for hora, factor_hora in PERFIL_HORA.items():
                        pax = int(
                            base_diario * factor_dia * factor_mes
                            * factor_hora * random.uniform(0.88, 1.12)
                        )
                        w.writerow([d.strftime("%Y-%m-%d"), linea, hora, max(0, pax)])
                d += timedelta(days=1)
        print(f"  ✓ {ruta} ({ruta.stat().st_size//1024} KB)")

    # GTFS-lite: solo stops.txt + routes.txt
    gtfs = out.parent / "metro_gtfs" / "2024"
    gtfs.mkdir(parents=True, exist_ok=True)

    stops = gtfs / "stops.txt"
    if not stops.exists() or stops.stat().st_size == 0:
        with open(stops, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["stop_id", "stop_name", "stop_lat", "stop_lon"])
            for eid, nom, _, lat, lon in ESTACIONES_METRO:
                w.writerow([eid, nom, lat, lon])
        print(f"  ✓ {stops}")

    routes = gtfs / "routes.txt"
    if not routes.exists() or routes.stat().st_size == 0:
        with open(routes, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["route_id", "route_short_name", "route_type"])
            w.writerow(["A", "Metro Línea A", "1"])
            w.writerow(["B", "Metro Línea B", "1"])
        print(f"  ✓ {routes}")


# ── 2. EnCicla ─────────────────────────────────────────────────────────────
ESTACIONES_ENCICLA = [
    # 90 estaciones representativas. (id, nombre, lat, lon, capacidad)
    *[(f"ENC{i:03d}", f"Estación EnCicla {i}",
       6.18 + random.uniform(-0.05, 0.10),
       -75.59 + random.uniform(-0.05, 0.05),
       random.choice([10, 12, 15, 18, 20])) for i in range(1, 91)]
]

def gen_encicla():
    out = RAW / "encicla_estaciones"
    out.mkdir(parents=True, exist_ok=True)
    fest = out / "estaciones_encicla.json"
    if not fest.exists() or fest.stat().st_size == 0:
        records = [
            {
                "_id": i + 1,
                "estacion_id": e[0],
                "nombre": e[1],
                "latitud": e[2],
                "longitud": e[3],
                "capacidad_anclajes": e[4],
                "estado": "activa",
            }
            for i, e in enumerate(ESTACIONES_ENCICLA)
        ]
        payload = {"result": {"records": records, "total": len(records)}}
        fest.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"  ✓ {fest}  ({len(records)} estaciones)")

    # Histórico de préstamos (sintético) — 6 meses, ~80 préstamos/día
    out_p = RAW / "encicla_prestamos"
    out_p.mkdir(parents=True, exist_ok=True)
    ruta = out_p / "prestamos_encicla_2024.csv"
    if ruta.exists() and ruta.stat().st_size > 0:
        print(f"  ⚠ Existe: {ruta.name}")
        return
    with open(ruta, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "id_viaje", "id_usuario", "estacion_origen", "estacion_destino",
            "ts_inicio", "ts_fin", "duracion_min",
        ])
        d0 = datetime(2024, 1, 1)
        viaje = 0
        for delta_dia in range(180):
            dia = d0 + timedelta(days=delta_dia)
            # más viajes en días laborales y temperatura "normal"
            n_viajes = random.randint(60, 120) if dia.weekday() < 5 else random.randint(20, 60)
            for _ in range(n_viajes):
                viaje += 1
                origen = random.choice(ESTACIONES_ENCICLA)[0]
                destino = random.choice(ESTACIONES_ENCICLA)[0]
                while destino == origen:
                    destino = random.choice(ESTACIONES_ENCICLA)[0]
                hora_inicio = random.choices(
                    population=list(range(6, 22)),
                    weights=[2, 3, 8, 10, 6, 4, 5, 6, 7, 6, 5, 4, 6, 9, 8, 5],
                    k=1,
                )[0]
                ts_inicio = dia.replace(hour=hora_inicio, minute=random.randint(0, 59))
                duracion = max(3, int(random.gauss(18, 9)))
                ts_fin = ts_inicio + timedelta(minutes=duracion)
                user = f"u{random.randint(10000, 99999)}"
                w.writerow([
                    viaje, user, origen, destino,
                    ts_inicio.isoformat(timespec="seconds"),
                    ts_fin.isoformat(timespec="seconds"),
                    duracion,
                ])
    print(f"  ✓ {ruta}  ({ruta.stat().st_size//1024} KB)")


# ── 3. SIATA ────────────────────────────────────────────────────────────────
ESTACIONES_SIATA = [
    # (id, nombre, zona, lat, lon)
    ("ESP_AEROPUERTO",   "Aeropuerto",       "valle_aburra_norte",   6.220, -75.590),
    ("ESP_POBLADO",      "El Poblado",       "valle_aburra_sur",     6.210, -75.570),
    ("ESP_BELEN",        "Belén",            "valle_aburra_centro",  6.230, -75.610),
    ("ESP_CENTRO",       "Centro",           "valle_aburra_centro",  6.250, -75.570),
    ("ESP_LAURELES",     "Laureles",         "valle_aburra_centro",  6.245, -75.595),
    ("ESP_BELLO",        "Bello",            "valle_aburra_norte",   6.330, -75.560),
    ("ESP_ITAGUI",       "Itagüí",           "valle_aburra_sur",     6.165, -75.605),
    ("ESP_CALDAS",       "Caldas",           "valle_aburra_sur",     6.090, -75.640),
    ("ESP_MANRIQUE",     "Manrique",         "valle_aburra_nororiental", 6.275, -75.555),
    ("ESP_ARANJUEZ",     "Aranjuez",         "valle_aburra_nororiental", 6.270, -75.560),
]

def gen_siata():
    """Genera archivos SIATA en el formato que espera ingest_siata.py (post Sprint 4).

    Produce:
      - siata_pm25_horario.csv   — long: timestamp, estacion_id, variable, valor
      - siata_pm10_horario.csv   — idem para PM10
      - siata_estaciones.tab     — TSV: codigo, nombre_completo, nombre_corto, lat, lon, municipio
    """
    out = RAW / "siata_historico"
    out.mkdir(parents=True, exist_ok=True)

    # Eliminar archivo con nombre incorrecto si existe
    viejo = out / "siata_pm25_horario_2023.csv"
    if viejo.exists():
        viejo.unlink()

    # TAB de estaciones (nombre_corto = estacion_id en los CSV)
    ruta_tab = out / "siata_estaciones.tab"
    if not ruta_tab.exists() or ruta_tab.stat().st_size == 0:
        with open(ruta_tab, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter="\t")
            w.writerow(["codigo", "nombre_completo", "nombre_corto", "latitud", "longitud", "municipio"])
            municipios = {
                "ESP_AEROPUERTO": ("Aeropuerto Olaya Herrera", "Medellín"),
                "ESP_POBLADO":    ("El Poblado",               "Medellín"),
                "ESP_BELEN":      ("Belén",                    "Medellín"),
                "ESP_CENTRO":     ("Centro de Medellín",       "Medellín"),
                "ESP_LAURELES":   ("Laureles",                 "Medellín"),
                "ESP_BELLO":      ("Bello Centro",             "Bello"),
                "ESP_ITAGUI":     ("Itagüí Centro",            "Itagüí"),
                "ESP_CALDAS":     ("Caldas",                   "Caldas"),
                "ESP_MANRIQUE":   ("Manrique",                 "Medellín"),
                "ESP_ARANJUEZ":   ("Aranjuez",                 "Medellín"),
            }
            for eid, nom, zona, lat, lon in ESTACIONES_SIATA:
                nombre_largo, mpio = municipios.get(eid, (nom, "Medellín"))
                # codigo puede ser igual al nombre_corto para sintético
                w.writerow([eid, nombre_largo, eid, lat, lon, mpio])
        print(f"  ✓ {ruta_tab.name}")

    d0 = datetime(2023, 1, 1)
    horas_totales = 365 * 24

    # Generar PM2.5 y PM10 en formato long
    for variable, escala in [("pm25", 1.0), ("pm10", 1.7)]:
        ruta = out / f"siata_{variable}_horario.csv"
        if ruta.exists() and ruta.stat().st_size > 0:
            print(f"  ⚠ Existe: {ruta.name}")
            continue
        with open(ruta, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["timestamp", "estacion_id", "variable", "valor"])
            for h in range(horas_totales):
                ts = d0 + timedelta(hours=h)
                mes_factor = 1.4 if ts.month in (2, 3) else 1.0 if ts.month in (10, 11) else 0.8
                hora_factor = 1.5 if ts.hour in (7, 8, 18, 19) else 0.9
                for eid, *_ in ESTACIONES_SIATA:
                    base = 22 * mes_factor * hora_factor * escala
                    if eid in ("ESP_CENTRO", "ESP_AEROPUERTO"):
                        base *= 1.25
                    valor = max(5.0, random.gauss(base, 9 * escala))
                    # SIATA usa -999 como centinela de nulo
                    if random.random() < 0.005:
                        valor = -999.0
                    w.writerow([ts.isoformat(timespec="seconds"), eid, variable, round(valor, 2)])
        print(f"  ✓ {ruta.name}  ({ruta.stat().st_size//1024} KB)")


def main():
    print("Generando muestras sintéticas para fuentes con descarga bloqueada...")
    print()
    print("[Metro Afluencia + GTFS]")
    gen_metro_afluencia()
    print()
    print("[EnCicla estaciones + préstamos]")
    gen_encicla()
    print()
    print("[SIATA PM2.5 horario]")
    gen_siata()
    print()
    print("✅ Muestras listas en data/raw/")
    print()
    print("Para reemplazar por datos reales, sobrescribir los archivos en")
    print("  data/raw/metro_afluencia/, data/raw/encicla_*/ , data/raw/siata_historico/")


if __name__ == "__main__":
    sys.exit(main() or 0)
