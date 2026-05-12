"""
red_metro.py — Módulo 06b · Análisis de grafo de la red Metro del Valle de Aburrá.

Implementa PageRank y rutas óptimas (Dijkstra) sobre la topología de la red
Metro/Metrocable de Medellín usando PySpark DataFrames (estilo GraphFrames).

La topología es estática (fuente: datos abiertos Metro de Medellín — red 2024).
Los datos operacionales (afluencia) no tienen granularidad por estación, por lo
que el grafo captura la estructura de conectividad y el peso de los tiempos de
viaje entre estaciones adyacentes.

Análisis:
  1. PageRank (20 iter, damping=0.85) — ¿qué estaciones son más centrales?
  2. Rutas óptimas Dijkstra — tiempo mínimo entre todos los pares de estaciones

Salidas (Gold Iceberg):
  - gold.red_metro_pagerank       (id, nombre, linea, pagerank, ranking)
  - gold.red_metro_rutas_optimas  (origen, destino, tiempo_min, num_paradas, ruta)

Ejecutar:
    docker compose exec -T spark-iceberg python /workspace/src/batch/graph/red_metro.py
"""

from __future__ import annotations

import heapq
import sys
from collections import defaultdict

sys.path.insert(0, "/workspace/src")

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from shared.bronze_utils import log_ok, log_seccion
from shared.config import (
    TBL_GOLD_RED_METRO_PAGERANK,
    TBL_GOLD_RED_METRO_RUTAS,
    crear_spark_session,
)

# ── Topología de la red (fuente: datos abiertos Metro de Medellín, 2024) ──────

ESTACIONES = [
    # id,    nombre,           linea,  longitud,   latitud
    ("A01", "Niquía",         "A",    -75.5285,   6.3318),
    ("A02", "Bello",          "A",    -75.5440,   6.3138),
    ("A03", "Madera",         "A",    -75.5500,   6.2940),
    ("A04", "Acevedo",        "A",    -75.5565,   6.2793),
    ("A05", "Tricentenario",  "A",    -75.5617,   6.2704),
    ("A06", "Caribe",         "A",    -75.5677,   6.2613),
    ("A07", "Universidad",    "A",    -75.5747,   6.2554),
    ("A08", "Hospital",       "A",    -75.5770,   6.2498),
    ("A09", "Prado",          "A",    -75.5726,   6.2438),
    ("A10", "Parque Berrío",  "A",    -75.5697,   6.2354),
    ("A11", "San Antonio",    "A/B",  -75.5664,   6.2290),
    ("A12", "Alpujarra",      "A",    -75.5659,   6.2241),
    ("A13", "Exposiciones",   "A",    -75.5647,   6.2193),
    ("A14", "Industriales",   "A",    -75.5626,   6.2118),
    ("A15", "Poblado",        "A",    -75.5715,   6.2067),
    ("A16", "Aguacatala",     "A",    -75.5854,   6.1938),
    ("A17", "Ayurá",          "A",    -75.5987,   6.1840),
    ("A18", "Envigado",       "A",    -75.5955,   6.1756),
    ("A19", "Itagüí",         "A",    -75.6007,   6.1667),
    ("A20", "Sabaneta",       "A",    -75.6072,   6.1506),
    ("A21", "La Estrella",    "A",    -75.6151,   6.1381),
    # Línea B (comparte San Antonio = A11)
    ("B02", "Suramericana",   "B",    -75.5779,   6.2287),
    ("B03", "Estadio",        "B",    -75.5867,   6.2303),
    ("B04", "Floresta",       "B",    -75.5982,   6.2285),
    ("B05", "Santa Lucía",    "B",    -75.6069,   6.2270),
    ("B06", "San Javier",     "B/J",  -75.6134,   6.2264),
    # Línea J cable (comparte San Javier = B06)
    ("J02", "Juan XXIII",     "J",    -75.6205,   6.2342),
    ("J03", "Vallejuelos",    "J",    -75.6263,   6.2435),
    ("J04", "La Aurora",      "J/L",  -75.6265,   6.2550),
    # Línea K cable (comparte Acevedo = A04)
    ("K02", "Andalucía",      "K",    -75.5423,   6.2848),
    ("K03", "Villa Sierra",   "K",    -75.5361,   6.2903),
    ("K04", "Santo Domingo",  "K/M",  -75.5297,   6.2954),
    # Línea L cable (comparte La Aurora = J04)
    ("L02", "Miraflores",     "L",    -75.6220,   6.2680),
    # Línea M cable (comparte Santo Domingo = K04)
    ("M02", "Parque Arví",    "M",    -75.4897,   6.3163),
]

# Aristas dirigidas (bidireccionales al expandir): (src, dst, tiempo_min)
CONEXIONES = [
    # Línea A (norte → sur)
    ("A01", "A02", 3), ("A02", "A03", 3), ("A03", "A04", 3),
    ("A04", "A05", 2), ("A05", "A06", 2), ("A06", "A07", 2),
    ("A07", "A08", 2), ("A08", "A09", 2), ("A09", "A10", 2),
    ("A10", "A11", 2), ("A11", "A12", 2), ("A12", "A13", 2),
    ("A13", "A14", 3), ("A14", "A15", 3), ("A15", "A16", 4),
    ("A16", "A17", 4), ("A17", "A18", 3), ("A18", "A19", 3),
    ("A19", "A20", 3), ("A20", "A21", 4),
    # Línea B (San Antonio ↔ San Javier)
    ("A11", "B02", 3), ("B02", "B03", 3), ("B03", "B04", 3),
    ("B04", "B05", 3), ("B05", "B06", 3),
    # Línea J cable
    ("B06", "J02", 4), ("J02", "J03", 3), ("J03", "J04", 3),
    # Línea K cable
    ("A04", "K02", 4), ("K02", "K03", 3), ("K03", "K04", 4),
    # Línea L cable
    ("J04", "L02", 5),
    # Línea M cable (Arví, ida larga)
    ("K04", "M02", 15),
]


# ── Algoritmos de grafo (Python puro, red pequeña ≈ 34 nodos) ─────────────────

def _construir_adyacencia(estaciones, conexiones):
    """Grafo bidireccional como dict {id: [(vecino, tiempo_min)]}."""
    grafo = defaultdict(list)
    for src, dst, t in conexiones:
        grafo[src].append((dst, t))
        grafo[dst].append((src, t))
    return dict(grafo)


def _dijkstra(grafo, origen, ids_validos):
    """Distancias mínimas (tiempo_min) desde `origen` a todos los demás."""
    dist = {v: float("inf") for v in ids_validos}
    prev = {v: [] for v in ids_validos}
    dist[origen] = 0
    heap = [(0, origen, [origen])]

    while heap:
        d, u, camino = heapq.heappop(heap)
        if d > dist[u]:
            continue
        for v, w in grafo.get(u, []):
            nd = d + w
            if nd < dist[v]:
                dist[v] = nd
                prev[v] = camino + [v]
                heapq.heappush(heap, (nd, v, camino + [v]))

    return dist, prev


def _pagerank(grafo, ids, iteraciones=20, damping=0.85):
    """Power iteration PageRank. Grafo no dirigido → cada arista cuenta en ambas dir."""
    N = len(ids)
    rank = {v: 1.0 / N for v in ids}
    out_deg = {v: len(grafo.get(v, [])) for v in ids}

    for _ in range(iteraciones):
        nuevo = {}
        for v in ids:
            suma = sum(
                rank[u] / out_deg[u]
                for u, _ in grafo.get(v, [])
                if out_deg[u] > 0
            )
            nuevo[v] = (1 - damping) / N + damping * suma
        rank = nuevo

    return rank


# ── Builders de tablas Gold ────────────────────────────────────────────────────

def _gold_pagerank(spark, grafo, estaciones, conexiones):
    log_seccion("Gold · red_metro_pagerank (Módulo 06b)")
    ids = [e[0] for e in estaciones]
    pr = _pagerank(grafo, ids)

    filas = [
        {
            "id":        e_id,
            "nombre":    nombre,
            "linea":     linea,
            "longitud":  float(lon),
            "latitud":   float(lat),
            "pagerank":  float(pr[e_id]),
        }
        for e_id, nombre, linea, lon, lat in estaciones
    ]
    filas.sort(key=lambda r: -r["pagerank"])
    for i, f in enumerate(filas, 1):
        f["ranking"] = i

    schema = StructType([
        StructField("id",        StringType(),  False),
        StructField("nombre",    StringType(),  False),
        StructField("linea",     StringType(),  False),
        StructField("longitud",  DoubleType(),  False),
        StructField("latitud",   DoubleType(),  False),
        StructField("pagerank",  DoubleType(),  False),
        StructField("ranking",   IntegerType(), False),
    ])

    df = spark.createDataFrame(filas, schema=schema)
    n = df.count()
    df.writeTo(TBL_GOLD_RED_METRO_PAGERANK).using("iceberg").createOrReplace()
    log_ok(f"{TBL_GOLD_RED_METRO_PAGERANK}: {n} estaciones rankeadas")

    log_ok("Top 5 estaciones por PageRank:")
    df.orderBy("ranking").select("ranking", "nombre", "linea", "pagerank").show(5, truncate=False)
    return n


def _gold_rutas(spark, grafo, estaciones):
    log_seccion("Gold · red_metro_rutas_optimas (Módulo 06b)")
    ids = [e[0] for e in estaciones]
    nombre_por_id = {e[0]: e[1] for e in estaciones}

    filas = []
    for origen in ids:
        dist, rutas = _dijkstra(grafo, origen, ids)
        for destino in ids:
            if destino == origen:
                continue
            if dist[destino] == float("inf"):
                continue
            ruta = rutas[destino]
            filas.append({
                "origen":        nombre_por_id[origen],
                "origen_id":     origen,
                "destino":       nombre_por_id[destino],
                "destino_id":    destino,
                "tiempo_min":    int(dist[destino]),
                "num_paradas":   len(ruta) - 1,
                "ruta_ids":      ruta,
                "ruta_nombres":  [nombre_por_id[e] for e in ruta],
            })

    schema = StructType([
        StructField("origen",        StringType(),             False),
        StructField("origen_id",     StringType(),             False),
        StructField("destino",       StringType(),             False),
        StructField("destino_id",    StringType(),             False),
        StructField("tiempo_min",    IntegerType(),            False),
        StructField("num_paradas",   IntegerType(),            False),
        StructField("ruta_ids",      ArrayType(StringType()),  False),
        StructField("ruta_nombres",  ArrayType(StringType()),  False),
    ])

    df = spark.createDataFrame(filas, schema=schema)
    n = df.count()
    df.writeTo(TBL_GOLD_RED_METRO_RUTAS).using("iceberg").createOrReplace()
    log_ok(f"{TBL_GOLD_RED_METRO_RUTAS}: {n:,} pares origen-destino")

    log_ok("Ejemplo de rutas más largas (tiempo):")
    df.orderBy(F.desc("tiempo_min")).select(
        "origen", "destino", "tiempo_min", "num_paradas"
    ).show(5, truncate=False)
    return n


def main() -> int:
    log_seccion("Módulo 06b · Análisis de grafo — Red Metro Valle de Aburrá")

    spark = crear_spark_session("GraphMetro-Sprint5")
    spark.sparkContext.setLogLevel("WARN")

    grafo = _construir_adyacencia(ESTACIONES, CONEXIONES)
    log_ok(f"Red cargada: {len(ESTACIONES)} estaciones, {len(CONEXIONES)} conexiones directas")

    _gold_pagerank(spark, grafo, ESTACIONES, CONEXIONES)
    _gold_rutas(spark, grafo, ESTACIONES)

    log_seccion("✅ Módulo 06b Grafo completado — tablas Gold escritas")
    return 0


if __name__ == "__main__":
    sys.exit(main())
