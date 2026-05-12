"""
job_hibrido.py — Materializa la sección 4.3 de la propuesta: el percentil
histórico (Gold-derived) se convierte en un umbral operacional del sistema
de tiempo real.

Caso de uso:
    "Cuando llueva fuerte en alguna zona del Valle, ¿la afluencia del Metro
    en esa franja horaria está cayendo por debajo del p90 histórico para esa
    franja, sugiriendo que los usuarios están migrando a EnCicla/Metroplús?"

Cómo funciona:
    1. Al arranque carga los percentiles desde GOLD (Iceberg vía PyIceberg).
       Si la tabla `demo.pulsomed.gold.percentiles_metro` no existe o el REST
       Catalog no responde, cae al JSON precomputado
       `data/processed/percentiles_metro.json` (estrategia robusta).
    2. Suscribe a DOS tópicos:
         · `siata.lecturas`        → mantiene última lluvia/PM2.5 del valle.
         · `metro.validaciones`    → mantiene afluencia rolling 5min por línea.
    3. Cada `INTERVALO_EVAL_S` segundos evalúa:
         · Si está lloviendo (precip_mm_5min > UMBRAL_LLUVIA_MM)
         · Y la afluencia 5min de la línea < FACTOR × p_referencia(franja),
       emite alerta a `mongodb.pulsomed.alertas_hibridas`.

Migración Sprint 4 — Iceberg en vivo:
    En Sprint 3 este job leía SIEMPRE el JSON. Ahora intenta primero PyIceberg
    contra el REST Catalog (variable de entorno ICEBERG_REST_URI), y sólo si
    falla cae al JSON. Esto demuestra el valor del REST Catalog: el mismo
    Gold que produce `make build-gold` es consumido inmediatamente por un
    proceso streaming sin pasar por archivos intermedios.

Variables:
    INTERVALO_EVAL_S        (default 10)    cada cuánto evaluar el cruce
    UMBRAL_LLUVIA_MM        (default 0.3)   precipitación 5min mínima para alerta
    FACTOR_AFLUENCIA        (default 0.7)   afluencia < 0.7 × p_referencia → alerta
    PERCENTIL_REFERENCIA    (default p90)   cuál percentil usar (p75/p90/p95)
    FORZAR_FUENTE_JSON      (default "")    si =="1", se salta PyIceberg
                                            (útil para tests sin el catálogo)
"""

from __future__ import annotations

import json
import os
import signal
import sys
from collections import defaultdict, deque
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, "/workspace/src")

try:
    from kafka import KafkaConsumer
    from pymongo import MongoClient
except ImportError:
    print("ERROR: pip install kafka-python pymongo", flush=True)
    sys.exit(1)

from shared.config import (
    CATALOG,
    KAFKA_BOOTSTRAP,
    MONGO_DB,
    MONGO_URI,
    NS_GOLD,
    TBL_GOLD_PERCENTILES_METRO,
    TOPIC_METRO,
    TOPIC_SIATA,
    WAREHOUSE_PATH,
)

JSON_PERCENTILES = Path("/workspace/data/processed/percentiles_metro.json")
COL_HIBRIDAS = "alertas_hibridas"

INTERVALO_EVAL_S = int(os.getenv("INTERVALO_EVAL_S", "10"))
UMBRAL_LLUVIA_MM = float(os.getenv("UMBRAL_LLUVIA_MM", "0.3"))
FACTOR_AFLUENCIA = float(os.getenv("FACTOR_AFLUENCIA", "0.7"))
PERCENTIL_REF = os.getenv("PERCENTIL_REFERENCIA", "p90")
FORZAR_FUENTE_JSON = os.getenv("FORZAR_FUENTE_JSON", "") == "1"
ICEBERG_REST_URI = os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181")
ICEBERG_S3_ENDPOINT = os.getenv("ICEBERG_S3_ENDPOINT", "http://minio:9000")

DETENER = False


def _handler(_sig, _frame):
    global DETENER
    DETENER = True


signal.signal(signal.SIGINT, _handler)
signal.signal(signal.SIGTERM, _handler)


def _franja(hora: int) -> str:
    if 5 <= hora <= 8:
        return "punta_am"
    if 9 <= hora <= 11:
        return "valle_am"
    if 12 <= hora <= 13:
        return "almuerzo"
    if 14 <= hora <= 16:
        return "valle_pm"
    if 17 <= hora <= 20:
        return "punta_pm"
    return "nocturno"


def _cargar_percentiles_desde_iceberg() -> dict[str, dict[str, dict]] | None:
    """Intenta cargar `demo.pulsomed.gold.percentiles_metro` vía PyIceberg.

    Devuelve el mismo formato {linea: {franja: {p50,p75,p90,p95,muestras}}}
    que el JSON, o None si la tabla no existe / falla la conexión.
    """
    try:
        from pyiceberg.catalog import load_catalog
    except ImportError:
        print("  ⚠ pyiceberg no instalado — fallback a JSON", flush=True)
        return None

    try:
        catalog = load_catalog(
            "default",
            **{
                "type": "rest",
                "uri": ICEBERG_REST_URI,
                "s3.endpoint": ICEBERG_S3_ENDPOINT,
                "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID", ""),
                "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
                "s3.region": os.getenv("AWS_REGION", "us-east-1"),
                "warehouse": WAREHOUSE_PATH,
            },
        )
        tabla_corta = TBL_GOLD_PERCENTILES_METRO.split(".", 1)[1]  # "pulsomed.gold.percentiles_metro"
        tabla = catalog.load_table(tabla_corta)
        df = tabla.scan().to_arrow().to_pandas()
    except Exception as exc:
        print(f"  ⚠ PyIceberg falló ({type(exc).__name__}: {exc}) — fallback a JSON", flush=True)
        return None

    if df.empty:
        print("  ⚠ tabla gold.percentiles_metro vacía — fallback a JSON", flush=True)
        return None

    salida: dict[str, dict[str, dict]] = {}
    for _, row in df.iterrows():
        linea = row["linea"]
        franja = row["franja_horaria"]
        salida.setdefault(linea, {})[franja] = {
            "p50": int(row["p50"]),
            "p75": int(row["p75"]),
            "p90": int(row["p90"]),
            "p95": int(row["p95"]),
            "muestras": int(row["muestras"]),
        }
    print(
        f"→ Percentiles cargados desde ICEBERG ({TBL_GOLD_PERCENTILES_METRO}): "
        f"{len(salida)} líneas, {len(df)} filas",
        flush=True,
    )
    return salida


def _cargar_percentiles_desde_json() -> dict[str, dict[str, dict]]:
    if not JSON_PERCENTILES.exists():
        print(f"ERROR: falta {JSON_PERCENTILES} y PyIceberg también falló.", flush=True)
        print("Correr: make build-gold  o  make exportar-referencias", flush=True)
        sys.exit(2)
    data = json.loads(JSON_PERCENTILES.read_text(encoding="utf-8"))
    valores = data.get("valores", {})
    print(f"→ Percentiles cargados desde JSON precomputado: {len(valores)} líneas", flush=True)
    return valores


def _cargar_percentiles() -> dict[str, dict[str, dict]]:
    if FORZAR_FUENTE_JSON:
        print("→ FORZAR_FUENTE_JSON=1 — saltando PyIceberg", flush=True)
        return _cargar_percentiles_desde_json()
    desde_iceberg = _cargar_percentiles_desde_iceberg()
    if desde_iceberg is not None:
        return desde_iceberg
    return _cargar_percentiles_desde_json()


class VentanaRolling5Min:
    """Buffer de eventos con timestamp; descarta los más viejos que 5min."""

    VENTANA = timedelta(minutes=5)

    def __init__(self) -> None:
        self.eventos: deque[tuple[datetime, int]] = deque()

    def agregar(self, ts: datetime, valor: int) -> None:
        self.eventos.append((ts, valor))
        self._purgar()

    def _purgar(self) -> None:
        if not self.eventos:
            return
        ahora = self.eventos[-1][0]
        limite = ahora - self.VENTANA
        while self.eventos and self.eventos[0][0] < limite:
            self.eventos.popleft()

    def suma(self) -> int:
        return sum(v for _, v in self.eventos)

    def cuenta(self) -> int:
        return len(self.eventos)


def main() -> int:
    percentiles = _cargar_percentiles()
    print(f"→ Configuración:  factor_afluencia={FACTOR_AFLUENCIA}  lluvia≥{UMBRAL_LLUVIA_MM}mm  ref={PERCENTIL_REF}", flush=True)

    consumer = KafkaConsumer(
        TOPIC_METRO, TOPIC_SIATA,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="pulsomed-hibrido",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=2000,
    )
    mongo = MongoClient(MONGO_URI)
    coll = mongo[MONGO_DB][COL_HIBRIDAS]
    coll.create_index([("linea", 1), ("ventana_inicio", 1)], unique=True)

    ventanas: dict[str, VentanaRolling5Min] = defaultdict(VentanaRolling5Min)
    lluvia_mm_5min: float = 0.0
    pm25_red: float = 0.0
    ultima_eval = datetime.utcnow()
    eventos_consumidos = 0

    print(f"→ Esperando eventos en {TOPIC_METRO} + {TOPIC_SIATA}…", flush=True)

    while not DETENER:
        try:
            for msg in consumer:
                eventos_consumidos += 1
                v = msg.value
                topico = msg.topic
                if topico == TOPIC_METRO:
                    linea = (v.get("linea") or "").strip()
                    if not linea:
                        continue
                    pax = int(v.get("pasajeros_periodo") or 0)
                    try:
                        ts = datetime.fromisoformat(v.get("timestamp", "").replace("Z", ""))
                    except Exception:
                        ts = datetime.utcnow()
                    ventanas[linea].agregar(ts, pax)
                elif topico == TOPIC_SIATA:
                    precip = v.get("precipitacion_mm")
                    pm = v.get("pm25")
                    if precip is not None:
                        try:
                            lluvia_mm_5min = float(precip)
                        except (TypeError, ValueError):
                            pass
                    if pm is not None:
                        try:
                            pm25_red = float(pm)
                        except (TypeError, ValueError):
                            pass
                if DETENER:
                    break
        except Exception as exc:
            print(f"  ⚠ error consumiendo: {exc}", flush=True)

        # Evaluar el cruce cada INTERVALO_EVAL_S segundos
        ahora = datetime.utcnow()
        if (ahora - ultima_eval).total_seconds() < INTERVALO_EVAL_S:
            continue
        ultima_eval = ahora

        if lluvia_mm_5min < UMBRAL_LLUVIA_MM:
            continue  # sin lluvia, no es escenario híbrido

        for linea, ventana in ventanas.items():
            if ventana.cuenta() == 0:
                continue
            afluencia_5min = ventana.suma()
            ref = percentiles.get(linea, {})
            franja = _franja(ahora.hour)
            stats = ref.get(franja)
            if not stats:
                continue
            # Convertir referencia (pasajeros/hora) a equivalente 5min
            referencia_5min = stats.get(PERCENTIL_REF, 0) * (5 / 60)
            if referencia_5min <= 0:
                continue
            ratio = afluencia_5min / referencia_5min
            if ratio >= FACTOR_AFLUENCIA:
                continue
            doc = {
                "linea": linea,
                "franja_horaria": franja,
                "ventana_inicio": ahora.replace(second=0, microsecond=0),
                "ventana_fin": ahora.replace(second=0, microsecond=0) + timedelta(minutes=5),
                "afluencia_5min_actual": afluencia_5min,
                "afluencia_5min_referencia": int(referencia_5min),
                "ratio_vs_referencia": round(ratio, 3),
                "percentil_referencia": PERCENTIL_REF,
                "lluvia_mm": lluvia_mm_5min,
                "pm25_red": pm25_red,
                "tipo": "ALERTA_HIBRIDA",
                "hipotesis": "afluencia_baja_durante_lluvia",
                "factor_umbral": FACTOR_AFLUENCIA,
                "emitido_en": ahora,
            }
            try:
                coll.insert_one(doc)
                print(
                    f"  🌧️🚇 HÍBRIDA  {linea:<12}  franja={franja:<10}  "
                    f"afluencia={afluencia_5min:<6}  ref={int(referencia_5min):<6}  "
                    f"ratio={ratio:.2f}  lluvia={lluvia_mm_5min:.1f}mm",
                    flush=True,
                )
            except Exception as exc:
                m = str(exc).lower()
                if "duplicate" not in m and "e11000" not in m:
                    print(f"  ⚠ Mongo: {exc}", flush=True)

    consumer.close()
    mongo.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
