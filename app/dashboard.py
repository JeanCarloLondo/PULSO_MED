"""
dashboard.py — Tablero Streamlit para Pulso Medellín · Sprint 3.

Lee MongoDB cada 5 segundos y muestra el estado actual del sistema:
  · Alertas PM2.5  (S-2)
  · Disponibilidad EnCicla con mapa  (S-1)
  · Afluencia Metro RT por línea  (S-4)
  · Alertas SIMM corredor activo  (S-3)
  · Alertas híbridas batch↔streaming  (sección 4.3 propuesta)

Uso:
    pip install streamlit pymongo pandas pydeck
    streamlit run app/dashboard.py

Variables:
    MONGO_HOST              (default "localhost", desde host)
    MONGO_PORT              (default "27017")
    MONGO_INITDB_ROOT_USERNAME / _PASSWORD
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import pandas as pd
import pydeck as pdk
import streamlit as st
from pymongo import MongoClient

# ── Conexión Mongo ────────────────────────────────────────────────────────────

MONGO_USER = quote_plus(os.getenv("MONGO_INITDB_ROOT_USERNAME", "admin"))
MONGO_PASS = quote_plus(os.getenv("MONGO_INITDB_ROOT_PASSWORD", "admin12345"))
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
MONGO_DB = "pulsomed"
MONGO_URI = (
    f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}?authSource=admin"
)


@st.cache_resource
def _conectar() -> MongoClient:
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)


def _df_desde(coll_name: str, filtro: dict | None = None, limite: int = 200, sort_field: str = "emitido_en") -> pd.DataFrame:
    cli = _conectar()
    cur = cli[MONGO_DB][coll_name].find(filtro or {}, {"_id": 0}).sort(sort_field, -1).limit(limite)
    docs = list(cur)
    if not docs:
        return pd.DataFrame()
    return pd.DataFrame(docs)


# ── Layout ────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="Pulso Medellín", layout="wide", initial_sidebar_state="collapsed")
st.title("Pulso Medellín · Tablero operacional")
st.caption(
    "Streaming de las 6 fuentes de movilidad del Valle de Aburrá. "
    "Refresca cada 5 segundos. Los productores deben estar corriendo (`make pipeline-streaming-completo`)."
)

with st.sidebar:
    st.header("Filtros")
    minutos = st.slider("Mostrar últimos N minutos", 5, 120, 15, step=5)
    refresh_s = st.slider("Refresco automático (s)", 0, 30, 5, step=1)
    st.markdown("---")
    st.markdown("**Sprints cubiertos**")
    st.markdown(
        "- S-1 (EnCicla disp.)\n"
        "- S-2 (alerta PM2.5)\n"
        "- S-3 (corredor SIMM)\n"
        "- S-4 (afluencia Metro)\n"
        "- 4.3 Job híbrido"
    )

corte = datetime.utcnow() - timedelta(minutes=minutos)
filtro_tiempo = {"emitido_en": {"$gte": corte}}

# Fila superior — KPIs
col1, col2, col3, col4, col5 = st.columns(5)
df_alertas_aire = _df_desde("alertas_aire", filtro_tiempo)
df_encicla = _df_desde("disponibilidad_encicla", filtro_tiempo)
df_metro = _df_desde("afluencia_metro_rt", filtro_tiempo)
df_simm = _df_desde("aforos_corredor", filtro_tiempo)
df_hibridas = _df_desde("alertas_hibridas", filtro_tiempo)

col1.metric("PM2.5 alertas", len(df_alertas_aire))
col2.metric(
    "EnCicla disp. crítica",
    int(df_encicla["tipo"].eq("ALERTA_DISPONIBILIDAD").sum()) if not df_encicla.empty else 0,
)
col3.metric(
    "SIMM corredor riesgo",
    int(df_simm["tipo"].eq("ALERTA_CORREDOR_RIESGO").sum()) if not df_simm.empty else 0,
)
col4.metric("Metro snapshots", len(df_metro))
col5.metric("Híbridas batch↔stream", len(df_hibridas))

st.markdown("---")

# Sección S-1: mapa EnCicla
st.subheader("Disponibilidad EnCicla (S-1)")
if df_encicla.empty:
    st.info("Aún no hay snapshots de EnCicla. Lanzar el productor + job.")
else:
    ult = df_encicla.sort_values("emitido_en").drop_duplicates(["estacion_id"], keep="last").copy()
    ult["riesgo"] = ult["tipo"].eq("ALERTA_DISPONIBILIDAD")
    ult["radius_m"] = 80
    ult["color"] = ult["riesgo"].map(lambda b: [220, 38, 38, 220] if b else [34, 197, 94, 180])
    capa = pdk.Layer(
        "ScatterplotLayer",
        data=ult,
        get_position=["longitud", "latitud"],
        get_radius="radius_m",
        get_fill_color="color",
        pickable=True,
    )
    deck = pdk.Deck(
        layers=[capa],
        initial_view_state=pdk.ViewState(latitude=6.245, longitude=-75.59, zoom=11.2, pitch=0),
        tooltip={"text": "{nombre}\nbicis_min: {bicicletas_min}/{capacidad_anclajes}"},
    )
    st.pydeck_chart(deck)
    criticas = ult[ult["riesgo"]].sort_values("bicicletas_min")
    if not criticas.empty:
        st.write("Estaciones críticas:")
        st.dataframe(
            criticas[["estacion_id", "nombre", "bicicletas_min", "capacidad_anclajes", "ventana_inicio"]],
            hide_index=True,
            use_container_width=True,
        )

# Sección S-2: alertas aire
st.subheader("Alertas PM2.5 — calidad del aire (S-2)")
if df_alertas_aire.empty:
    st.info("Sin alertas en la ventana seleccionada.")
else:
    st.dataframe(
        df_alertas_aire[["zona", "ventana_inicio", "pm25_promedio", "gravedad", "lecturas_en_ventana"]]
        .head(20),
        hide_index=True,
        use_container_width=True,
    )

# Sección S-4: afluencia Metro RT
st.subheader("Afluencia Metro tiempo real (S-4)")
if df_metro.empty:
    st.info("Sin afluencia Metro en la ventana.")
else:
    pivot = df_metro.pivot_table(
        index="ventana_inicio", columns="linea", values="pasajeros_acumulados", aggfunc="sum"
    ).fillna(0)
    st.line_chart(pivot)

# Sección S-3: SIMM corredores
st.subheader("Corredores SIMM (S-3)")
if df_simm.empty:
    st.info("Sin lecturas SIMM en la ventana.")
else:
    alertas_simm = df_simm[df_simm["tipo"] == "ALERTA_CORREDOR_RIESGO"]
    if not alertas_simm.empty:
        st.warning(f"⚠ {len(alertas_simm)} alertas activas en corredores de alta siniestralidad")
    st.dataframe(
        df_simm[["corredor", "ventana_inicio", "intensidad_por_minuto", "tipo", "alta_siniestralidad"]]
        .head(30),
        hide_index=True,
        use_container_width=True,
    )

# Sección 4.3: alertas híbridas
st.subheader("Alertas híbridas batch↔streaming · sección 4.3 de la propuesta")
if df_hibridas.empty:
    st.info(
        "El job híbrido no ha emitido alertas en la ventana. Recordar: requiere "
        "que esté lloviendo (precip>0.3mm) y la afluencia caiga bajo p90 histórico."
    )
else:
    st.dataframe(
        df_hibridas[
            ["linea", "franja_horaria", "afluencia_5min_actual",
             "afluencia_5min_referencia", "ratio_vs_referencia",
             "lluvia_mm", "ventana_inicio"]
        ].head(20),
        hide_index=True,
        use_container_width=True,
    )

# Auto-refresh
if refresh_s > 0:
    st.markdown(
        f"<meta http-equiv='refresh' content='{refresh_s}'>",
        unsafe_allow_html=True,
    )
