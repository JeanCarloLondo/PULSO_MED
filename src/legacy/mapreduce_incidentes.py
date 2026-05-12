"""
mapreduce_incidentes.py — Job MapReduce (mrjob) que procesa los CSV LEGACY de
incidentes viales con DOS esquemas distintos (pre/post 2017, ambos sin
encabezado) y emite un único schema canónico, deduplicado por nro_radicado.

Este es el job pedido por el Módulo 01 (Arqueología de datos) de la propuesta.

Cómo funciona:

    Mapper:
      · Detecta el esquema mirando la fecha del registro:
          - dd/mm/yyyy hh:mm:ss   → ESQUEMA VIEJO (7 columnas)
          - ISO 8601 (yyyy-MM-ddT...) → ESQUEMA NUEVO (8 columnas)
      · Convierte la fecha a un formato canónico (yyyy-MM-dd hh:mm:ss).
      · Parsea ubicación según convención del esquema:
          - viejo: 'lon|lat' separado por pipe
          - nuevo: '[lon, lat]' string
      · Emite (nro_radicado, registro_normalizado_tsv)

    Reducer:
      · Si un nro_radicado aparece en ambos esquemas, prefiere el NUEVO
        (tiene comuna explícita y mejor formato de coords).
      · Emite registros únicos.

Schema canónico de salida (TSV, sin encabezado):
    nro_radicado \\t fecha \\t anio \\t mes \\t clase \\t gravedad
                 \\t barrio \\t comuna \\t direccion \\t longitud \\t latitud

Ejecutar en local (modo inline, equivalente al stand-alone runner):

    python src/legacy/mapreduce_incidentes.py \\
        data/raw/medata_legacy/incidentes_pre2017.csv \\
        data/raw/medata_legacy/incidentes_post2017.csv \\
        --output-dir data/processed/incidentes_normalizados \\
        --no-output

Para correr sobre Hadoop real (cuando el contenedor exista):

    python src/legacy/mapreduce_incidentes.py -r hadoop \\
        hdfs:///pulsomed/incidentes_pre2017.csv \\
        hdfs:///pulsomed/incidentes_post2017.csv

Dependencias:
    pip install mrjob
"""

from __future__ import annotations

import csv
import io
import re
import sys

try:
    from mrjob.job import MRJob
    from mrjob.step import MRStep
except ImportError:
    print(
        "ERROR: falta mrjob. Instalar:\n"
        "    pip install mrjob\n"
        "    o, dentro del contenedor spark-iceberg:\n"
        "    docker compose exec spark-iceberg pip install mrjob",
        file=sys.stderr,
    )
    sys.exit(1)


RE_FECHA_VIEJA = re.compile(r"^\s*(\d{2})/(\d{2})/(\d{4})\s+(\d{2}:\d{2}:\d{2})")
RE_FECHA_ISO = re.compile(r"^\s*(\d{4})-(\d{2})-(\d{2})T(\d{2}:\d{2}:\d{2})")


def _normalizar_fecha(valor: str) -> tuple[str | None, int | None, int | None]:
    """Devuelve (fecha_canonica yyyy-MM-dd hh:mm:ss, año, mes) o (None, None, None)."""
    if not valor:
        return None, None, None
    m = RE_FECHA_ISO.match(valor)
    if m:
        anio, mes, dia, hora = m.group(1), m.group(2), m.group(3), m.group(4)
        return f"{anio}-{mes}-{dia} {hora}", int(anio), int(mes)
    m = RE_FECHA_VIEJA.match(valor)
    if m:
        dia, mes, anio, hora = m.group(1), m.group(2), m.group(3), m.group(4)
        return f"{anio}-{mes}-{dia} {hora}", int(anio), int(mes)
    return None, None, None


def _parsear_ubicacion_vieja(valor: str) -> tuple[str, str]:
    """'-75.56|6.24' → ('-75.56', '6.24'). Devuelve ('','') si no parsea."""
    if not valor or "|" not in valor:
        return "", ""
    a, b = valor.split("|", 1)
    return a.strip(), b.strip()


def _parsear_location_nueva(valor: str) -> tuple[str, str]:
    """'[-75.5688, 6.2431]' → ('-75.5688', '6.2431')."""
    if not valor:
        return "", ""
    s = valor.replace("[", "").replace("]", "").replace(" ", "")
    if "," not in s:
        return "", ""
    a, b = s.split(",", 1)
    return a.strip(), b.strip()


class IncidentesNormalizador(MRJob):
    """Mapper detecta esquema, reducer deduplica por nro_radicado."""

    # Salida TSV plana (lo que se ingiere a Bronze después).
    SORT_VALUES = True

    def mapper(self, _key, raw_line: str):
        if not raw_line.strip():
            return

        # Parsear como CSV — la línea puede contener comas dentro de campos
        try:
            campos = next(csv.reader(io.StringIO(raw_line)))
        except (csv.Error, StopIteration):
            return
        if not campos:
            return

        n = len(campos)
        if n == 7:
            # Esquema VIEJO: nro_radicado, fecha, clase, gravedad, barrio, direccion, ubicacion
            nro = campos[0].strip()
            fecha_raw, clase, gravedad, barrio, direccion, ubic = campos[1:7]
            comuna = ""  # no existe en el viejo
            lon, lat = _parsear_ubicacion_vieja(ubic)
            origen = "pre2017"
        elif n == 8:
            # Esquema NUEVO: nro_radicado, fecha, clase, gravedad, barrio, comuna, direccion, location
            nro = campos[0].strip()
            fecha_raw, clase, gravedad, barrio, comuna, direccion, loc = campos[1:8]
            lon, lat = _parsear_location_nueva(loc)
            origen = "post2017"
        else:
            # Línea malformada: la ignoramos pero la contamos
            self.increment_counter("calidad", "lineas_malformadas", 1)
            return

        if not nro:
            self.increment_counter("calidad", "sin_radicado", 1)
            return

        fecha_canonica, anio, mes = _normalizar_fecha(fecha_raw)
        if fecha_canonica is None:
            self.increment_counter("calidad", "fecha_no_reconocida", 1)
            return

        registro = {
            "nro_radicado": nro,
            "fecha": fecha_canonica,
            "anio": anio,
            "mes": mes,
            "clase": clase.strip(),
            "gravedad": gravedad.strip(),
            "barrio": barrio.strip(),
            "comuna": comuna.strip(),
            "direccion": direccion.strip(),
            "longitud": lon,
            "latitud": lat,
            "origen_esquema": origen,
        }
        self.increment_counter("entrada", f"esquema_{origen}", 1)
        yield nro, registro

    def reducer(self, nro, registros):
        # Si hay duplicados con esquemas distintos, ganar el más nuevo (más info).
        seleccionado = None
        n_dups = 0
        for r in registros:
            n_dups += 1
            if seleccionado is None:
                seleccionado = r
                continue
            if seleccionado.get("origen_esquema") == "pre2017" and r.get("origen_esquema") == "post2017":
                seleccionado = r
        if n_dups > 1:
            self.increment_counter("salida", "duplicados_resueltos", 1)
        self.increment_counter("salida", "registros_emitidos", 1)

        # TSV plano: ordenado conforme al schema canónico documentado arriba.
        campos_ordenados = (
            seleccionado["nro_radicado"], seleccionado["fecha"],
            str(seleccionado["anio"]), str(seleccionado["mes"]),
            seleccionado["clase"], seleccionado["gravedad"],
            seleccionado["barrio"], seleccionado["comuna"],
            seleccionado["direccion"],
            seleccionado["longitud"], seleccionado["latitud"],
        )
        yield None, "\t".join(campos_ordenados)

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]


if __name__ == "__main__":
    IncidentesNormalizador.run()
