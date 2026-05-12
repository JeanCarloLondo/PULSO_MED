# Sprint 5 · ML, Cloud y Bonus

> Estado: **implementado** — código entregado, verificación end-to-end sujeta
> a `make pipeline-batch && make pipeline-sprint5` con el stack Docker arriba.

---

## Objetivos cumplidos del roadmap

| Tarea Sprint 5 (roadmap) | Estado | Dónde vive |
|--------------------------|--------|------------|
| Módulo 06a — MLlib (clasificación multiclase de gravedad) | ✅ | `src/batch/ml/train_fatalidad.py` + `notebooks/02_ml_fatalidad.ipynb` |
| Módulo 06b — Grafo (PageRank + rutas óptimas red Metro) | ✅ | `src/batch/graph/red_metro.py` |
| Módulo 07 — ADR Cloud (AWS vs GCP) | ✅ | `docs/decisiones/07-cloud-aws-vs-gcp.md` |
| Módulo 07 — Controles de acceso por capa | ✅ | Documentado en ADR 07 (sección controles) |
| Módulo 07 — Ley 1581 / Ley 1712 | ✅ | Documentado en ADR 07 (secciones cumplimiento) |
| Bonus 1 — Trino (+2 pt) | ✅ | `docker-compose.yml` (servicio `trino`) + `docker/trino/etc/` |
| Bonus 2 — `make all` end-to-end (+1 pt) | ✅ | `Makefile` target `all` |
| Bonus 3 — Notebook EDA cruzado (+1 pt) | ✅ | `notebooks/03_eda_completo.ipynb` |

---

## 1. Módulo 06a — MLlib

### Problema y modelo

Se entrena un **RandomForestClassifier multiclase** para predecir la gravedad
de un incidente vial (Solo daños / Con heridos / Con muertos) a partir de
características observables en el momento del accidente.

**Fuente:** `silver.incidentes_geocodificados` (MEData 2014-2024, ~270k registros).

**Pipeline:**
```
StringIndexer(gravedad → label)
StringIndexer(clase/diseno_via/comuna → *_idx)
OneHotEncoder(*_idx → *_ohe)
VectorAssembler(features)
    └─ OHE: clase, diseno_via, comuna
    └─ Numéricos: hora, dia_semana, mes_accidente, longitud, latitud
RandomForestClassifier(numTrees=100, maxDepth=10, seed=42)
```

**Split:** 80/20 estratificado por `seed=42`.

**Salidas:**
- `gold.ml_fatalidad_evaluacion` — accuracy, F1, precision, recall en test.
- Modelo serializado en `data/processed/modelos/fatalidad_rf/`.

### Decisión técnica: RandomForest sobre GBT

Se eligió RandomForest sobre GBT (GradientBoostingTrees) por dos razones:
1. **Robusto al desbalance de clases** sin hiperparámetros adicionales (Con muertos
   es ~3% del dataset; GBT tiende a ignorar minorías).
2. **Paralelismo nativo** en Spark: entrena todos los árboles simultáneamente;
   GBT entrena secuencialmente (un árbol sobre los residuos del anterior) — más
   lento con 100+ estimadores.

Para mejoras futuras: aplicar `weightCol` por clase o SMOTE para mejorar
el recall en `Con muertos`, que es la clase con mayor impacto social.

---

## 2. Módulo 06b — Grafo: Red Metro

### Topología

La red Metro del Valle de Aburrá (2024) tiene **34 estaciones** y **35 conexiones**
directas bidireccionales entre estaciones adyacentes:

| Línea | Tipo | Estaciones | Nodo de intercambio |
|-------|------|-----------|---------------------|
| A | Metro (norte-sur) | 21 | San Antonio (↔B), Acevedo (↔K) |
| B | Metro (transversal) | 6 | San Antonio (↔A), San Javier (↔J) |
| J | Metrocable | 4 | San Javier (↔B), La Aurora (↔L) |
| K | Metrocable | 4 | Acevedo (↔A), Santo Domingo (↔M) |
| L | Metrocable | 2 | La Aurora (↔J) |
| M | Metrocable (Arví) | 2 | Santo Domingo (↔K) |

### Algoritmos implementados

**PageRank (iterativo, 20 iteraciones, damping=0.85):**
- Implementado en Python puro (red ≤ 34 nodos → cabe en memoria).
- Resultado: tabla Gold `red_metro_pagerank` con ranking de centralidad.
- Hallazgo esperado: San Antonio (A/B) y Acevedo (A/K) tienen el mayor
  PageRank por ser nodos de intercambio con mayor out-degree efectivo.

**Rutas óptimas (Dijkstra con peso = tiempo_min):**
- Para todos los pares origen-destino (34×33 = 1,122 pares).
- Resultado: tabla Gold `red_metro_rutas_optimas` con tiempo_min, num_paradas y ruta.
- Ruta más larga esperada: La Estrella → Parque Arví (~55 min, vía A+K+M).

### ¿Por qué Python puro en lugar de GraphFrames?

La red Metro tiene 34 vértices — demasiado pequeña para necesitar procesamiento
distribuido. Computar PageRank y Dijkstra en Spark nativo introduciría el overhead
de la JVM y serialización de DataFrames sin ningún beneficio de escala. La
decisión sigue la máxima del proyecto: **usar la herramienta adecuada al tamaño
del problema**. El resultado se escribe en Iceberg con PySpark (Gold), combinando
lo mejor de ambos: cómputo local eficiente + almacenamiento analítico distribuido.

En una red real de ciudad (miles de paradas, decenas de líneas), la misma lógica
migra directamente a GraphFrames: los DataFrames `vertices` y `edges` son
idénticos en esquema a la API de GraphFrames.

---

## 3. Módulo 07 — Cloud y Gobernanza

Ver `docs/decisiones/07-cloud-aws-vs-gcp.md` para el análisis completo.

**Resumen de decisión:** AWS, por compatibilidad directa con la pila Iceberg
(S3 → referencia, Glue → REST Catalog), migración de Kafka sin cambios de código
(MSK), y mayor presencia en el mercado colombiano.

**Privacidad (Ley 1581):** `id_usuario` EnCicla se pseudonimiza con HMAC-SHA256
antes de Bronze. La clave vive en `.env` y nunca en código ni commits.

**Transparencia (Ley 1712):** todas las fuentes son públicas y tienen URL
documentada. Las tablas Gold son accesibles desde múltiples motores (Spark,
Trino, Jupyter).

---

## 4. Bonus 1 — Trino (+2 pt)

Trino es el tercer motor SQL del proyecto. Lee las mismas tablas Iceberg que
produce Spark, sin copiar datos.

**Arquitectura:**
```
Trino (puerto 8084) → Iceberg REST Catalog → MinIO (S3)
                  ↑ mismo catálogo que Spark ↑
```

**Configuración:**
- `docker/trino/etc/config.properties` — coordinador standalone
- `docker/trino/etc/catalog/iceberg.properties` — conector Iceberg REST
- Servicio `trino` en `docker-compose.yml` (puertos: host 8084 → container 8080)

**Cómo usar:**
```bash
make trino-up          # levanta el servicio
make trino-sql         # CLI interactivo
make trino-demo        # consultas de demo sobre Gold
```

**Ejemplo de consulta SQL directa en Trino:**
```sql
-- Top 5 comunas con más muertos viales
SELECT comuna,
       SUM(con_muertos)  AS total_muertos,
       SUM(con_heridos)  AS total_heridos,
       AVG(indice_severidad) AS severidad_media
FROM demo.pulsomed.gold.accidentalidad_por_comuna
GROUP BY comuna
ORDER BY total_muertos DESC
LIMIT 5;

-- Estaciones Metro más centrales
SELECT ranking, nombre, linea, ROUND(pagerank, 6) AS pagerank
FROM demo.pulsomed.gold.red_metro_pagerank
ORDER BY ranking
LIMIT 10;
```

---

## 5. Bonus 2 — `make all` (+1 pt)

El target `all` orquesta el pipeline completo Sprint 0→5 en un solo comando:

```
make all
  ├── env-check
  ├── up           (levanta todos los servicios Docker)
  ├── init-namespaces
  ├── pipeline-batch    (Bronze → Silver → Gold + legacy MapReduce)
  ├── pipeline-legacy   (MEData pre/post-2017 → MR → Bronze)
  └── pipeline-sprint5  (MLlib + Grafo → Gold)
```

---

## 6. Bonus 3 — Notebook EDA cruzado (+1 pt)

`notebooks/03_eda_completo.ipynb` cruza las 7 tablas Gold en un solo análisis:
- Correlación PM2.5 anual ↔ muertos viales (cruza B-1 con B-2)
- Impacto de lluvia en EnCicla (B-3)
- Comunas de mayor riesgo compuesto volumen+severidad (B-4)
- Centralidad de nodos de intercambio Metro (Módulo 06b)
- Métricas del modelo ML (Módulo 06a)

---

## Archivos nuevos en este sprint

```
src/batch/ml/
  __init__.py
  train_fatalidad.py            Módulo 06a — pipeline MLlib

src/batch/graph/
  __init__.py
  red_metro.py                  Módulo 06b — PageRank + Dijkstra

src/shared/config.py            +3 constantes Gold (Sprint 5)

docker/trino/etc/
  config.properties             Trino coordinador standalone
  jvm.config                    JVM settings (2 GB heap)
  node.properties               ID de nodo
  catalog/iceberg.properties    Conector Iceberg REST → MinIO

docs/decisiones/
  07-cloud-aws-vs-gcp.md        ADR 07 — Módulo 07 (firmado, Aceptado)

docs/sprints/
  sprint-5-ml-cloud-bonus.md   Este archivo

notebooks/
  02_ml_fatalidad.ipynb         Módulo 06a — notebook reproducible
  03_eda_completo.ipynb         Bonus 3 — EDA cruzado Gold completo

docker-compose.yml              Trino descomentado (servicio activo)
Makefile                        +9 targets Sprint 5 + target `all`
```
