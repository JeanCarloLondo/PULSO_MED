# Notebooks

Notebooks Jupyter para EDA, validación de capas, demos y entregables.

| Notebook | Sprint | Para qué |
|----------|--------|----------|
| `01_eda_bronze.ipynb` | 1 | Ver qué llegó a Bronze, validar conteos por fuente |
| `02_eda_silver.ipynb` | 1 | Validar joins espaciales y temporales |
| `03_eda_gold.ipynb` | 1 | Responder preguntas B-1..B-4 con gráficas |
| `04_streaming_alertas.ipynb` | 3 | Consultar MongoDB de las últimas alertas RT |
| `05_ml_fatalidad.ipynb` | 5 | Modelo MLlib de fatalidad en incidentes |
| `06_graphx_metro.ipynb` | 5 | Rutas mínimas en la red Metro con GraphFrames |

## Cómo correr

Los notebooks viven en este folder y están montados en el contenedor de Spark
en `/home/iceberg/notebooks`. Después de `make up`, abrir Jupyter Lab en
http://localhost:8888 y navegar a `/notebooks/`.
