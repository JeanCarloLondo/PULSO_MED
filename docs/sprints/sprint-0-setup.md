# Sprint 0 · Setup & Foundations

> **Responsable principal:** todos los miembros deben correr esto en su máquina.
> **Definition of Done:** `make up && make smoke` retorna `0` en las tres máquinas del equipo.

---

## ¿Qué hacemos en este sprint?

**Una sola cosa:** dejar el stack del Lakehouse corriendo localmente con un solo comando, sin tocar datos reales todavía. Esto es la base sobre la que se construye todo lo demás. Si esto no funciona, los Sprints 1+ se vuelven imposibles de depurar.

**Lo que NO hacemos:**

-  Descargar datasets reales (eso es Sprint 1).
-  Levantar Kafka ni Flink (eso es Sprint 2; ya están comentados en el compose).
-  Escribir lógica de negocio (eso es Sprints 1+).

---

## Prerrequisitos en cada máquina del equipo

Antes de hacer nada, cada miembro necesita instalado:

| Herramienta | Versión mínima | Cómo verificar |
|-------------|----------------|----------------|
| Docker Engine | 24.x | `docker --version` |
| Docker Compose plugin | v2.20+ | `docker compose version` |
| GNU Make | 3.81+ | `make --version` |
| Git | 2.30+ | `git --version` |
| Python | 3.10+ | `python3 --version` *(solo para los smoke tests del host)* |

### Recursos mínimos recomendados

- **RAM:** 16 GB (8 GB es viable pero apretado; con Kafka/Flink en Sprint 2 se vuelve incómodo).
- **Disco libre:** 30 GB (los datasets de SIATA y MEData pesan).
- **CPU:** 4 cores+.

### Notas por sistema operativo

- **macOS (Apple Silicon):** Docker Desktop con la opción "Use Rosetta for x86/amd64 emulation" activada (Settings → General). Algunas imágenes de Iceberg/Spark son `linux/amd64` puras.
- **macOS (Intel):** sin requisitos especiales.
- **Windows:** **WSL2 obligatorio**. Todos los comandos `make` y `docker` se corren desde una terminal Ubuntu/WSL, NO desde PowerShell. El proyecto debe vivir DENTRO del filesystem de WSL (`~/proyectos/pulso-medellin`), no en `/mnt/c/...`, porque montar volúmenes desde NTFS es lentísimo.
- **Linux:** asegurarse de que el usuario está en el grupo `docker` (`sudo usermod -aG docker $USER` y reloguearse).

---

## Paso 1 — Clonar / colocar el proyecto

El proyecto vive dentro de la carpeta **`SISTEMAS INTENSIVOS DE DATOS/`**. Asumiendo que están trabajando en un único repo Git:

```bash
cd "<ruta-a-tu-carpeta-de-trabajo>/SISTEMAS INTENSIVOS DE DATOS"
# si todavía no existe el subfolder, copien el contenido del .zip aquí
ls pulso-medellin
```

Deben ver: `README.md`, `docker-compose.yml`, `Makefile`, `.env.example`, `docs/`, `src/`, etc.

> ** Importante:** **NO** trabajen dentro de la carpeta de "Reinforcement Learning". Esa es de otro proyecto. Toda la actividad de este curso vive bajo `SISTEMAS INTENSIVOS DE DATOS/pulso-medellin/`.

---

## Paso 2 — Configurar variables de entorno

```bash
cd pulso-medellin
cp .env.example .env
```

Abran `.env` y revisen los valores. Para Sprint 0 los defaults sirven, pero **cambien las contraseñas** antes de subir a un repo público:

- `MINIO_ROOT_USER` y `MINIO_ROOT_PASSWORD`
- `MONGO_INITDB_ROOT_USERNAME` y `MONGO_INITDB_ROOT_PASSWORD`

> **Nunca** suban `.env` a Git. El archivo `.gitignore` ya lo previene, pero verifiquen con `git status` antes de cada commit.

---

## Paso 3 — Levantar el stack

```bash
make up
```

Lo que va a pasar (puede tardar 3-8 minutos la primera vez por las descargas de imágenes):

1. Docker descarga las imágenes (`minio`, `tabulario/iceberg-rest`, `tabulario/spark-iceberg`, `mongo`).
2. Arranca MinIO. Espera a su healthcheck.
3. El contenedor `mc` corre **una sola vez** y crea el bucket `warehouse`. Termina con exit 0.
4. Arranca el REST Catalog apuntando al bucket de MinIO.
5. Arranca el contenedor de Spark (con Jupyter Lab adentro).
6. Arranca MongoDB.

Verifiquen con:

```bash
make ps
```

Deberían ver todos los servicios `running` o `healthy`. El servicio `mc` debe aparecer como `Exited (0)`, eso es **lo correcto** — su única tarea era crear el bucket.

### URLs útiles después de `make up`

| Servicio | URL | Credenciales |
|----------|-----|--------------|
| MinIO Console | http://localhost:9001 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` del `.env` |
| Iceberg REST | http://localhost:8181/v1/config | (no auth en local) |
| Spark UI | http://localhost:8080 | (no auth) |
| Jupyter Lab (Spark) | http://localhost:8888 | (no auth en local) |
| MongoDB | mongodb://localhost:27017 | del `.env` |

---

## Paso 4 — Correr smoke tests

Esta es la prueba de fuego del Sprint 0:

```bash
make smoke
```

Este comando ejecuta tres pruebas en orden:

1. **`test_minio.py`** — verifica que el bucket `warehouse` existe en MinIO usando `boto3`.
2. **`test_iceberg.py`** — abre una sesión Spark, crea la base `pulsomed.bronze` si no existe, crea una tabla de prueba `pulsomed.bronze._smoke_test`, inserta 3 filas, las lee, y dropea la tabla. Esto valida MinIO + REST Catalog + Spark + Iceberg de un golpe.
3. **`test_mongodb.py`** — conecta a Mongo, inserta un documento en `pulsomed._smoke`, lo lee, lo borra.

Si los tres pasan, **el Sprint 0 está terminado**. Hagan merge a `main` y abran el Sprint 1.

### Si algo falla

Ver el log del servicio sospechoso:

```bash
make logs SERVICE=iceberg-rest
make logs SERVICE=minio
make logs SERVICE=spark-iceberg
```

Errores comunes y cómo desbloquearlos están abajo en la sección **Troubleshooting**.

---

## Paso 5 — Apagar todo cuando terminen

```bash
make down       # apaga los contenedores, conserva los volúmenes
make clean      # apaga Y borra los volúmenes (cuidado, pierden los datos)
```

Para el Sprint 0 está bien usar `make clean` si quieren empezar de cero. Para el Sprint 1+, eviten `clean` o perderán los datos cargados.

---

## Tareas concretas para cerrar el sprint

Una sugerencia de distribución entre los tres miembros del equipo:

| # | Tarea | Sugerido para | Salida verificable |
|---|-------|---------------|--------------------|
| 1 | Validar `make up` en macOS / Linux / WSL2 | Cada uno en su máquina | Captura de `make ps` en cada SO |
| 2 | Pulir el smoke test de Iceberg (cubrir append + read + drop) | Una persona | `tests/smoke/test_iceberg.py` con docstrings |
| 3 | Documentar 3 errores comunes encontrados en el setup | Uno por persona | Sección "Troubleshooting" abajo crece |
| 4 | Crear plantilla de PR (`.github/pull_request_template.md`) | Una persona | Plantilla en repo |
| 5 | Configurar pre-commit hooks (`black`, `isort`, `markdownlint`) | Una persona | `.pre-commit-config.yaml` |
| 6 | Subir un short-screencast de 2 min "demo Sprint 0" | Equipo | Link en el ticket de cierre |

---

## Troubleshooting

### `make up` se queda colgado en "iceberg-rest is starting"

**Causa típica:** el bucket `warehouse` no se creó porque el servicio `mc` falló silenciosamente.

```bash
make logs SERVICE=mc
```

Si dice `mc: <ERROR>: ... Connection refused` es porque `mc` arrancó antes que MinIO terminara de levantar. Soluciones:

1. `make down && make up` — el segundo intento suele funcionar porque las imágenes ya están descargadas y MinIO arranca más rápido.
2. Si persiste, aumenten el `start_period` del healthcheck de MinIO en `docker-compose.yml`.

### Error `aws-sdk` o `S3FileIO` en logs del REST Catalog

**Causa:** la imagen de `iceberg-rest` no encuentra credenciales o el endpoint S3.

Verifiquen que las variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` y `CATALOG_S3_ENDPOINT` están bien definidas en `docker-compose.yml` y que coinciden con las del servicio MinIO.

### Spark levanta pero el smoke test dice `Catalog 'pulsomed' is not found`

**Causa:** Spark no tiene configurado el catálogo. Si están corriendo `pyspark` desde el host (no recomendado en Sprint 0), necesitan pasar las configs explícitamente. Pero en Sprint 0 los smoke tests se corren **dentro del contenedor** (`docker compose exec spark-iceberg ...`), donde la config ya viene en variables de entorno.

Revisen el comando exacto en `Makefile` — debería ser:

```makefile
docker compose exec -T spark-iceberg python /workspace/tests/smoke/test_iceberg.py
```

### En Windows: "permission denied" al montar `tests/`

**Causa:** están trabajando desde `/mnt/c/...`. Muevan el repo a `~/proyectos/...` dentro de WSL.

### Puerto 9000 ocupado (macOS)

**Causa:** el panel de control de macOS también usa el puerto 9000 ("AirPlay Receiver"). Apaguen "AirPlay Receiver" en System Settings → General → AirDrop & Handoff.

---

## ¿Qué se desbloquea con este sprint?

Cuando este sprint termina, podemos:

- ✅ Empezar el Sprint 1 (ingesta a Bronze) sin perder tiempo configurando.
- ✅ Que cualquier persona del jurado/profesor reproduzca el ambiente con `make up`.
- ✅ Empezar a tomarle el pulso al stack (qué tan rápido arranca, cuánta RAM consume) antes de que importe.

---

## Checklist final del Sprint 0

- [ ] `make up` arranca sin errores en las 3 máquinas del equipo.
- [ ] `make ps` muestra `minio`, `iceberg-rest`, `spark-iceberg`, `mongodb` corriendo.
- [ ] `make smoke` retorna `0`.
- [ ] El bucket `warehouse` existe en MinIO Console (puerto 9001).
- [ ] La tabla `pulsomed.bronze._smoke_test` se crea y se borra sin errores.
- [ ] El `.env.example` está versionado, el `.env` real **no**.
- [ ] `docs/sprints/sprint-0-setup.md` (este archivo) tiene al menos una sección de Troubleshooting nueva escrita por el equipo.
- [ ] Hay un PR cerrado a `main` con el setup verificado.

Cuando todo lo anterior esté en verde, **abran el ticket del Sprint 1**.
