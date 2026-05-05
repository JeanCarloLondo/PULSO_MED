# Architecture Decision Records (ADRs)

Esta carpeta guarda las decisiones técnicas grandes del proyecto, cada una en
un archivo aparte. Un ADR no es un manual: es la **historia razonada** de por
qué tomamos una decisión, qué alternativas consideramos, y qué pasaría si
mañana cambia.

## Plantilla

Cada ADR sigue este esquema (roba de Michael Nygard):

```markdown
# ADR N · <Título corto>

**Estado:** Propuesto | Aceptado | Rechazado | Reemplazado por ADR M
**Fecha:** YYYY-MM-DD
**Decisores:** nombres del equipo

## Contexto

Qué fuerzas técnicas y de negocio nos están empujando a decidir esto.

## Decisión

Qué decidimos. Una frase, máximo dos.

## Alternativas evaluadas

- Opción A — pros, contras
- Opción B — pros, contras
- Opción C — pros, contras

## Consecuencias

- Lo que se vuelve más fácil con esta decisión.
- Lo que se vuelve más difícil.
- Qué señales nos indicarían que toca revisar esta decisión.
```

## ADRs planeados

| # | Título | Sprint | Estado |
|---|--------|--------|--------|
| 02 | Lambda vs Kappa | Sprint 4 | ⏳ por escribir |
| 04 | Benchmark de formatos (CSV/Parquet/ZSTD) | Sprint 1 | ⏳ |
| 05 | Delta Lake vs Apache Iceberg | Sprint 4 | ⏳ |
| 07 | AWS vs GCP como proveedor cloud | Sprint 5 | ⏳ |

> La numeración empieza en 02 porque coincide con los Módulos 02, 04, 05 y 07
> definidos en la propuesta original (sección 8 del PDF).
