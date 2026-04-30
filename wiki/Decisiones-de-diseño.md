# Decisiones de diseño

## ADR-001: Use cases concretos, sin interfaz propia

**Estado:** Aceptado

### Contexto

Los use cases representan lógica de negocio que no tiene alternativa: no hay dos implementaciones de "crear usuario". Exponerlos como interfaces los trataría como infraestructura intercambiable, lo cual contradice su rol en DDD.

### Decisión

Los use cases son structs concretos. Ningún package define una interfaz que los represente en código de producción. Los handlers dependen directamente de ellos:

```go
handler.NewUserHandler(usecase.NewUserUseCase(userRepo))
```

Para testear handlers en aislamiento, cada handler define una **interfaz privada mínima** que vive únicamente en ese package y no es visible desde afuera:

```
internal/
  usecase/
    user.go     ← struct concreto, sin interfaz propia
  handler/
    user.go     ← userCreator (privada, solo para testing)
```

### Consecuencias

- Agregar un segundo adapter (gRPC, CLI) requiere depender del mismo struct concreto o duplicar la interfaz local. La duplicación es mínima y aceptable.
- Los tests del use case mockean repositorios (infraestructura), nunca el use case mismo.

---

## ADR-002: Redis como cache de timeline (fanout on write)

**Estado:** Aceptado

### Contexto

El sistema debe estar optimizado para lecturas. El timeline de un usuario puede requerir un JOIN entre follows, tweets y usuarios en PostgreSQL, con ordenamiento y paginación. A escala, esto es caro.

### Alternativas evaluadas

| Opción | Descripción | Trade-off |
|---|---|---|
| **Fanout on write** (elegida) | Al publicar, se escribe en el timeline Redis de cada seguidor | Escritura más costosa, lectura O(1) |
| Fanout on read | Al leer, se construye el timeline en el momento | Lectura costosa, escritura simple |
| Pull híbrido | Combina ambas según cantidad de seguidores | Mayor complejidad operativa |

### Decisión

Fanout on write via RabbitMQ. Al publicar un tweet, se emite un evento `tweet.created` que el consumer procesa de forma asíncrona para hacer el fanout. `GET /timeline` lee directamente de Redis.

### Consecuencias

- Latencia de lectura: O(1) desde Redis en casi todos los casos.
- Usuarios con millones de seguidores (celebrities) generarían fanout muy costoso. En producción se mitigaría con una estrategia híbrida para ese segmento; para esta kata no aplica.
- Al hacer follow, se hace backfill de los últimos N tweets del autor (configurable via `FOLLOW_BACKFILL_LIMIT`).

---

## ADR-003: Estrategia de testing

**Estado:** Aceptado

### Decisión

Dos niveles de test:

**Unit tests** (siempre corren):
- Mockean repositorios e interfaces privadas de los handlers.
- Cubren: validación de inputs, mapeo de errores de dominio a HTTP status codes, serialización.

**Integration tests** (`INTEGRATION=1`):
- Levantan PostgreSQL y Redis reales (no mocks).
- Testean el flujo completo de punta a punta incluyendo escrituras en base de datos.
- Son el test primario para el happy path de cada handler.

### Motivación

Los mocks de base de datos pasaron tests que fallaban en producción por divergencias en migraciones. Los tests de integración con infra real son la red de seguridad principal para el happy path.
