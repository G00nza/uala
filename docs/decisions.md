# Architecture Decision Records

## ADR-001: Use cases son concretos — no se abstraen con interfaces

**Date:** 2026-04-29  
**Status:** Accepted

### Context

Los use cases son el núcleo del proyecto. A diferencia de los repositorios o clientes externos, un use case representa lógica de negocio que no tiene alternativa: no hay dos implementaciones de "crear usuario" ni existe una versión de test del use case que sea diferente a la real. Exponerlo como interfaz implicaría tratarlo como infraestructura intercambiable, lo cual contradice su rol en DDD.

Se evaluó mover las interfaces al package `usecase` como input ports (patrón hexagonal). Eso resuelve el problema de "quién dueño del contrato", pero introduce otro: el use case empieza a diseñarse alrededor de una abstracción que nadie más consume, y la capa de aplicación queda acoplada a su propio esquema de testing.

### Decision

Los use cases son structs concretos. Ningún package define una interfaz que los represente en código de producción.

Los handlers dependen directamente de los use cases concretos:

```
handler.NewUserHandler(usecase.NewUserUseCase(userRepo))
```

Para testear el handler HTTP en aislamiento (validación de inputs, mapeo de errores a status codes, serialización), cada handler define una interfaz privada mínima que sirve como seam de testing. Esta interfaz vive únicamente en el package handler y no es visible desde afuera.

```
internal/
  usecase/
    user.go   ← struct concreto, sin interfaz propia
  handler/
    user.go   ← userCreator (privada, solo para testing)
```

### Estrategia de testing

- **Integration tests** (`INTEGRATION=1`): levantan Postgres + Redis reales, testean el flujo completo de punta a punta incluyendo escrituras en base de datos. Son el test primario para el happy path de cada handler.
- **Unit tests con mock**: cubren validación de inputs, mapeo de errores de dominio a HTTP status codes y lógica de serialización. Usan la interfaz privada del handler para inyectar un stub.

### Consequences

- Agregar un segundo adapter (gRPC, CLI) requiere depender del mismo struct concreto o duplicar la interfaz local. Esto es aceptable: la duplicación es mínima y el use case mantiene un contrato único.
- Los tests del use case mockean repositorios (infraestructura real), nunca el use case mismo.
