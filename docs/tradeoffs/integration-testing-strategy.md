# Trade-off: Estrategia de Integration Tests para el path async

## Decisión adoptada

Los tests de integración cubren **cada frontera por separado**, no el flujo completo end-to-end:

- **HTTP → RabbitMQ:** el handler test verifica que el usecase llama al publisher con el evento correcto; el publisher test verifica que el mensaje llega al broker.
- **RabbitMQ → Redis:** el consumer test verifica que un mensaje recibido dispara el fanout correcto en Redis.
- **Redis → HTTP:** el handler integration test verifica `GET /timeline` contra Redis real.

El contrato de serialización entre publisher y consumer está garantizado por el tipo: los eventos (`TweetCreatedEvent`, `FollowCreatedEvent`, `FanoutRetryEvent`) son structs tipados definidos en `domain/events.go`. Ambos lados usan el mismo tipo — si el struct cambia, el compilador detecta la inconsistencia antes de correr ningún test.

---

## Alternativa considerada: test E2E async completo

Un único test que recorra `POST /tweet → RabbitMQ → consumer → Redis fanout → GET /timeline` con todos los servicios reales corriendo.

### Desventajas

- **Frágil por timing:** requiere esperar a que el consumer procese el mensaje antes de hacer el GET, lo que introduce sleeps o polling no determinista.
- **Acoplamiento excesivo:** un cambio en cualquier capa (nombre de queue, schema del evento, estrategia de fanout) rompe el test aunque el contrato externo del sistema sea idéntico.
- **Restringe evolución:** migrar de RabbitMQ a otro broker, o cambiar la estrategia de fanout, invalida el test completo en lugar de solo el test de la capa afectada.
- **Overkill para lo que agrega:** la cobertura incremental sobre los tests por capas es mínima dado que el contrato ya está garantizado por tipos.

### Ventajas

- Detectaría fallas de infraestructura (configuración errónea de queues, permisos) que los tests por capas no capturan.

---

## Cuándo reconsiderar

Considerar un test E2E si:
- El sistema incorpora múltiples servicios desplegados de forma independiente con contratos versionados (gRPC, AsyncAPI) donde la garantía de tipos no alcanza.
- Se adopta contract testing explícito (e.g., Pact) como reemplazo del compilador para equipos con distintos lenguajes en publisher y consumer.
