# Trade-off: Estrategia de fanout para usuarios con muchos seguidores

## Problema

El fanout actual itera sobre todos los followers de un usuario para escribir su tweet en cada timeline. Para un usuario con 10M seguidores esto implica 20M operaciones Redis por tweet, bloqueando el consumer durante minutos.

---

## Decisión adoptada: Fanout filtrado por actividad + TTL universal

El fanout solo escribe a followers que tuvieron actividad en las últimas `ACTIVITY_TTL` horas. La actividad se registra cuando el usuario llama a `GET /timeline`, publicando un `UserActivityEvent` con el timestamp del momento del read. Un consumer actualiza `users.last_active` en Postgres con ese valor.

El fanout usa `GetActiveFollowers` — un JOIN en Postgres que retorna solo followers activos junto con su `last_active`. No hay concepto de "celebrity threshold": el mismo mecanismo aplica a todos los usuarios, independientemente de la cantidad de seguidores.

El TTL de las keys Redis del timeline se gestiona en función de la actividad:
- **En write (fanout):** `EXPIRE NX` con el tiempo restante calculado como `ACTIVITY_TTL - time.Since(follower.LastActive)`. El `NX` garantiza que solo se setea TTL si la key es nueva — `ZADD`/`HSET` no modifican el TTL de keys existentes en Redis.
- **En read:** `EXPIRE` con el `ACTIVITY_TTL` completo, renovando la ventana desde el momento del read.

Invariante: si `timeline:{userID}` existe en Redis, el usuario estaba activo al momento del último fanout o read recibido.

### Por qué

- Reduce el fanout de 10M a solo los followers que realmente van a leer el tweet.
- Sin cambios en el read path — el fallback a Postgres para cache miss ya existe.
- Sin concepto de threshold: uniformidad en el comportamiento para todos los usuarios.
- El `last_active` viaja en el payload del evento (no se recalcula en el consumer) para que el timestamp refleje el momento real del read, no el momento de procesamiento del mensaje.

---

## Alternativas consideradas

### A — Hybrid push/pull (modelo Twitter clásico)

Fanout completo para usuarios normales; para celebrities (followers > threshold), fanout solo a activos. En el read, se mergea el push timeline con tweets recientes de celebrities seguidas.

**Descartada porque:** requiere cambios en el read path (merge + queries extra por celebrity), introduce el concepto de threshold como magic number, y agrega complejidad que no aporta ventaja sobre la opción D dado que el fallback a Postgres ya maneja el cold start.

### B — Celebrity-only pull

Celebrities nunca tienen fanout. El read path siempre consulta sus tweets directamente.

**Descartada porque:** mueve el cuello de botella al read path — cada `GET /timeline` dispara N queries extra si el usuario sigue N celebrities. No escala en lecturas.

### C — Sharded async fanout

Se mantiene el modelo push completo pero el fanout se distribuye en batches via una queue secundaria para no bloquear el consumer principal.

**Descartada porque:** no reduce el volumen total de writes a Redis (sigue siendo 10M operaciones para 10M followers). Difiere el problema sin resolverlo.

---

## Dónde guardar last_active: Redis vs Postgres

Se evaluaron dos opciones para almacenar la señal de actividad:

**Redis (`active:{userID}` con TTL):** escrita en O(1), pero para filtrar followers activos en el fanout requeriría cargar todos los follower IDs desde Postgres y luego chequear Redis por cada uno — O(N) donde N es el total de followers, que es exactamente el problema que se quiere resolver.

**Postgres (`last_active` column):** permite filtrar en el JOIN directamente. Con un índice en `last_active`, la query retorna solo los followers activos sin pasar por la memoria del consumer. Es más pesado para escrituras pero resuelve el filtrado en la capa correcta.

**Decisión: Postgres.** El fanout query es el camino crítico. La columna `last_active` ya está disponible en el resultado de `GetActiveFollowers`, por lo que el remaining TTL para Redis se calcula a partir de ese dato sin necesidad de un Redis key separado para actividad.

---

## Cuándo reconsiderar

- Si la tasa de timeline reads genera contención en `users.last_active` por exceso de UPDATEs: introducir deduplicación en el publisher (no publicar si el evento fue publicado en los últimos N segundos para ese usuario).
- Si el `ACTIVITY_TTL` de 24h resulta demasiado agresivo (muchos cache misses en usuarios que leen cada 25-48h): aumentar el TTL o hacerlo configurable por segmento de usuario.
