# Trade-off: Redis Timeline Storage

## Decisión actual (post celebrity fanout)

El timeline de cada usuario se almacena en Redis usando **dos estructuras separadas**:

```
timeline:{user_id}       →  Sorted Set  (score: unix timestamp, member: tweet_id)
timeline:data:{user_id}  →  Hash        (field: tweet_id, value: JSON del tweet)
```

La lectura combina `ZREVRANGE` para obtener los IDs ordenados y `HMGET` para recuperar el contenido.

### Por qué se migró a esta estructura

Durante iter-2 se almacenaba el JSON completo del tweet como member del Sorted Set. Al introducir el fanout filtrado por actividad (celebrity fanout), cada tweet se escribe múltiples veces — una por follower activo. Con JSON completo como member, cada escritura duplica el payload completo. Con IDs como member y un Hash compartido, el contenido del tweet se almacena una sola vez por key de usuario (`HSetNX` es idempotente).

### Comportamiento de TTL

- `AppendTweet` usa `ExpireNX` para setear el TTL de la ventana de actividad solo en keys nuevas (no sobreescribe TTL existente).
- `GetTimeline` usa `Expire` para renovar el TTL al valor completo de `ACTIVITY_TTL` en cada lectura.
- Las keys expiran naturalmente cuando el usuario pasa a inactivo, liberando memoria sin eviction explícita.

---

## Estrategia original (iter-2)

En iter-2 se guardaba el JSON completo del tweet como member del Sorted Set:

```
timeline:{user_id} → ZREVRANGE → devuelve tweets completos en una sola operación
```

### Ventajas que tenía

- `GET /timeline` requería una única llamada a Redis.
- Implementación más simple.

### Por qué se abandonó

- Con fanout por actividad, cada tweet se escribe N veces (una por follower activo). El JSON completo como member implica duplicar el payload en cada escritura.
- La estructura ID + Hash permite `HSetNX` idempotente: si el tweet ya existe en el hash de un usuario, no se reescribe.

---

## Cuándo migrar nuevamente

Si el sistema introduce edición de tweets y la consistencia cache-source se vuelve crítica, considerar invalidación del hash en lugar de TTL pasivo.
