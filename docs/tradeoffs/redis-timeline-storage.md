# Trade-off: Redis Timeline Storage

## Decisión adoptada

El timeline de cada usuario se almacena en Redis como un **Sorted Set** donde:
- **score:** unix timestamp del tweet
- **member:** JSON completo del tweet (mismo contenido que en PostgreSQL)

```
timeline:{user_id} → ZREVRANGE → devuelve tweets completos en una sola operación
```

### Por qué

- `GET /timeline` requiere una única llamada a Redis, sin lookups secundarios
- Los tweets son inmutables en este sistema, por lo que la duplicación no genera inconsistencias
- Alineado con el requerimiento de optimización para lecturas

---

## Alternativa considerada: IDs + bulk get

Guardar solo el `tweet_id` como member y hacer un `MGET` para obtener el contenido de cada tweet desde un hash separado en Redis.

```
timeline:{user_id}   → Sorted Set de tweet_ids
tweet:{tweet_id}     → Hash con el contenido del tweet

GET /timeline:
  1. ZREVRANGE timeline:{user_id} → lista de IDs
  2. MGET tweet:{id1} tweet:{id2} ... → contenido de cada tweet
```

### Ventajas

- Menor uso de memoria: el contenido del tweet se almacena una sola vez, independientemente de cuántos seguidores tenga el autor
- Si en el futuro los tweets fueran editables, solo habría que actualizar `tweet:{tweet_id}` sin tocar ningún timeline

### Desventajas

- Dos operaciones por timeline request en lugar de una
- Mayor complejidad de implementación

---

## Cuándo migrar

Considerar esta migración cuando:
- La memoria de Redis sea un constraint medible (usuarios con decenas de miles de seguidores y tweets frecuentes)
- Se introduzca edición de tweets en el sistema

Hasta entonces, la decisión adoptada es la más simple y performante para el caso de uso actual.
