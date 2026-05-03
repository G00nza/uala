---
title: Load Test Script Design
date: 2026-05-02
status: approved
---

# Load Test Script

Script Python (stdlib pura, sin dependencias externas) para medir la performance real del proyecto desde una máquina remota. Ejecuta un escenario de carga mixta realista con ramp-up agresivo.

---

## Objetivo

Medir throughput, latencia (p50/p95/p99) y tasa de error de la API bajo carga mixta, simulando patrones de actividad reales: usuarios siempre activos, usuarios que alternan entre activo e inactivo, y usuarios one-shot. El entorno objetivo tiene TTL de Redis en 30s, lo que hace que los cache misses sean observables durante la prueba.

---

## Endpoints bajo prueba

| Endpoint | Peso en carga mixta |
|---|---|
| `GET /timeline` | 80% |
| `POST /tweets` | 15% |
| `POST /follow` | 5% |

El header `X-User-ID` identifica al usuario en todos los requests.

---

## Fases

### Fase 1 — Seed

Corre antes de la carga. Crea datos iniciales usando la propia API con un `ThreadPoolExecutor` de tamaño configurable (`--seed-workers`, default 50).

**Orden de operaciones:**
1. Crear 2 usuarios celebrity (IDs guardados en memoria para uso en toda la prueba).
2. Crear `--users` (default 1000) usuarios normales en paralelo.
3. Cada usuario normal sigue a los 2 celebrities + un subconjunto aleatorio de `--follows-per-user` (default 200) usuarios normales.
4. Cada usuario normal publica `--tweets-per-user` (default 100) tweets iniciales.

Al finalizar, el seed guarda los IDs de celebrities y usuarios en `seed_state.json` en el directorio de trabajo. El seed puede correrse de forma aislada con `--seed-only`, o saltarse con `--skip-seed` (en cuyo caso el script carga los IDs desde `seed_state.json`).

### Fase 2 — Load (ramp-up + sostenida)

Cada worker simula un usuario virtual (VU). El ramp-up agrega `--ramp-step` (default 50) workers cada `--ramp-interval` (default 10) segundos hasta alcanzar `--max-vus` (default 500). La fase sostenida continúa hasta cumplir `--duration` (default 120) segundos desde el inicio de la carga.

**Perfiles de VU (asignados aleatoriamente al crear el worker):**

| Perfil | Proporción | Comportamiento |
|---|---|---|
| Always-on | 50% | Loop continuo, nunca duerme. Sostiene la carga base. |
| Cycler | 25% | Alterna 15s activo / 45s dormido indefinidamente. Con TTL=30s, cada reactivación produce un cache miss en el primer `GET /timeline`. |
| One-shot | 25% | Activo durante un período aleatorio entre 10s y 60s, luego termina definitivamente. No se reemplaza. |

**Inicialización de cada nuevo VU durante la fase de carga:**
Antes de entrar al loop, cada worker sigue a los 2 celebrities (igual que en el seed), para mantener el estado consistente con los usuarios creados en la fase 1.

**Corte automático:** si el error rate supera el 20% durante 10 segundos consecutivos, el ramp-up se detiene y el nivel actual de VUs se mantiene hasta el fin de `--duration`.

---

## Métricas

Un thread colector centralizado drena una `queue.Queue` thread-safe cada segundo. Cada request escribe una entrada con: timestamp, endpoint, latencia (ms), HTTP status, perfil de VU.

**Output en tiempo real (cada 5 segundos):**
```
[t=30s] VUs: 150 (active: 112) | RPS: 842 | p50: 12ms | p95: 87ms | p99: 210ms | errors: 0.3%
  timeline: p95=45ms  tweets: p95=120ms  follow: p95=95ms
```

**Archivos generados al terminar:**
- `results_<timestamp>.csv` — todos los requests crudos (timestamp, endpoint, latency_ms, status, vu_profile)
- `summary_<timestamp>.txt` — resumen por endpoint: p50/p95/p99, throughput, error rate, total requests

El CSV es importable en Grafana para cruzar con las métricas de Prometheus del servidor.

---

## Estructura del archivo

Un único archivo `load_test.py` en la raíz del proyecto. Sin dependencias externas. Requiere Python 3.8+.

**Módulos stdlib utilizados:** `threading`, `http.client`, `queue`, `csv`, `argparse`, `random`, `time`, `statistics`, `concurrent.futures`.

---

## Uso

```bash
# Seed + carga completa (caso típico)
python3 load_test.py --target http://192.168.1.10:8080 \
  --users 1000 --tweets-per-user 100 --follows-per-user 200 \
  --max-vus 500 --ramp-step 50 --ramp-interval 10 --duration 600

# Solo seed
python3 load_test.py --target http://192.168.1.10:8080 --seed-only

# Solo carga (asume seed ya corrido, carga IDs desde seed_state.json)
python3 load_test.py --target http://192.168.1.10:8080 \
  --skip-seed --max-vus 500 --ramp-step 50 --ramp-interval 10 --duration 600
```

## Flags

| Flag | Default | Descripción |
|---|---|---|
| `--target` | requerido | URL base del servidor |
| `--users` | 1000 | Usuarios normales a crear en seed |
| `--tweets-per-user` | 100 | Tweets iniciales por usuario |
| `--follows-per-user` | 200 | Follows por usuario en seed |
| `--max-vus` | 500 | Máximo de workers concurrentes |
| `--ramp-step` | 50 | VUs a agregar por oleada |
| `--ramp-interval` | 10 | Segundos entre oleadas |
| `--duration` | 600 | Duración total de la fase de carga (segundos) |
| `--seed-only` | false | Solo correr el seed, no la carga |
| `--skip-seed` | false | Saltear el seed |
| `--seed-workers` | 50 | Concurrencia durante el seed |
