# Iter 0 — Infrastructure Base Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Dejar el entorno local listo para desarrollar con PostgreSQL en Docker, estructura de carpetas Clean Architecture, y Makefile con comandos básicos.

**Architecture:** No hay código de aplicación. La estructura de carpetas refleja Clean Architecture: `cmd/api` (entry point), `internal/domain`, `internal/usecase`, `internal/handler`, `internal/repository`, `internal/infra`, `migrations`. Docker Compose levanta PostgreSQL localmente.

**Tech Stack:** Go 1.26, Docker Compose, PostgreSQL 16, Make

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `docker-compose.yml` | Create | PostgreSQL service local |
| `Makefile` | Create | Comandos: up, down, run, test |
| `cmd/api/main.go` | Create | Entry point mínimo compilable |
| `main.go` | Delete | Reemplazado por cmd/api/main.go |
| `internal/domain/.gitkeep` | Create | Placeholder capa domain |
| `internal/usecase/.gitkeep` | Create | Placeholder capa usecase |
| `internal/handler/.gitkeep` | Create | Placeholder capa handler |
| `internal/repository/.gitkeep` | Create | Placeholder capa repository |
| `internal/infra/.gitkeep` | Create | Placeholder capa infra |
| `migrations/.gitkeep` | Create | Placeholder migraciones SQL |

---

### Task 1: docker-compose.yml con PostgreSQL

**Files:**
- Create: `docker-compose.yml`

- [ ] **Step 1: Crear docker-compose.yml**

```yaml
services:
  postgres:
    image: postgres:16-alpine
    container_name: uala_postgres
    environment:
      POSTGRES_USER: uala
      POSTGRES_PASSWORD: uala
      POSTGRES_DB: uala
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U uala"]
      interval: 5s
      timeout: 5s
      retries: 5

volumes:
  postgres_data:
```

- [ ] **Step 2: Verificar que levanta**

```bash
docker compose up -d
docker compose ps
```

Expected: `uala_postgres` en estado `healthy` (puede tardar ~10s).

- [ ] **Step 3: Bajar los contenedores**

```bash
docker compose down
```

---

### Task 2: Makefile con comandos básicos

**Files:**
- Create: `Makefile`

- [ ] **Step 1: Crear Makefile**

```makefile
.PHONY: up down run test

up:
	docker compose up -d

down:
	docker compose down

run:
	go run ./cmd/api/...

test:
	go test ./...
```

- [ ] **Step 2: Verificar que `make up` y `make down` funcionan**

```bash
make up
docker compose ps
make down
```

Expected: contenedor postgres arranca y para sin errores.

---

### Task 3: Estructura de carpetas Clean Architecture

**Files:**
- Create: `cmd/api/main.go`
- Create: `internal/domain/.gitkeep`
- Create: `internal/usecase/.gitkeep`
- Create: `internal/handler/.gitkeep`
- Create: `internal/repository/.gitkeep`
- Create: `internal/infra/.gitkeep`
- Create: `migrations/.gitkeep`
- Delete: `main.go` (root)

- [ ] **Step 1: Crear cmd/api/main.go mínimo**

```go
package main

import "fmt"

func main() {
	fmt.Println("uala api starting...")
}
```

- [ ] **Step 2: Crear placeholders para las capas**

```bash
mkdir -p internal/domain internal/usecase internal/handler internal/repository internal/infra migrations
touch internal/domain/.gitkeep internal/usecase/.gitkeep internal/handler/.gitkeep
touch internal/repository/.gitkeep internal/infra/.gitkeep migrations/.gitkeep
```

- [ ] **Step 3: Eliminar main.go del root**

```bash
rm main.go
```

- [ ] **Step 4: Verificar que compila**

```bash
go build ./cmd/api/...
```

Expected: sin errores, genera binario.

- [ ] **Step 5: Verificar `make run`**

```bash
make run
```

Expected: imprime `uala api starting...` y termina.

- [ ] **Step 6: Verificar `make test`**

```bash
make test
```

Expected: `ok` o `no test files` — sin errores de compilación.

- [ ] **Step 7: Commit**

```bash
git init
git add docker-compose.yml Makefile cmd/ internal/ migrations/ go.mod
git commit -m "feat: iter-0 infrastructure base — clean arch structure + docker compose"
```

---

## Self-Review

**Spec coverage:**
- [x] `docker-compose.yml` con PostgreSQL → Task 1
- [x] Estructura de carpetas clean arch → Task 3
- [x] Makefile con `up`, `down`, `run`, `test` → Task 2

**Placeholder scan:** Ningún paso tiene TBD, TODO, o código incompleto.

**Type consistency:** No hay tipos de dominio en este iter.
