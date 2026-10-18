# Custom Data Lineage & Provenance Tracker (Go)

A modular, standard-library-only starter implementation for tracking dataset versions, lineage edges, job runs, and audit events. It uses an in-memory repository by default, so it runs without a database. The repository interface is the persistence seam for a future SQLite/PostgreSQL adapter.

## Requirements

- Go 1.22+

## Run

```sh
go test ./...
go run ./cmd/demo
```

## Structure

```text
cmd/demo/main.go                 Runnable example
internal/provenance/models.go    Domain types and validation
internal/provenance/repository.go Repository interface and in-memory adapter
internal/provenance/service.go   Dataset/job/lineage/audit use cases
internal/provenance/service_test.go
```

## Behavior and design choices

- **Immutable dataset versions:** each registration creates a new version for a dataset name. A version is identified by `(dataset name, version)` and carries a content hash and metadata.
- **Explicit errors:** public operations return errors; invalid references and invalid state transitions are rejected.
- **Bounded lineage traversal:** upstream/downstream queries require a positive maximum depth and protect against cycles.
- **Job lifecycle:** runs transition from `running` to `succeeded` or `failed`; a run can only finish once. A context is checked before work is recorded. The service does not spawn goroutines.
- **Decoupled processing:** this package records metadata about jobs; it does not execute data processing itself.
- **Audit history:** dataset-related events can be filtered by UTC time range.
- **Concurrency:** the in-memory repository protects state with a mutex. Its methods are synchronous and safe for concurrent callers.

## Simplifications / production follow-ups

The in-memory store is volatile and intended for a prototype or tests. For production, implement `Repository` using a durable database and add schema migrations, authentication/authorization, retention policy, metrics, tracing, and transaction semantics appropriate to the chosen database. The content hash is supplied by the caller; this package does not read or hash dataset files.
