package provenance

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Repository is intentionally small. Implementations must be safe for concurrent use.
type Repository interface {
	CreateDataset(context.Context, Dataset) error
	GetDataset(context.Context, DatasetRef) (Dataset, error)
	LatestDataset(context.Context, string) (Dataset, error)
	ListDatasets(context.Context, string) ([]Dataset, error)
	CreateJob(context.Context, JobRun) error
	GetJob(context.Context, string) (JobRun, error)
	UpdateJob(context.Context, JobRun) error
	CreateEdge(context.Context, LineageEdge) error
	Edges(context.Context) ([]LineageEdge, error)
	AppendAudit(context.Context, AuditEvent) error
	AuditEvents(context.Context, time.Time, time.Time) ([]AuditEvent, error)
}

type memoryRepository struct {
	mu       sync.RWMutex
	datasets map[string]Dataset
	latest   map[string]int
	jobs     map[string]JobRun
	edges    []LineageEdge
	audit    []AuditEvent
}

func NewMemoryRepository() Repository {
	return &memoryRepository{datasets: map[string]Dataset{}, latest: map[string]int{}, jobs: map[string]JobRun{}}
}

func (m *memoryRepository) CreateDataset(ctx context.Context, d Dataset) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := d.Ref.Key()
	if _, ok := m.datasets[key]; ok {
		return ErrAlreadyExists
	}
	m.datasets[key] = cloneDataset(d)
	if d.Ref.Version > m.latest[d.Ref.Name] {
		m.latest[d.Ref.Name] = d.Ref.Version
	}
	return nil
}
func (m *memoryRepository) GetDataset(ctx context.Context, ref DatasetRef) (Dataset, error) {
	if err := ctx.Err(); err != nil {
		return Dataset{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	d, ok := m.datasets[ref.Key()]
	if !ok {
		return Dataset{}, ErrNotFound
	}
	return cloneDataset(d), nil
}
func (m *memoryRepository) LatestDataset(ctx context.Context, name string) (Dataset, error) {
	if err := ctx.Err(); err != nil {
		return Dataset{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.latest[name]
	if !ok {
		return Dataset{}, ErrNotFound
	}
	d, ok := m.datasets[DatasetRef{Name: name, Version: v}.Key()]
	if !ok {
		return Dataset{}, ErrNotFound
	}
	return cloneDataset(d), nil
}
func (m *memoryRepository) ListDatasets(ctx context.Context, name string) ([]Dataset, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Dataset, 0)
	for _, d := range m.datasets {
		if name == "" || d.Ref.Name == name {
			out = append(out, cloneDataset(d))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Ref.Name == out[j].Ref.Name {
			return out[i].Ref.Version < out[j].Ref.Version
		}
		return out[i].Ref.Name < out[j].Ref.Name
	})
	return out, nil
}
func (m *memoryRepository) CreateJob(ctx context.Context, j JobRun) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.jobs[j.ID]; ok {
		return ErrAlreadyExists
	}
	m.jobs[j.ID] = cloneJob(j)
	return nil
}
func (m *memoryRepository) GetJob(ctx context.Context, id string) (JobRun, error) {
	if err := ctx.Err(); err != nil {
		return JobRun{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	j, ok := m.jobs[id]
	if !ok {
		return JobRun{}, ErrNotFound
	}
	return cloneJob(j), nil
}
func (m *memoryRepository) UpdateJob(ctx context.Context, j JobRun) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.jobs[j.ID]; !ok {
		return ErrNotFound
	}
	m.jobs[j.ID] = cloneJob(j)
	return nil
}
func (m *memoryRepository) CreateEdge(ctx context.Context, e LineageEdge) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, old := range m.edges {
		if old.From == e.From && old.To == e.To && old.JobRunID == e.JobRunID {
			return nil
		}
	}
	m.edges = append(m.edges, e)
	return nil
}
func (m *memoryRepository) Edges(ctx context.Context) ([]LineageEdge, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]LineageEdge(nil), m.edges...), nil
}
func (m *memoryRepository) AppendAudit(ctx context.Context, e AuditEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.audit = append(m.audit, cloneAudit(e))
	return nil
}
func (m *memoryRepository) AuditEvents(ctx context.Context, from, to time.Time) ([]AuditEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]AuditEvent, 0)
	for _, e := range m.audit {
		if (from.IsZero() || !e.At.Before(from)) && (to.IsZero() || !e.At.After(to)) {
			out = append(out, cloneAudit(e))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].At.Before(out[j].At) })
	return out, nil
}

func cloneDataset(d Dataset) Dataset { d.Metadata = cloneMap(d.Metadata); return d }
func cloneJob(j JobRun) JobRun {
	j.Inputs = append([]DatasetRef(nil), j.Inputs...)
	j.Outputs = append([]DatasetRef(nil), j.Outputs...)
	if j.FinishedAt != nil {
		t := *j.FinishedAt
		j.FinishedAt = &t
	}
	return j
}
func cloneAudit(e AuditEvent) AuditEvent {
	if e.Dataset != nil {
		d := *e.Dataset
		e.Dataset = &d
	}
	e.Details = cloneMap(e.Details)
	return e
}
func cloneMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
