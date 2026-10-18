package provenance

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"
)

type Service struct {
	repo Repository
	now  func() time.Time
}

func NewService(repo Repository) (*Service, error) {
	if repo == nil {
		return nil, fmt.Errorf("repository: %w", ErrInvalid)
	}
	return &Service{repo: repo, now: func() time.Time { return time.Now().UTC() }}, nil
}

// RegisterDataset creates a new immutable version. The first version is 1.
func (s *Service) RegisterDataset(ctx context.Context, name, contentHash string, metadata map[string]string) (Dataset, error) {
	if err := ctx.Err(); err != nil {
		return Dataset{}, err
	}
	name = strings.TrimSpace(name)
	if err := validateName(name); err != nil {
		return Dataset{}, fmt.Errorf("dataset name: %w", err)
	}
	latest, err := s.repo.LatestDataset(ctx, name)
	version := 1
	if err == nil {
		version = latest.Ref.Version + 1
	} else if err != ErrNotFound {
		return Dataset{}, err
	}
	d := Dataset{Ref: DatasetRef{Name: name, Version: version}, ContentHash: contentHash, Metadata: cloneMap(metadata), CreatedAt: s.now()}
	if err := s.repo.CreateDataset(ctx, d); err != nil {
		return Dataset{}, err
	}
	ref := d.Ref
	if err := s.audit(ctx, AuditEvent{Type: "dataset.registered", At: d.CreatedAt, Dataset: &ref, Details: map[string]string{"content_hash": contentHash}}); err != nil {
		return Dataset{}, err
	}
	return d, nil
}

func (s *Service) GetDataset(ctx context.Context, ref DatasetRef) (Dataset, error) {
	if err := ref.Validate(); err != nil {
		return Dataset{}, err
	}
	return s.repo.GetDataset(ctx, ref)
}
func (s *Service) ListDatasets(ctx context.Context, name string) ([]Dataset, error) {
	return s.repo.ListDatasets(ctx, strings.TrimSpace(name))
}

func (s *Service) StartJob(ctx context.Context, name string, inputs []DatasetRef) (JobRun, error) {
	if err := ctx.Err(); err != nil {
		return JobRun{}, err
	}
	if err := validateName(name); err != nil {
		return JobRun{}, fmt.Errorf("job name: %w", err)
	}
	for _, ref := range inputs {
		if err := ref.Validate(); err != nil {
			return JobRun{}, err
		}
		if _, err := s.repo.GetDataset(ctx, ref); err != nil {
			return JobRun{}, fmt.Errorf("input %s: %w", ref.Key(), err)
		}
	}
	id, err := newID()
	if err != nil {
		return JobRun{}, err
	}
	j := JobRun{ID: id, Name: strings.TrimSpace(name), Status: JobRunning, StartedAt: s.now(), Inputs: append([]DatasetRef(nil), inputs...)}
	if err := s.repo.CreateJob(ctx, j); err != nil {
		return JobRun{}, err
	}
	if err := s.audit(ctx, AuditEvent{Type: "job.started", At: j.StartedAt, JobRunID: j.ID, Details: map[string]string{"name": j.Name}}); err != nil {
		return JobRun{}, err
	}
	return j, nil
}

// CompleteJob records output versions and lineage edges, then closes the run successfully.
func (s *Service) CompleteJob(ctx context.Context, id string, outputs []DatasetRef) (JobRun, error) {
	return s.finishJob(ctx, id, outputs, "")
}
func (s *Service) FailJob(ctx context.Context, id, reason string) (JobRun, error) {
	return s.finishJob(ctx, id, nil, reason)
}
func (s *Service) finishJob(ctx context.Context, id string, outputs []DatasetRef, failure string) (JobRun, error) {
	if err := ctx.Err(); err != nil {
		return JobRun{}, err
	}
	j, err := s.repo.GetJob(ctx, id)
	if err != nil {
		return JobRun{}, err
	}
	if j.Status != JobRunning {
		return JobRun{}, ErrInvalidState
	}
	if failure == "" {
		for _, ref := range outputs {
			if err := ref.Validate(); err != nil {
				return JobRun{}, err
			}
			if _, err := s.repo.GetDataset(ctx, ref); err != nil {
				return JobRun{}, fmt.Errorf("output %s: %w", ref.Key(), err)
			}
		}
		for _, in := range j.Inputs {
			for _, out := range outputs {
				if in == out {
					return JobRun{}, fmt.Errorf("self lineage %s: %w", in.Key(), ErrInvalid)
				}
			}
		}
		for _, in := range j.Inputs {
			for _, out := range outputs {
				if err := s.repo.CreateEdge(ctx, LineageEdge{From: in, To: out, JobRunID: j.ID, CreatedAt: s.now()}); err != nil {
					return JobRun{}, err
				}
			}
		}
		j.Status = JobSucceeded
		j.Outputs = append([]DatasetRef(nil), outputs...)
	} else {
		j.Status = JobFailed
		j.Error = failure
	}
	finished := s.now()
	j.FinishedAt = &finished
	if err := s.repo.UpdateJob(ctx, j); err != nil {
		return JobRun{}, err
	}
	typ := "job.succeeded"
	if j.Status == JobFailed {
		typ = "job.failed"
	}
	if err := s.audit(ctx, AuditEvent{Type: typ, At: finished, JobRunID: j.ID, Details: map[string]string{"status": string(j.Status), "error": j.Error}}); err != nil {
		return JobRun{}, err
	}
	return j, nil
}
func (s *Service) GetJob(ctx context.Context, id string) (JobRun, error) {
	return s.repo.GetJob(ctx, id)
}

// Lineage returns all reachable dataset versions up to maxDepth, excluding the start node.
func (s *Service) Lineage(ctx context.Context, start DatasetRef, direction string, maxDepth int) ([]DatasetRef, error) {
	if err := start.Validate(); err != nil {
		return nil, err
	}
	if maxDepth < 1 {
		return nil, fmt.Errorf("maxDepth must be positive: %w", ErrInvalid)
	}
	if direction != "upstream" && direction != "downstream" {
		return nil, fmt.Errorf("direction must be upstream or downstream: %w", ErrInvalid)
	}
	if _, err := s.repo.GetDataset(ctx, start); err != nil {
		return nil, err
	}
	edges, err := s.repo.Edges(ctx)
	if err != nil {
		return nil, err
	}
	adj := map[DatasetRef][]DatasetRef{}
	for _, e := range edges {
		if direction == "downstream" {
			adj[e.From] = append(adj[e.From], e.To)
		} else {
			adj[e.To] = append(adj[e.To], e.From)
		}
	}
	type item struct {
		ref   DatasetRef
		depth int
	}
	queue := []item{{start, 0}}
	seen := map[DatasetRef]bool{start: true}
	result := make([]DatasetRef, 0)
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cur := queue[0]
		queue = queue[1:]
		if cur.depth >= maxDepth {
			continue
		}
		for _, next := range adj[cur.ref] {
			if seen[next] {
				continue
			}
			seen[next] = true
			result = append(result, next)
			queue = append(queue, item{next, cur.depth + 1})
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Name == result[j].Name {
			return result[i].Version < result[j].Version
		}
		return result[i].Name < result[j].Name
	})
	return result, nil
}

func (s *Service) AuditTrail(ctx context.Context, ref DatasetRef, from, to time.Time) ([]AuditEvent, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	if !from.IsZero() && !to.IsZero() && to.Before(from) {
		return nil, fmt.Errorf("time range is reversed: %w", ErrInvalid)
	}
	events, err := s.repo.AuditEvents(ctx, from, to)
	if err != nil {
		return nil, err
	}
	out := make([]AuditEvent, 0)
	for _, e := range events {
		if e.Dataset != nil && *e.Dataset == ref {
			out = append(out, e)
			continue
		}
		if e.JobRunID != "" {
			j, err := s.repo.GetJob(ctx, e.JobRunID)
			if err == nil {
				for _, r := range j.Inputs {
					if r == ref {
						out = append(out, e)
						break
					}
				}
				for _, r := range j.Outputs {
					if r == ref {
						out = append(out, e)
						break
					}
				}
			}
		}
	}
	return out, nil
}
func (s *Service) AllAuditEvents(ctx context.Context, from, to time.Time) ([]AuditEvent, error) {
	if !from.IsZero() && !to.IsZero() && to.Before(from) {
		return nil, ErrInvalid
	}
	return s.repo.AuditEvents(ctx, from, to)
}
func (s *Service) audit(ctx context.Context, e AuditEvent) error {
	id, err := newID()
	if err != nil {
		return err
	}
	e.ID = id
	return s.repo.AppendAudit(ctx, e)
}
func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
