package provenance

import (
	"context"
	"errors"
	"testing"
	"time"
)

func newTestService(t *testing.T) *Service {
	t.Helper()
	s, err := NewService(NewMemoryRepository())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestRegisterCreatesImmutableVersions(t *testing.T) {
	s := newTestService(t)
	ctx := context.Background()
	a, err := s.RegisterDataset(ctx, "orders", "hash-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.RegisterDataset(ctx, "orders", "hash-2", map[string]string{"owner": "data"})
	if err != nil {
		t.Fatal(err)
	}
	if a.Ref.Version != 1 || b.Ref.Version != 2 {
		t.Fatalf("versions: %d, %d", a.Ref.Version, b.Ref.Version)
	}
	got, err := s.GetDataset(ctx, a.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if got.ContentHash != "hash-1" {
		t.Fatalf("old version mutated: %q", got.ContentHash)
	}
}

func TestLineageAndAuditTrail(t *testing.T) {
	s := newTestService(t)
	ctx := context.Background()
	a, _ := s.RegisterDataset(ctx, "raw", "a", nil)
	b, _ := s.RegisterDataset(ctx, "clean", "b", nil)
	c, _ := s.RegisterDataset(ctx, "mart", "c", nil)
	j1, err := s.StartJob(ctx, "clean", []DatasetRef{a.Ref})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.CompleteJob(ctx, j1.ID, []DatasetRef{b.Ref}); err != nil {
		t.Fatal(err)
	}
	j2, err := s.StartJob(ctx, "mart", []DatasetRef{b.Ref})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.CompleteJob(ctx, j2.ID, []DatasetRef{c.Ref}); err != nil {
		t.Fatal(err)
	}
	up, err := s.Lineage(ctx, c.Ref, "upstream", 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(up) != 2 {
		t.Fatalf("want 2 upstream nodes, got %d", len(up))
	}
	down, err := s.Lineage(ctx, a.Ref, "downstream", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(down) != 1 || down[0] != b.Ref {
		t.Fatalf("unexpected depth-1 downstream: %#v", down)
	}
	trail, err := s.AuditTrail(ctx, b.Ref, time.Time{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(trail) == 0 {
		t.Fatal("expected audit events")
	}
}

func TestFailedJobCannotFinishTwice(t *testing.T) {
	s := newTestService(t)
	ctx := context.Background()
	j, err := s.StartJob(ctx, "broken", nil)
	if err != nil {
		t.Fatal(err)
	}
	finished, err := s.FailJob(ctx, j.ID, "upstream unavailable")
	if err != nil {
		t.Fatal(err)
	}
	if finished.Status != JobFailed {
		t.Fatalf("status=%s", finished.Status)
	}
	_, err = s.CompleteJob(ctx, j.ID, nil)
	if !errors.Is(err, ErrInvalidState) {
		t.Fatalf("want ErrInvalidState, got %v", err)
	}
}

func TestCanceledContext(t *testing.T) {
	s := newTestService(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.RegisterDataset(ctx, "x", "hash", nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}
