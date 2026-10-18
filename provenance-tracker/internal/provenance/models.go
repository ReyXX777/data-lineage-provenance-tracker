package provenance

import (
	"errors"
	"strings"
	"time"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrInvalid       = errors.New("invalid input")
	ErrInvalidState  = errors.New("invalid state transition")
	ErrAlreadyExists = errors.New("already exists")
)

type DatasetRef struct {
	Name    string `json:"name"`
	Version int    `json:"version"`
}

func (r DatasetRef) Validate() error {
	if strings.TrimSpace(r.Name) == "" || r.Version < 1 {
		return ErrInvalid
	}
	return nil
}

func (r DatasetRef) Key() string { return r.Name + "@" + itoa(r.Version) }

type Dataset struct {
	Ref         DatasetRef        `json:"ref"`
	ContentHash string            `json:"content_hash"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
}

type JobStatus string

const (
	JobRunning   JobStatus = "running"
	JobSucceeded JobStatus = "succeeded"
	JobFailed    JobStatus = "failed"
)

type JobRun struct {
	ID         string       `json:"id"`
	Name       string       `json:"name"`
	Status     JobStatus    `json:"status"`
	StartedAt  time.Time    `json:"started_at"`
	FinishedAt *time.Time   `json:"finished_at,omitempty"`
	Error      string       `json:"error,omitempty"`
	Inputs     []DatasetRef `json:"inputs,omitempty"`
	Outputs    []DatasetRef `json:"outputs,omitempty"`
}

type LineageEdge struct {
	From      DatasetRef `json:"from"`
	To        DatasetRef `json:"to"`
	JobRunID  string     `json:"job_run_id"`
	CreatedAt time.Time  `json:"created_at"`
}

type AuditEvent struct {
	ID       string            `json:"id"`
	Type     string            `json:"type"`
	At       time.Time         `json:"at"`
	Dataset  *DatasetRef       `json:"dataset,omitempty"`
	JobRunID string            `json:"job_run_id,omitempty"`
	Details  map[string]string `json:"details,omitempty"`
}

func validateName(s string) error {
	if strings.TrimSpace(s) == "" {
		return ErrInvalid
	}
	return nil
}

// Avoid importing strconv in every caller; this helper is intentionally private.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	p := len(buf)
	for i > 0 {
		p--
		buf[p] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		p--
		buf[p] = '-'
	}
	return string(buf[p:])
}
