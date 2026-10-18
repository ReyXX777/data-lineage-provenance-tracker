package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"example.com/provenance-tracker/internal/provenance"
)

func main() {
	ctx := context.Background()
	repo := provenance.NewMemoryRepository()
	svc, err := provenance.NewService(repo)
	if err != nil {
		log.Fatal(err)
	}
	raw, err := svc.RegisterDataset(ctx, "orders_raw", "sha256:raw-demo", map[string]string{"format": "csv"})
	if err != nil {
		log.Fatal(err)
	}
	job, err := svc.StartJob(ctx, "clean-orders", []provenance.DatasetRef{raw.Ref})
	if err != nil {
		log.Fatal(err)
	}
	clean, err := svc.RegisterDataset(ctx, "orders_clean", "sha256:clean-demo", map[string]string{"format": "parquet"})
	if err != nil {
		log.Fatal(err)
	}
	job, err = svc.CompleteJob(ctx, job.ID, []provenance.DatasetRef{clean.Ref})
	if err != nil {
		log.Fatal(err)
	}
	lineage, err := svc.Lineage(ctx, clean.Ref, "upstream", 5)
	if err != nil {
		log.Fatal(err)
	}
	result := map[string]any{"job": job, "upstream_lineage": lineage}
	b, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(b))
}
