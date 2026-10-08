package lakehouse

import (
	"context"
	"testing"
)

func TestFullVersionHistory(t *testing.T) {
	client, err := NewClient(Config{
		Endpoint:   "127.0.0.1:9000",
		BucketName: "university-lakehouse",
	})
	if err != nil {
		t.Fatalf("failed: %v", err)
	}

	ctx := context.Background()
	streams, err := client.ListStreams(ctx)
	if err != nil {
		t.Fatalf("failed to list streams: %v", err)
	}
	t.Logf("Found %d streams in MinIO staging/ (or legacy bronze paths)", len(streams))

	// Test orders table which has part_0 and part_1!
	res, err := client.QueryVersionHistory(ctx, "tenant_client_01", "orders", "")
	if err != nil {
		t.Fatalf("failed to query orders: %v", err)
	}

	t.Logf("Orders: Files=%d, Records=%d, Unique Entities=%d, Columns=%v",
		res.FilesRead, res.TotalRecords, res.UniqueEntities, res.Columns)

	for i, tl := range res.Timelines {
		if i >= 3 {
			break
		}
		t.Logf("Entity #%d [ID=%s]: Versions=%d, Status=%s", i+1, tl.RecordID, tl.TotalVersions, tl.CurrentStatus)
		for _, v := range tl.Versions {
			t.Logf("   - Ver %d [%s] at %s (current: %v): changes=%+v, data=%+v",
				v.VersionNumber, v.Operation, v.ValidFrom, v.IsCurrent, v.Changes, v.Data)
		}
	}
}
