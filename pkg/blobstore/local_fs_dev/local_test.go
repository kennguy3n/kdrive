package local_fs_dev_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/kchat/drive/pkg/blobstore"
	"github.com/kchat/drive/pkg/blobstore/local_fs_dev"
	"github.com/kchat/drive/pkg/contracttest"
)

func TestContractSuite(t *testing.T) {
	root := t.TempDir()
	p, err := local_fs_dev.New(root)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	contracttest.RunSuite(t, contracttest.Adapter{
		Name:      "local_fs_dev",
		Store:     p,
		Inventory: p,
	})
}

func TestDiskCacheWarm(t *testing.T) {
	root := t.TempDir()
	p1, err := local_fs_dev.New(root)
	if err != nil {
		t.Fatalf("New p1: %v", err)
	}
	ctx := context.Background()
	key := "warmtest"
	body := []byte("persist me")
	if _, err := putObject(ctx, p1, key, body); err != nil {
		t.Fatalf("Put: %v", err)
	}
	p2, err := local_fs_dev.New(root)
	if err != nil {
		t.Fatalf("New p2: %v", err)
	}
	meta, err := p2.Head(ctx, blobstore.ObjectRef{Key: key})
	if err != nil {
		t.Fatalf("Head after reopen: %v", err)
	}
	if meta.Size != int64(len(body)) {
		t.Errorf("Head size = %d, want %d", meta.Size, len(body))
	}
}

func putObject(ctx context.Context, p *local_fs_dev.Provider, key string, body []byte) (blobstore.PutResult, error) {
	sum := sha256.Sum256(body)
	return p.Put(ctx, blobstore.PutRequest{
		Key:            key,
		Body:           bytes.NewReader(body),
		ExpectedLength: int64(len(body)),
		ChecksumSHA256: hex.EncodeToString(sum[:]),
		ContentType:    "application/octet-stream",
	})
}
