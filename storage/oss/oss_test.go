package oss

import (
	"context"
	"strings"
	"testing"

	"github.com/hkloudou/lake/v3/storage"
)

// TestPresignPut_ForbidsOverwrite: the signed URL must pin
// x-oss-forbid-overwrite so a presigned PUT can create its object only once,
// and the header must be handed to the client (it is part of the signature).
// SignURL is pure local computation, so this needs no network.
func TestPresignPut_ForbidsOverwrite(t *testing.T) {
	c, err := New(Config{Endpoint: "oss-cn-hangzhou", AccessKey: "ak", SecretKey: "sk"})
	if err != nil {
		t.Fatal(err)
	}
	up, err := c.Bucket("bkt").(storage.Presigner).PresignPut(context.Background(), "users", "4f3a/(users/x.dat",
		storage.PresignOptions{UserMetadata: map[string]string{"catalog": "users"}})
	if err != nil {
		t.Fatal(err)
	}
	if up.Headers["x-oss-forbid-overwrite"] != "true" {
		t.Fatalf("headers = %v, want x-oss-forbid-overwrite=true", up.Headers)
	}
	if up.Headers["x-oss-meta-catalog"] != "users" || up.Method != "PUT" || !strings.Contains(up.URL, "Signature=") {
		t.Fatalf("unexpected presign result: %+v", up)
	}
}
