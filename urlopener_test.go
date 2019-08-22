package k8sblob

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob"
)

func TestCanOpenBucket(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
	defer cancel()
	bucket, err := blob.OpenBucket(ctx, "kubernetes://")
	require.NoError(err)
	require.NotNil(bucket)
}

func TestCanReadAndWriteObjects(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
	defer cancel()
	bucket, err := blob.OpenBucket(ctx, "kubernetes://")
	require.NoError(err)

	err = bucket.WriteAll(ctx, "test-object", []byte("hello world!"), nil)
	require.NoError(err)

	content, err := bucket.ReadAll(ctx, "test-object")
	require.NoError(err)
	require.Equal("hello world!", string(content))
}

func TestCanDeleteObjects(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
	defer cancel()
	bucket, err := blob.OpenBucket(ctx, "kubernetes://")
	require.NoError(err)

	err = bucket.WriteAll(ctx, "test-object", []byte("hello world!"), nil)
	require.NoError(err)

	err = bucket.Delete(ctx, "test-object")
	require.NoError(err)
}

func TestCanCopyObjects(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
	defer cancel()
	bucket, err := blob.OpenBucket(ctx, "kubernetes://")
	require.NoError(err)

	err = bucket.WriteAll(ctx, "test-object-1", []byte("hello world!"), nil)
	require.NoError(err)
	err = bucket.WriteAll(ctx, "test-object-2", []byte("world hello!"), nil)
	require.NoError(err)

	err = bucket.Copy(ctx, "test-object-2", "test-object-1", nil)
	require.NoError(err)
}
