package k8sblob

import (
	"context"
	"io"
	"io/ioutil"
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

func TestCanListObjects(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
	defer cancel()
	bucket, err := blob.OpenBucket(ctx, "kubernetes://")
	require.NoError(err)

	err = bucket.WriteAll(ctx, "test-object", []byte("hello world!"), nil)
	require.NoError(err)

	results := []string{}
	iter := bucket.List(nil)
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(err)
		if !obj.IsDir {
			results = append(results, obj.Key)
		}
	}

	require.Contains(results, "test-object")
}

func TestCanReadAndWriteBiggerObjects(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
	defer cancel()
	bucket, err := blob.OpenBucket(ctx, "kubernetes://")
	require.NoError(err)

	filename := "./example.json"
	content, err := ioutil.ReadFile(filename)
	require.NoError(err)

	err = bucket.WriteAll(ctx, filename, content, nil)
	require.NoError(err)

	result, err := bucket.ReadAll(ctx, filename)
	require.NoError(err)
	require.Equal(string(content), string(result))
}
