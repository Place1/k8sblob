package k8sblob

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/pkg/errors"
	"gocloud.dev/blob"
	"gocloud.dev/blob/driver"
	"gocloud.dev/gcerrors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

var Scheme = "kubernetes"

func init() {
	blob.DefaultURLMux().RegisterBucket(Scheme, new(kubernetesOpener))
}

type kubernetesOpener struct {
	init   sync.Once
	opener *URLOpener
	err    error
}

func (o *kubernetesOpener) OpenBucketURL(ctx context.Context, u *url.URL) (*blob.Bucket, error) {
	o.init.Do(func() {
		kubeconfig, err := ioutil.ReadFile(os.Getenv("KUBECONFIG"))
		if err != nil {
			o.err = errors.Wrap(err, "failed to read kubeconfig file")
			return
		}
		config, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
		if err != nil {
			o.err = errors.Wrap(err, "invalid kubeconfig file")
			return
		}
		config.Timeout = time.Duration(2) * time.Second
		clientset, err := kubernetes.NewForConfig(config)
		if err != nil {
			o.err = errors.Wrap(err, "failed to create kubernetes client from kubeconfig")
			return
		}
		o.opener = &URLOpener{Client: clientset}
	})
	if o.err != nil {
		return nil, fmt.Errorf("open bucket %v: %v", u, o.err)
	}
	return o.opener.OpenBucketURL(ctx, u)
}

type URLOpener struct {
	Client *kubernetes.Clientset
}

func (o *URLOpener) OpenBucketURL(ctx context.Context, u *url.URL) (*blob.Bucket, error) {
	return blob.NewBucket(&Bucket{namespace: "default", client: o.Client}), nil
}

type Bucket struct {
	client    *kubernetes.Clientset
	namespace string // the kubernetes namespace where config maps will be stored
}

func (b *Bucket) ErrorCode(err error) gcerrors.ErrorCode {
	return gcerrors.Unknown
}

func (b *Bucket) As(i interface{}) bool {
	return false
}

func (b *Bucket) ErrorAs(error, interface{}) bool {
	return false
}

func (b *Bucket) Attributes(ctx context.Context, key string) (*driver.Attributes, error) {
	return nil, errors.New("not implemented")
}

func (b *Bucket) ListPaged(ctx context.Context, opts *driver.ListOptions) (*driver.ListPage, error) {
	list, err := b.client.CoreV1().ConfigMaps(b.namespace).List(metav1.ListOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list config maps")
	}
	objects := []*driver.ListObject{}
	for _, value := range list.Items {
		objects = append(objects, &driver.ListObject{
			Key:   value.Data["filename"],
			Size:  int64(len(value.BinaryData["file"])),
			IsDir: false,
		})
	}
	return &driver.ListPage{
		Objects:       objects,
		NextPageToken: nil,
	}, nil
}

func (b *Bucket) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *driver.ReaderOptions) (driver.Reader, error) {
	return NewConfigMapStorageReader(b.client, b.namespace, escapeKey(key), key), nil
}

func (b *Bucket) NewTypedWriter(ctx context.Context, key string, contentType string, opts *driver.WriterOptions) (driver.Writer, error) {
	return NewConfigMapStorageWriter(b.client, b.namespace, escapeKey(key), key), nil
}

func (b *Bucket) Copy(ctx context.Context, dstKey, srcKey string, opts *driver.CopyOptions) error {
	_, err := io.Copy(
		NewConfigMapStorageWriter(b.client, b.namespace, escapeKey(dstKey), dstKey),
		NewConfigMapStorageReader(b.client, b.namespace, escapeKey(srcKey), srcKey),
	)
	return err
}

func (b *Bucket) Delete(ctx context.Context, key string) error {
	return b.client.CoreV1().ConfigMaps(b.namespace).Delete(escapeKey(key), nil)
}

func (b *Bucket) SignedURL(ctx context.Context, key string, opts *driver.SignedURLOptions) (string, error) {
	return "", errors.New("kubernetes does not support signed object urls")
}

func (b *Bucket) Close() error {
	return errors.New("not implemented")
}

var escape = regexp.MustCompile("/")

func escapeKey(key string) string {
	sum := md5.Sum([]byte(key))
	return hex.EncodeToString(sum[:])
}
