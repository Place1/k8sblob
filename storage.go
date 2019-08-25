package k8sblob

import (
	"bytes"
	"io"
	"sync"

	"github.com/pkg/errors"
	"gocloud.dev/blob/driver"
	v1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type ConfigMapStorageReader struct {
	io.Reader
	namespace string
	name      string
	filename  string
	client    *kubernetes.Clientset
	buffer    io.Reader
	init      sync.Once
	err       error
}

type ConfigMapStorageWriter struct {
	io.Writer
	buffer    *bytes.Buffer
	namespace string
	name      string
	filename  string
	client    *kubernetes.Clientset
}

func NewConfigMapStorageReader(client *kubernetes.Clientset, namespace string, name string, filename string) *ConfigMapStorageReader {
	return &ConfigMapStorageReader{
		namespace: namespace,
		name:      name,
		filename:  filename,
		client:    client,
	}
}

func NewConfigMapStorageWriter(client *kubernetes.Clientset, namespace string, name string, filename string) *ConfigMapStorageWriter {
	buffer := new(bytes.Buffer)
	return &ConfigMapStorageWriter{
		Writer:    buffer,
		buffer:    buffer,
		namespace: namespace,
		name:      name,
		filename:  filename,
		client:    client,
	}
}

func (w *ConfigMapStorageReader) Read(p []byte) (int, error) {
	w.init.Do(func() {
		configMap, err := w.client.CoreV1().ConfigMaps(w.namespace).Get(w.name, metav1.GetOptions{})
		if err != nil {
			w.err = err
		} else {
			w.buffer = bytes.NewBuffer(configMap.BinaryData["file"])
		}
	})
	if w.err != nil {
		return 0, errors.Wrapf(w.err, "failed to read filename %v", w.filename)
	}
	return w.buffer.Read(p)
}

func (w *ConfigMapStorageReader) Close() error {
	return nil
}

func (w *ConfigMapStorageWriter) Close() error {
	binaryData := map[string][]byte{}
	binaryData["file"] = w.buffer.Bytes()
	next := &v1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "core/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      w.name,
			Namespace: w.namespace,
		},
		BinaryData: binaryData,
		Data: map[string]string{
			"filename": w.filename,
		},
	}

	_, err := w.client.CoreV1().ConfigMaps(w.namespace).Get(w.name, metav1.GetOptions{})
	if kerrors.IsNotFound(err) {
		_, err = w.client.CoreV1().ConfigMaps(w.namespace).Create(next)
		if err != nil {
			return errors.Wrap(err, "failed to create new config map for object")
		}
		return nil
	} else if err != nil {
		return errors.Wrap(err, "failed to check for an existing config map")
	}

	_, err = w.client.CoreV1().ConfigMaps(w.namespace).Update(next)
	if err != nil {
		return errors.Wrap(err, "failed to update existing config map")
	}

	return nil
}

func (w *ConfigMapStorageReader) Attributes() *driver.ReaderAttributes {
	return nil
}

func (w *ConfigMapStorageReader) As(interface{}) bool {
	return false
}
