package k8sblob

import (
	"bytes"
	"io"

	"github.com/pkg/errors"
	"gocloud.dev/blob/driver"
	v1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type ConfigMapStorageReader struct {
	namespace string
	name      string
	filename  string
	client    *kubernetes.Clientset
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
	configMap, err := w.client.CoreV1().ConfigMaps(w.namespace).Get(w.name, metav1.GetOptions{})
	if err != nil {
		return 0, err
	}
	return copy(p, configMap.BinaryData["file"]), io.EOF
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
