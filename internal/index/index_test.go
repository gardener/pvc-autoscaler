package index_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gardener/pvc-autoscaler/internal/annotation"
	"github.com/gardener/pvc-autoscaler/internal/index"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("Index", func() {
	Context("# IndexerFunc", func() {
		pvc1 := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "sample-pvc",
				Annotations: map[string]string{
					annotation.IsEnabled: "true",
				},
			},
		}

		pvc2 := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "sample-pvc",
				Annotations: map[string]string{
					annotation.IsEnabled: "false",
				},
			},
		}

		pvc3 := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "sample-pvc",
				Annotations: map[string]string{},
			},
		}

		pod1 := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "sample-pod",
				Annotations: map[string]string{},
			},
		}

		Expect(index.IndexerFunc(pvc1)).To(Equal([]string{"true"}))
		Expect(index.IndexerFunc(pvc2)).To(Equal([]string{"false"}))
		Expect(index.IndexerFunc(pvc3)).To(Equal([]string{}))
		Expect(index.IndexerFunc(pod1)).To(Equal([]string{}))
	})
})
