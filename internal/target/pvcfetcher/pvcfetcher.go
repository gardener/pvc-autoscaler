// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package pvcfetcher

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
	"github.com/gardener/pvc-autoscaler/internal/target/selectorfetcher"
)

var (
	// ErrNoClient is returned when the [Fetcher] is configured without a Kubernetes client.
	ErrNoClient = errors.New("no client provided")

	// ErrNoSelectorFetcher is returned when the [Fetcher] is configured without a selector fetcher.
	ErrNoSelectorFetcher = errors.New("no selector fetcher provided")
)

// Fetcher is an interface that can be used to fetch all PersistentVolumeClaims
// that are managed by a PersistentVolumeClaimAutoscaler's targetRef.
type Fetcher interface {
	// Fetch returns all PersistentVolumeClaims that are managed by the given PersistentVolumeClaimAutoscaler's targetRef.
	Fetch(ctx context.Context, pvca *v1alpha1.PersistentVolumeClaimAutoscaler) ([]*corev1.PersistentVolumeClaim, error)
}

type pvcFetcher struct {
	client          client.Client
	selectorFetcher selectorfetcher.Fetcher
}

// Option is a function which configures the [Fetcher].
type Option func(*pvcFetcher)

// New creates a new PVC [Fetcher] with the given options.
func New(opts ...Option) (Fetcher, error) {
	f := &pvcFetcher{}
	for _, opt := range opts {
		opt(f)
	}

	if f.client == nil {
		return nil, ErrNoClient
	}

	if f.selectorFetcher == nil {
		return nil, ErrNoSelectorFetcher
	}

	return f, nil
}

// WithClient configures the [Fetcher] with the given Kubernetes client.
func WithClient(c client.Client) Option {
	return func(f *pvcFetcher) {
		f.client = c
	}
}

// WithSelectorFetcher configures the [Fetcher] with the given fetcher.
func WithSelectorFetcher(sf selectorfetcher.Fetcher) Option {
	return func(f *pvcFetcher) {
		f.selectorFetcher = sf
	}
}

func (f *pvcFetcher) Fetch(ctx context.Context, pvca *v1alpha1.PersistentVolumeClaimAutoscaler) ([]*corev1.PersistentVolumeClaim, error) {
	// For backwards compatibility handle the case where the PVCA target ref points directly to a PVC.
	if pvca.Spec.TargetRef.Kind == "PersistentVolumeClaim" {
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pvca.Spec.TargetRef.Name,
				Namespace: pvca.Namespace,
			},
		}

		if err := f.client.Get(ctx, client.ObjectKeyFromObject(pvc), pvc); err != nil {
			return nil, fmt.Errorf("failed to get PersistentVolumeClaim %s under PersistentVolumeClaimAutoscaler %s: %w", client.ObjectKeyFromObject(pvc), client.ObjectKeyFromObject(pvca), err)
		}

		return []*corev1.PersistentVolumeClaim{pvc}, nil
	}

	selector, err := f.selectorFetcher.Fetch(ctx, pvca.Namespace, pvca.Spec.TargetRef)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch selector for target %s: %w", pvca.Spec.TargetRef.String(), err)
	}

	podList := &corev1.PodList{}
	if err := f.client.List(ctx, podList, &client.ListOptions{LabelSelector: selector, Namespace: pvca.Namespace}); err != nil {
		return nil, fmt.Errorf("failed to list Pods for PersistentVolumeClaimAutoscaler %s: %w", client.ObjectKeyFromObject(pvca), err)
	}

	return f.getPVCsFromPods(ctx, podList.Items, pvca)
}

func (f *pvcFetcher) getPVCsFromPods(ctx context.Context, pods []corev1.Pod, pvca *v1alpha1.PersistentVolumeClaimAutoscaler) ([]*corev1.PersistentVolumeClaim, error) {
	// Use a map to deduplicate PVCs (multiple pods might reference the same PVC)
	pvcMap := make(map[string]*corev1.PersistentVolumeClaim)

	for _, pod := range pods {
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim == nil {
				continue
			}

			pvcKey := client.ObjectKey{
				Namespace: pod.Namespace,
				Name:      volume.PersistentVolumeClaim.ClaimName,
			}

			if _, exists := pvcMap[pvcKey.String()]; exists {
				continue
			}

			pvc := &corev1.PersistentVolumeClaim{}
			if err := f.client.Get(ctx, pvcKey, pvc); err != nil {
				return nil, fmt.Errorf("failed to get PersistentVolumeClaim %s referenced by Pod %s under PersistentVolumeClaimAutoscaler %s: %w", pvcKey, client.ObjectKeyFromObject(&pod), client.ObjectKeyFromObject(pvca), err)
			}

			pvcMap[pvcKey.String()] = pvc
		}
	}

	pvcs := make([]*corev1.PersistentVolumeClaim, 0, len(pvcMap))
	for _, pvc := range pvcMap {
		pvcs = append(pvcs, pvc)
	}

	return pvcs, nil
}
