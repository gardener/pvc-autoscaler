// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package pod

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/gardener/pvc-autoscaler/internal/common"
	"github.com/gardener/pvc-autoscaler/internal/utils"
)

// WebhookPath is the path the Pod mutating webhook is served on.
const WebhookPath = "/mutate--v1-pod"

// +kubebuilder:webhook:path=/mutate--v1-pod,mutating=true,failurePolicy=ignore,sideEffects=None,groups="",resources=pods,verbs=create,versions=v1,name=mpod.autoscaling.gardener.cloud,admissionReviewVersions=v1

// Mutator is an admission webhook handler which adds the offline-resize
// scheduling gate to Pods whose PersistentVolumeClaim failed to resize online.
type Mutator struct {
	client         client.Client
	decoder        admission.Decoder
	autoscalerName string
}

var _ admission.Handler = (*Mutator)(nil)

// NewMutator returns a new Pod [Mutator]. It resolves PVC ownership scoped to
// PersistentVolumeClaimAutoscalers whose spec.autoscalerName matches
// autoscalerName, so it gates exactly the Pods whose PVCs the offline-resize
// recovery controller of this instance manages.
func NewMutator(c client.Client, decoder admission.Decoder, autoscalerName string) *Mutator {
	return &Mutator{
		client:         c,
		decoder:        decoder,
		autoscalerName: autoscalerName,
	}
}

// SetupWebhookWithManager registers the Pod mutating webhook with the manager's
// webhook server under [WebhookPath].
func (m *Mutator) SetupWebhookWithManager(mgr ctrl.Manager) error {
	mgr.GetWebhookServer().Register(WebhookPath, &admission.Webhook{Handler: m})
	return nil
}

// Handle implements [admission.Handler].
func (m *Mutator) Handle(ctx context.Context, req admission.Request) admission.Response {
	logger := log.FromContext(ctx)

	pod := &corev1.Pod{}
	if err := m.decoder.Decode(req, pod); err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	namespace := pod.Namespace

	if m.shouldGate(ctx, logger, namespace, pod) {
		utils.AddSchedulingGate(pod, common.SchedulingGateOfflineResize)
		logger.Info("adding offline-resize scheduling gate to pod", "namespace", namespace, "generateName", pod.GenerateName)
	}

	marshaled, err := json.Marshal(pod)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	return admission.PatchResponseFromRaw(req.Object.Raw, marshaled)
}

// shouldGate reports whether the given Pod should receive the offline-resize
// scheduling gate.
func (m *Mutator) shouldGate(ctx context.Context, logger logr.Logger, namespace string, pod *corev1.Pod) bool {
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}

		pvc := &corev1.PersistentVolumeClaim{}
		key := client.ObjectKey{Namespace: namespace, Name: volume.PersistentVolumeClaim.ClaimName}
		if err := m.client.Get(ctx, key, pvc); err != nil {
			if apierrors.IsNotFound(err) {
				// The PVC does not exist yet - nothing to gate on.
				continue
			}

			logger.Error(err, "failed to get PersistentVolumeClaim referenced by pod, not gating", "pvc", key)

			continue
		}

		if !utils.IsPersistentVolumeClaimConditionTrue(pvc, corev1.PersistentVolumeClaimControllerResizeError) || utils.IsPersistentVolumeClaimConditionTrue(pvc, corev1.PersistentVolumeClaimFileSystemResizePending) {
			continue
		}

		owner, policy, err := utils.FindOwningPVCAAndPolicy(ctx, m.client, m.autoscalerName, pvc)
		if err != nil {
			// Do not block Pod creation because of a transient lookup error.
			logger.Error(err, "failed to determine ownership of PersistentVolumeClaim referenced by pod, not gating", "pvc", key)

			continue
		}

		if owner != nil && utils.IsOfflineResizeEnabled(owner, policy) {
			return true
		}
	}

	return false
}
