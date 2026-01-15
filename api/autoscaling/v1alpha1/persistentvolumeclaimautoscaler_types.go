// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PersistentVolumeClaimAutoscalerSpec defines the desired state of
// PersistentVolumeClaimAutoscaler
type PersistentVolumeClaimAutoscalerSpec struct {
	// IncreaseBy specifies an increase by percentage value (e.g. 10%, 20%,
	// etc.) by which the Persistent Volume Claim storage will be resized.
	IncreaseBy string `json:"increaseBy,omitempty"`

	// Threshold specifies the threshold value in percentage (e.g. 10%, 20%,
	// etc.) for the PVC. Once the available capacity (free space) for the
	// PVC reaches or drops below the specified threshold this will trigger
	// a resize operation by the controller.
	Threshold string `json:"threshold,omitempty"`

	// MaxCapacity specifies the maximum capacity up to which a PVC is
	// allowed to be extended. The max capacity is specified as a
	// [k8s.io/apimachinery/pkg/api/resource.Quantity] value.
	MaxCapacity resource.Quantity `json:"maxCapacity,omitempty"`

	// ScaleTargetRef specifies the reference to the PVC which will be
	// managed by the controller.
	ScaleTargetRef corev1.LocalObjectReference `json:"scaleTargetRef,omitempty"`
}

// PersistentVolumeClaimAutoscalerStatus defines the observed state of
// PersistentVolumeClaimAutoscaler
type PersistentVolumeClaimAutoscalerStatus struct {
	// LastCheck specifies the last time the PVC was checked by the controller.
	LastCheck metav1.Time `json:"lastCheck,omitempty"`

	// NextCheck specifies the next scheduled check of the PVC by the
	// controller.
	NextCheck metav1.Time `json:"nextCheck,omitempty"`

	// UsedSpacePercentage specifies the last observed used space of the PVC
	// as a percentage.
	UsedSpacePercentage string `json:"usedSpacePercentage,omitempty"`

	// FreeSpacePercentage specifies the last observed free space of the PVC
	// as a percentage.
	FreeSpacePercentage string `json:"freeSpacePercentage,omitempty"`

	// UsedInodesPercentage specifies the last observed used inodes of the
	// PVC as a percentage.
	UsedInodesPercentage string `json:"usedInodesPercentage,omitempty"`

	// FreeInodesPercentage specifies the last observed free inodes of the
	// PVC as a percentage.
	FreeInodesPercentage string `json:"freeInodesPercentage,omitempty"`

	// PrevSize specifies the previous .status.capacity.storage value of the
	// PVC, just before resizing it.
	PrevSize resource.Quantity `json:"prevSize,omitempty"`

	// NewSize specifies the new size to which the PVC will be resized.
	NewSize resource.Quantity `json:"newSize,omitempty"`

	// Conditions specifies the status conditions.
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=pvca
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.spec.scaleTargetRef.name`
// +kubebuilder:printcolumn:name="Increase By",type=string,JSONPath=`.spec.increaseBy`
// +kubebuilder:printcolumn:name="Threshold",type=string,JSONPath=`.spec.threshold`
// +kubebuilder:printcolumn:name="Max Capacity",type=string,JSONPath=`.spec.maxCapacity`

// PersistentVolumeClaimAutoscaler is the Schema for the
// persistentvolumeclaimautoscalers API
type PersistentVolumeClaimAutoscaler struct {
	metav1.TypeMeta   `json:",inline"`            // nolint:revive
	metav1.ObjectMeta `json:"metadata,omitempty"` // nolint:revive

	Spec   PersistentVolumeClaimAutoscalerSpec   `json:"spec,omitempty"`
	Status PersistentVolumeClaimAutoscalerStatus `json:"status,omitempty"`
}

// SetCondition sets the given [metav1.Condition] for the object.
func (obj *PersistentVolumeClaimAutoscaler) SetCondition(ctx context.Context, klient client.Client, condition metav1.Condition) error {
	patch := client.MergeFrom(obj.DeepCopy())
	conditions := obj.Status.Conditions
	if len(conditions) == 0 {
		conditions = make([]metav1.Condition, 0)
	}
	meta.SetStatusCondition(&conditions, condition)
	obj.Status.Conditions = conditions

	return klient.Status().Patch(ctx, obj, patch)
}

// +kubebuilder:object:root=true

// PersistentVolumeClaimAutoscalerList contains a list of PersistentVolumeClaimAutoscaler
type PersistentVolumeClaimAutoscalerList struct {
	metav1.TypeMeta `json:",inline"` // nolint:revive
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []PersistentVolumeClaimAutoscaler `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PersistentVolumeClaimAutoscaler{}, &PersistentVolumeClaimAutoscalerList{})
}
