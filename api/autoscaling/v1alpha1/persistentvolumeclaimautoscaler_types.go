// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"context"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ScaleUpPolicy defines the policy for scaling up a PVC
type ScaleUpPolicy struct {
	// UtilizationThresholdPercent specifies the threshold percentage for used space.
	// When the used space reaches or exceeds this threshold, a scale-up is triggered.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=80
	// +optional
	UtilizationThresholdPercent *int `json:"utilizationThresholdPercent,omitempty"`

	// StepPercent specifies the percentage increase for the PVC capacity during scale-up.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=10
	// +optional
	StepPercent *int `json:"stepPercent,omitempty"`

	// MinStepAbsolute specifies the minimum absolute increase in capacity during scale-up.
	// This ensures that the capacity increase is at least this amount, regardless of the percentage.
	// +optional
	// +kubebuilder:default="1Gi"
	MinStepAbsolute *resource.Quantity `json:"minStepAbsolute,omitempty"`

	// CooldownDuration specifies the duration to wait before another scale-up operation.
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('0s')",message="cooldownDuration must be >= 0s"
	// +optional
	CooldownDuration *metav1.Duration `json:"cooldownDuration,omitempty"`
}

// VolumePolicy defines the autoscaling policy for a specific PVC
// +kubebuilder:validation:XValidation:rule="!has(self.minCapacity) || !quantity(self.maxCapacity).isLessThan(quantity(self.minCapacity))",message="maxCapacity must be >= minCapacity"
type VolumePolicy struct {
	// MinCapacity specifies the minimum capacity for the PVC.
	// +kubebuilder:validation:XValidation:rule="self == null || quantity(self).isGreaterThan(quantity('0'))",message="minCapacity must be > 0 if specified"
	// +optional
	MinCapacity *resource.Quantity `json:"minCapacity,omitempty"`

	// MaxCapacity specifies the maximum capacity up to which a PVC is
	// allowed to be extended.
	// +kubebuilder:validation:XValidation:rule="quantity(self).isGreaterThan(quantity('0'))",message="maxCapacity must be > 0"
	MaxCapacity resource.Quantity `json:"maxCapacity"`

	// ScaleUp defines the policy for scaling up the PVC.
	// +kubebuilder:default:={}
	// +optional
	ScaleUp ScaleUpPolicy `json:"scaleUp,omitempty"`
}

// PersistentVolumeClaimAutoscalerSpec defines the desired state of
// PersistentVolumeClaimAutoscaler
type PersistentVolumeClaimAutoscalerSpec struct {
	// TargetRef specifies the reference to the workload controller (e.g., StatefulSet)
	// whose PVCs will be managed by the autoscaler.
	TargetRef autoscalingv1.CrossVersionObjectReference `json:"targetRef"`

	// VolumePolicies defines a list of policies for autoscaling PVCs.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=1
	VolumePolicies []VolumePolicy `json:"volumePolicies"`
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
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.spec.targetRef.name`

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
