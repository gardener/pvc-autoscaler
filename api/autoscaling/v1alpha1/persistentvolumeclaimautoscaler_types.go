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

// PersistentVolumeClaimAutoscalerSpec defines the desired state of the PersistentVolumeClaimAutoscaler.
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

// VolumePolicy defines the autoscaling policy for a specific PVC
type VolumePolicy struct {
	// MaxCapacity specifies the maximum capacity up to which a PVC is
	// allowed to be extended. The max capacity is specified as a
	// [k8s.io/apimachinery/pkg/api/resource.Quantity] value.
	// +kubebuilder:validation:XValidation:rule="quantity(self).isGreaterThan(quantity('0'))",message="maxCapacity must be > 0"
	MaxCapacity resource.Quantity `json:"maxCapacity"`

	// ScaleUp defines the rules for scaling up the PVC.
	// +kubebuilder:default:={}
	// +optional
	ScaleUp *ScalingRules `json:"scaleUp,omitempty"`
}

// ScalingRules defines the rules for scaling a PVC.
type ScalingRules struct {
	// UtilizationThresholdPercent specifies the threshold percentage for used space and inodes.
	// When the used space or inodes passes this threshold, the PVC is scaled.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=80
	// +optional
	UtilizationThresholdPercent *int `json:"utilizationThresholdPercent,omitempty"`

	// StepPercent specifies the percentage by which to change the PVC storage capacity when scaling.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=10
	// +optional
	StepPercent *int `json:"stepPercent,omitempty"`

	// MinStepAbsolute specifies the minimum absolute change in capacity during scaling.
	// This ensures that the change in capacity is at least this amount, regardless of the percentage.
	// +kubebuilder:validation:XValidation:rule="self == null || quantity(self).compareTo(quantity('1Gi')) >= 0",message="minStepAbsolute must be > 1 if specified"
	// +kubebuilder:default="1Gi"
	// +optional
	MinStepAbsolute *resource.Quantity `json:"minStepAbsolute,omitempty"`

	// This field is currenntly not used (NOOP), but will be implemented at a later stage.
	// CooldownDuration specifies the duration to wait before another scaling operation.
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="cooldownDuration must be > 0s"
	// +optional
	CooldownDuration *metav1.Duration `json:"cooldownDuration,omitempty"`
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
