// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"context"

	pathvalidation "k8s.io/apimachinery/pkg/api/validation/path"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// SetupWebhookWithManager will setup the manager to manage the webhooks
func (r *PersistentVolumeClaimAutoscaler) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &PersistentVolumeClaimAutoscaler{}).
		WithValidator(r).
		Complete()
}

// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-autoscaling-gardener-cloud-v1alpha1-persistentvolumeclaimautoscaler,mutating=false,failurePolicy=fail,sideEffects=None,groups=autoscaling.gardener.cloud,resources=persistentvolumeclaimautoscalers,verbs=create;update;delete,versions=v1alpha1,name=vpersistentvolumeclaimautoscaler.kb.io,admissionReviewVersions=v1

var _ admission.Validator[*PersistentVolumeClaimAutoscaler] = &PersistentVolumeClaimAutoscaler{}

// ValidateCreate implements [admission.Validator] so a webhook will be
// registered for the type
func (r *PersistentVolumeClaimAutoscaler) ValidateCreate(ctx context.Context, obj *PersistentVolumeClaimAutoscaler) (admission.Warnings, error) {
	return nil, validateResourceSpec(obj)
}

// ValidateUpdate implements [admission.Validator] so a webhook will be
// registered for the type
func (r *PersistentVolumeClaimAutoscaler) ValidateUpdate(ctx context.Context, oldObj, newObj *PersistentVolumeClaimAutoscaler) (admission.Warnings, error) {
	return nil, validateResourceSpec(newObj)
}

// ValidateDelete implements [admission.Validator] so a webhook will be
// registered for the type
func (r *PersistentVolumeClaimAutoscaler) ValidateDelete(ctx context.Context, obj *PersistentVolumeClaimAutoscaler) (admission.Warnings, error) {
	return nil, nil
}

// validateResourceSpec validates the resource spec
func validateResourceSpec(pvca *PersistentVolumeClaimAutoscaler) error {
	allErrs := make(field.ErrorList, 0)

	if len(pvca.Spec.TargetRef.Kind) == 0 {
		allErrs = append(allErrs, field.Required(field.NewPath("spec.targetRef.kind"), ""))
	} else {
		for _, msg := range pathvalidation.IsValidPathSegmentName(pvca.Spec.TargetRef.Kind) {
			allErrs = append(allErrs, field.Invalid(field.NewPath("spec.targetRef.kind"), pvca.Spec.TargetRef.Kind, msg))
		}
	}

	if pvca.Spec.TargetRef.Kind != "PersistentVolumeClaim" {
		e := field.Invalid(field.NewPath("spec.targetRef.kind"), pvca.Spec.TargetRef.Kind, "only PersistentVolumeClaim kind is supported")
		allErrs = append(allErrs, e)
	}

	if len(pvca.Spec.TargetRef.Name) == 0 {
		allErrs = append(allErrs, field.Required(field.NewPath("spec.targetRef.name"), ""))
	} else {
		for _, msg := range pathvalidation.IsValidPathSegmentName(pvca.Spec.TargetRef.Name) {
			allErrs = append(allErrs, field.Invalid(field.NewPath("spec.targetRef.name"), pvca.Spec.TargetRef.Name, msg))
		}
	}

	if len(pvca.Spec.TargetRef.APIVersion) == 0 {
		e := field.Required(field.NewPath("spec.targetRef.apiVersion"), "")
		allErrs = append(allErrs, e)
	}

	if pvca.Spec.TargetRef.APIVersion != "v1" {
		e := field.Invalid(field.NewPath("spec.targetRef.apiVersion"), pvca.Spec.TargetRef.APIVersion, "only v1 apiVersion is supported")
		allErrs = append(allErrs, e)
	}

	return allErrs.ToAggregate()
}
