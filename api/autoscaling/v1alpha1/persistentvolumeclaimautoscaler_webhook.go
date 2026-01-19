// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"context"
	"fmt"

	pathvalidation "k8s.io/apimachinery/pkg/api/validation/path"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/gardener/pvc-autoscaler/internal/common"
)

// SetupWebhookWithManager will setup the manager to manage the webhooks
func (r *PersistentVolumeClaimAutoscaler) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(r).
		WithDefaulter(r).
		WithValidator(r).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-autoscaling-gardener-cloud-v1alpha1-persistentvolumeclaimautoscaler,mutating=true,failurePolicy=fail,sideEffects=None,groups=autoscaling.gardener.cloud,resources=persistentvolumeclaimautoscalers,verbs=create;update,versions=v1alpha1,name=mpersistentvolumeclaimautoscaler.kb.io,admissionReviewVersions=v1

var _ webhook.CustomDefaulter = &PersistentVolumeClaimAutoscaler{}

// Default implements [webhook.CustomDefaulter] so a webhook will be registered
// for the type
func (r *PersistentVolumeClaimAutoscaler) Default(ctx context.Context, obj runtime.Object) error {
	pvca, ok := obj.(*PersistentVolumeClaimAutoscaler)
	if !ok {
		return fmt.Errorf("expected PersistentVolumeClaimAutoscaler resource, but got %T", obj)
	}

	for i := range pvca.Spec.VolumePolicies {
		if pvca.Spec.VolumePolicies[i].ScaleUp.UtilizationThresholdPercent == nil {
			defaultThreshold := common.DefaultThresholdValue
			pvca.Spec.VolumePolicies[i].ScaleUp.UtilizationThresholdPercent = &defaultThreshold
		}
		if pvca.Spec.VolumePolicies[i].ScaleUp.StepPercent == nil {
			defaultStep := common.DefaultIncreaseByValue
			pvca.Spec.VolumePolicies[i].ScaleUp.StepPercent = &defaultStep
		}
	}
	return nil
}

// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-autoscaling-gardener-cloud-v1alpha1-persistentvolumeclaimautoscaler,mutating=false,failurePolicy=fail,sideEffects=None,groups=autoscaling.gardener.cloud,resources=persistentvolumeclaimautoscalers,verbs=create;update;delete,versions=v1alpha1,name=vpersistentvolumeclaimautoscaler.kb.io,admissionReviewVersions=v1

var _ webhook.CustomValidator = &PersistentVolumeClaimAutoscaler{}

// ValidateCreate implements [webhook.CustomValidator] so a webhook will be
// registered for the type
func (r *PersistentVolumeClaimAutoscaler) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	return nil, validateResourceSpec(obj)
}

// ValidateUpdate implements [webhook.CustomValidator] so a webhook will be
// registered for the type
func (r *PersistentVolumeClaimAutoscaler) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	return nil, validateResourceSpec(newObj)
}

// ValidateDelete implements [webhook.CustomValidator] so a webhook will be
// registered for the type
func (r *PersistentVolumeClaimAutoscaler) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

// validateResourceSpec validates the resource spec
func validateResourceSpec(obj runtime.Object) error {
	pvca, ok := obj.(*PersistentVolumeClaimAutoscaler)
	if !ok {
		return fmt.Errorf("expected PersistentVolumeClaimAutoscaler resource, but got %T", obj)
	}

	allErrs := make(field.ErrorList, 0)

	if len(pvca.Spec.VolumePolicies) == 0 {
		e := field.Required(field.NewPath("spec.volumePolicies"), "at least one volume policy must be specified")
		allErrs = append(allErrs, e)
	}

	if len(pvca.Spec.VolumePolicies) > 1 {
		e := field.Invalid(field.NewPath("spec.volumePolicies"), pvca.Spec.VolumePolicies, "only one volume policy is supported currently")
		allErrs = append(allErrs, e)
	}

	for i, policy := range pvca.Spec.VolumePolicies {
		policyPath := field.NewPath("spec.volumePolicies").Index(i)
		if policy.MaxCapacity.IsZero() {
			e := field.Invalid(policyPath.Child("maxCapacity"), policy.MaxCapacity, "max capacity must be specified and greater than zero")
			allErrs = append(allErrs, e)
		}

		if !policy.MinCapacity.IsZero() && !policy.MaxCapacity.IsZero() {
			if policy.MinCapacity.Cmp(policy.MaxCapacity) > 0 {
				e := field.Invalid(policyPath.Child("minCapacity"), policy.MinCapacity, "min capacity must be less than or equal to max capacity")
				allErrs = append(allErrs, e)
			}
		}

		scaleUpPath := policyPath.Child("scaleUp")

		if *policy.ScaleUp.UtilizationThresholdPercent <= 0 || *policy.ScaleUp.UtilizationThresholdPercent > 100 {
			e := field.Invalid(scaleUpPath.Child("utilizationThresholdPercent"), *policy.ScaleUp.UtilizationThresholdPercent, "utilization threshold percent must be between 1 and 100")
			allErrs = append(allErrs, e)
		}

		if *policy.ScaleUp.StepPercent <= 0 || *policy.ScaleUp.StepPercent > 100 {
			e := field.Invalid(scaleUpPath.Child("stepPercent"), *policy.ScaleUp.StepPercent, "step percent must be between 1 and 100")
			allErrs = append(allErrs, e)
		}

		if policy.ScaleUp.MinStepAbsolute.IsZero() {
			e := field.Invalid(scaleUpPath.Child("minStepAbsolute"), policy.ScaleUp.MinStepAbsolute, "min step absolute must be specified and greater than zero")
			allErrs = append(allErrs, e)
		}

		if policy.ScaleUp.CooldownDuration.Duration <= 0 {
			e := field.Invalid(scaleUpPath.Child("cooldownDuration"), policy.ScaleUp.CooldownDuration, "cooldown duration must be greater than 0")
			allErrs = append(allErrs, e)
		}
	}

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
