// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package selectorfetcher

import (
	"context"
	"errors"
	"fmt"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	scaleclient "k8s.io/client-go/scale"
)

var (
	// ErrNoScaleClient is returned when the [Fetcher] is configured without a scale client.
	ErrNoScaleClient = errors.New("no scale client provided")

	// ErrNoRESTMapper is returned when the [Fetcher] is configured without a REST mapper.
	ErrNoRESTMapper = errors.New("no REST mapper provided")
)

// Fetcher is an interface that can be used to fetch the label selector from
// the scale subresource of an autoscalingv1.CrossVersionObjectReference.
type Fetcher interface {
	// Fetch returns the label selector from the scale subresource of the provided
	// autoscalingv1.CrossVersionObjectReference in the provided namespace.
	// If the provided autoscalingv1.CrossVersionObjectReference does not support
	// a scale subresource, an error is returned.
	Fetch(ctx context.Context, namespace string, targetRef autoscalingv1.CrossVersionObjectReference) (labels.Selector, error)
}

type selectorFetcher struct {
	scaleClient scaleclient.ScalesGetter
	restMapper  apimeta.RESTMapper
}

// Option is a function which configures the [Fetcher].
type Option func(*selectorFetcher)

// New creates a new selector [Fetcher] with the given options.
func New(opts ...Option) (Fetcher, error) {
	f := &selectorFetcher{}
	for _, opt := range opts {
		opt(f)
	}

	if f.scaleClient == nil {
		return nil, ErrNoScaleClient
	}

	if f.restMapper == nil {
		return nil, ErrNoRESTMapper
	}

	return f, nil
}

// WithScaleClient configures the [Fetcher] with the given scale client.
func WithScaleClient(sc scaleclient.ScalesGetter) Option {
	return func(f *selectorFetcher) {
		f.scaleClient = sc
	}
}

// WithRESTMapper configures the [Fetcher] with the given REST mapper.
func WithRESTMapper(rm apimeta.RESTMapper) Option {
	return func(f *selectorFetcher) {
		f.restMapper = rm
	}
}

func (f *selectorFetcher) Fetch(ctx context.Context, namespace string, targetRef autoscalingv1.CrossVersionObjectReference) (labels.Selector, error) {
	targetGV, err := schema.ParseGroupVersion(targetRef.APIVersion)
	if err != nil {
		return nil, fmt.Errorf("invalid API version in target reference: %w", err)
	}

	targetGK := schema.GroupKind{
		Group: targetGV.Group,
		Kind:  targetRef.Kind,
	}

	mappings, err := f.restMapper.RESTMappings(targetGK)
	if err != nil {
		return nil, fmt.Errorf("unable to determine resource for scale target reference: %w", err)
	}

	scale, _, err := f.scaleForResourceMappings(ctx, namespace, targetRef.Name, mappings)
	if err != nil {
		return nil, fmt.Errorf("could not get scale subresource for target %s: %w", targetRef.String(), err)
	}

	labelSelector, err := labels.Parse(scale.Status.Selector)
	if err != nil {
		return nil, fmt.Errorf("could not parse label selector for target %s: %w", targetRef.String(), err)
	}

	return labelSelector, nil
}

func (f *selectorFetcher) scaleForResourceMappings(ctx context.Context, namespace, name string, mappings []*apimeta.RESTMapping) (*autoscalingv1.Scale, schema.GroupResource, error) {
	// make sure we handle an empty set of mappings
	if len(mappings) == 0 {
		return nil, schema.GroupResource{}, errors.New("unrecognized resource")
	}

	errs := []error{}
	for _, mapping := range mappings {
		targetGR := mapping.Resource.GroupResource()
		scale, err := f.scaleClient.Scales(namespace).Get(ctx, targetGR, name, metav1.GetOptions{})
		if err == nil {
			return scale, targetGR, nil
		}

		errs = append(errs, err)
	}

	return nil, schema.GroupResource{}, errors.Join(errs...)
}
