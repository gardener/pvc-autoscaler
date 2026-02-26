// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package selectorfetcher_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	scalefake "k8s.io/client-go/scale/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/gardener/pvc-autoscaler/internal/target/selectorfetcher"
)

var _ = Describe("SelectorFetcher", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	Describe("New", func() {
		It("should return an error when no scale client is provided", func() {
			mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{})
			_, err := selectorfetcher.New(
				selectorfetcher.WithRESTMapper(mapper),
			)
			Expect(err).To(Equal(selectorfetcher.ErrNoScaleClient))
		})

		It("should return an error when no REST mapper is provided", func() {
			scaleClient := &scalefake.FakeScaleClient{}
			_, err := selectorfetcher.New(
				selectorfetcher.WithScaleClient(scaleClient),
			)
			Expect(err).To(Equal(selectorfetcher.ErrNoRESTMapper))
		})

		It("should create a fetcher when all required options are provided", func() {
			scaleClient := &scalefake.FakeScaleClient{}
			mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{})
			fetcher, err := selectorfetcher.New(
				selectorfetcher.WithScaleClient(scaleClient),
				selectorfetcher.WithRESTMapper(mapper),
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(fetcher).ToNot(BeNil())
		})
	})

	Describe("Fetch", func() {
		var (
			scaleClient     *scalefake.FakeScaleClient
			mapper          *meta.DefaultRESTMapper
			selectorFetcher selectorfetcher.Fetcher

			targetRef autoscalingv1.CrossVersionObjectReference
		)

		BeforeEach(func() {
			scaleClient = &scalefake.FakeScaleClient{}

			mapper = meta.NewDefaultRESTMapper([]schema.GroupVersion{appsv1.SchemeGroupVersion})
			mapper.Add(appsv1.SchemeGroupVersion.WithKind("StatefulSet"), meta.RESTScopeNamespace)

			targetRef = autoscalingv1.CrossVersionObjectReference{
				Kind:       "StatefulSet",
				Name:       "test-sts",
				APIVersion: "apps/v1",
			}
		})

		JustBeforeEach(func() {
			var err error
			selectorFetcher, err = selectorfetcher.New(
				selectorfetcher.WithScaleClient(scaleClient),
				selectorfetcher.WithRESTMapper(mapper),
			)
			Expect(err).ToNot(HaveOccurred())
		})

		When("REST mapper is empty", func() {
			BeforeEach(func() {
				mapper = meta.NewDefaultRESTMapper([]schema.GroupVersion{}) // Empty mapper
			})

			It("should return an error because REST mapper cannot find resource mappings", func() {
				_, err := selectorFetcher.Fetch(ctx, "default", targetRef)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("unable to determine resource"))
			})
		})

		When("REST mapper is not empty", func() {
			It("should return an error because REST mapper does not have mapping for Deployment", func() {
				targetRef = autoscalingv1.CrossVersionObjectReference{
					Kind:       "Deployment",
					Name:       "test-sts",
					APIVersion: "apps/v1",
				}

				_, err := selectorFetcher.Fetch(ctx, "default", targetRef)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("unable to determine resource"))
			})

			It("should return a label selector when scale subresource is found", func() {
				scale := &autoscalingv1.Scale{
					Status: autoscalingv1.ScaleStatus{
						Selector: "app=test,version=v1",
					},
				}

				scaleClient.AddReactor("get", "statefulsets/scale", func(action k8stesting.Action) (bool, runtime.Object, error) {
					return true, scale, nil
				})

				selector, err := selectorFetcher.Fetch(ctx, "default", targetRef)
				Expect(err).ToNot(HaveOccurred())
				Expect(selector).ToNot(BeNil())
				Expect(selector.Matches(labels.Set{
					"app":     "test",
					"version": "v1",
				})).To(BeTrue())
			})

			It("should return an error when scale selector is invalid", func() {
				scale := &autoscalingv1.Scale{
					Status: autoscalingv1.ScaleStatus{
						Selector: "!@#$invalid",
					},
				}

				scaleClient.AddReactor("get", "statefulsets/scale", func(action k8stesting.Action) (bool, runtime.Object, error) {
					return true, scale, nil
				})

				_, err := selectorFetcher.Fetch(ctx, "default", targetRef)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("could not parse label selector"))
			})
		})
	})
})
