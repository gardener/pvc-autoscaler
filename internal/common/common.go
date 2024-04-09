// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package common

import "errors"

// ErrNoMaxCapacity is an error which is returned when a PVC does not specify
// the max capacity.
var ErrNoMaxCapacity = errors.New("no max capacity specified")

// ErrZeroPercentage is returned whenever we expect a non-zero percentage
// values, but zero was found.
var ErrZeroPercentage = errors.New("zero percentage")

// ErrNoEventRecorder is returned when there was no event recorder specified to
// either the controller or any of the runnables, which need to use a recorder.
var ErrNoEventRecorder = errors.New("no event recorder provided")

// ErrNoEventChannel is returned when no event channel was configured for the
// controller.
var ErrNoEventChannel = errors.New("no event channel provided")

const (
	// ControllerName is the name of the controller
	ControllerName = "pvc_autoscaler"

	// DefaultThreshold is the default threshold value, if not specified for
	// a PVC object.
	DefaultThresholdValue = "10%"

	// DefaultIncreaseByValue is the default increase-by value, if not
	// specified for a PVC object.
	DefaultIncreaseByValue = "10%"
)
