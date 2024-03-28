package common

import "errors"

// ErrNoMaxCapacity is an error which is returned when a PVC does not specify
// the max capacity.
var ErrNoMaxCapacity = errors.New("no max capacity specified")

// ErrMaxCapacityReached is an error which is returned when the PVC max capacity
// has been reached.
var ErrMaxCapacityReached = errors.New("max capacity reached")

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
