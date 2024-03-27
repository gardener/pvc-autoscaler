package annotation

const (
	// Prefix is the prefix used by all annotations
	Prefix = "pvc.autoscaling.gardener.cloud/"

	// IsEnabled is the annotation used to specify that autoscaling is
	// enabled for the PVC
	IsEnabled = Prefix + "is-enabled"

	// IncreaseBy is an annotation, which specifies an increase by
	// percentage value (e.g. 10%, 20%, etc.) by which the Persistent Volume
	// Claim storage will be resized.
	IncreaseBy = Prefix + "increase-by"

	// Threshold is an annotation which specifies the threshold value in
	// percentage (e.g. 10%, 20%, etc.) for the PVC. Once the available
	// capacity for the PVC reaches or exceeds the specified threshold this
	// will trigger a resize operation by the controller.
	Threshold = Prefix + "threshold"

	// MaxCapacity is an annotation which specifies the maximum capacity up
	// to which a PVC is allowed to be extended. The max capacity is
	// specified as a [k8s.io/apimachinery/pkg/api/resource.Quantity] value.
	MaxCapacity = Prefix + "max-capacity"

	// LastCheck is the annotation which specifies the time since Unix epoch
	// of the last periodic check.
	LastCheck = Prefix + "last-check"

	// NextCheck is the annotation which specifies the time since Unix epoch
	// at which the next check is scheduled for.
	NextCheck = Prefix + "next-check"

	// UsedSpacePercentage is the annotation which specifies the last
	// observed used space of the PVC as a percentage.
	UsedSpacePercentage = Prefix + "used-space"

	// FreeSpacePercentage is the annotation which specifies the last
	// observed free space of the PVC as a percentage.
	FreeSpacePercentage = Prefix + "free-space"
)
