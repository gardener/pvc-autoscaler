package annotation

const (
	// Prefix is the prefix used by all annotations
	Prefix = "pvc.autoscaling.gardener.cloud/"

	// IsEnabled is the annotation used to specify that autoscaling is
	// enabled for the PVC
	IsEnabled = Prefix + "enable"
)
