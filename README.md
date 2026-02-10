# pvc-autoscaler

[![REUSE status](https://api.reuse.software/badge/github.com/gardener/pvc-autoscaler)](https://api.reuse.software/info/github.com/gardener/pvc-autoscaler)

`pvc-autoscaler` is a Kubernetes controller which periodically monitors
[persistent volumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) and
resizes them, if the available space or number of inodes drops below a certain
threshold.

# Requirements

- Kubernetes cluster
- Storage class with enabled
  [volume expansion](https://kubernetes.io/docs/concepts/storage/storage-classes/#allow-volume-expansion)
- Metrics source (currently only [Prometheus](https://prometheus.io/) is supported)
- [minikube](https://minikube.sigs.k8s.io/docs/) (for local development)

# Installation

There are a few ways to install `pvc-autoscaler`.

In order to install `pvc-autoscaler` within an existing Kubernetes cluster,
please refer to the included [dist/install.yaml](./dist/install.yaml) bundle.

``` shell
kubectl apply -f dist/install.yaml
```

Or you can install it this way instead.

``` shell
kubectl apply -f https://raw.githubusercontent.com/gardener/pvc-autoscaler/master/dist/install.yaml
```

Note, that the provided install bundle uses
[cert-manager](https://cert-manager.io/) for issuing TLS certificates for the
admission webhook server. If you intend on using another approach for issuing
TLS certificates, make sure to adjust the `default` kustomization instead.

In addition to that you could also install `pvc-autoscaler` using the default
[kustomization](https://kubectl.docs.kubernetes.io/references/kustomize/glossary/#kustomization)
located in [config/default](./config/default). Note that the `default`
kustomization is meant to be used in dev setups. For non-dev setups it is
recommended that you create an overlay using the default kustomization as a base.

``` shell
kustomize build config/default | kubectl apply -f -
```

`pvc-autoscaler` uses Prometheus as a metrics source in order to monitor the
persistent volumes usage. By default it will use the
http://prometheus-k8s.monitoring.svc.cluster.local:9090 endpoint.

If your Prometheus instance is running at a different endpoint, make sure that
you configure the `--prometheus-address` option for the
`pvc-autoscaler-controller-manager` deployment.

# Usage

In order to start monitoring and automatically resize a persistent volume, when
the available space or inodes drop below a certain threshold you need to
create a new `PersistentVolumeClaimAutoscaler` resource, e.g.

``` yaml
---
apiVersion: autoscaling.gardener.cloud/v1alpha1
kind: PersistentVolumeClaimAutoscaler
metadata:
  name: my-pvca
spec:
  targetRef:
    apiVersion: v1
    kind: PersistentVolumeClaim
    name: my-pvc
  volumePolicies:
  - maxCapacity: 3Gi
    scaleUp:
      utilizationThresholdPercent: 80
      stepPercent: 10
      minStepAbsolute: 1Gi
      cooldownDuration: 3600s
```

The following properties must be specified when creating a new
`PersistentVolumeClaimAutoscaler` resource.

| Property                                                     | Description                                                         | Default |
|:-------------------------------------------------------------|:--------------------------------------------------------------------|:-------:|
| `.spec.targetRef.name`                                       | Name of the PVC to monitor and autoscale                            | N/A     |
| `.spec.volumePolicies[].maxCapacity`                         | Max capacity up to which a PVC can be resized                       | N/A     |
| `.spec.volumePolicies[].scaleUp.utilizationThresholdPercent` | Threshold percentage for used space/inodes that triggers a resize   | `80`    |
| `.spec.volumePolicies[].scaleUp.stepPercent`                 | Percentage by which to increase the PVC during resize               | `10`    |
| `.spec.volumePolicies[].scaleUp.minStepAbsolute`             | Minimum absolute increase in capacity during scale-up               | `1Gi`   |
| `.spec.volumePolicies[].scaleUp.cooldownDuration`            | Duration to wait before another scale-up operation (currently NOOP) | N/A     |

In order to watch the status of the autoscaler you can `kubectl describe` your
`PersistentVolumeClaimAutoscaler` resource, where you will find information
about the latest observed state, last and next scheduled check, status
conditions, etc.

# Local development

The local environment of `pvc-autoscaler` uses
[minikube](https://minikube.sigs.k8s.io/docs/) for bootstrapping a dev
Kubernetes cluster.

In order to start a new local environment execute the following command.

``` shell
make minukube-start
```

The command above will install [OpenEBS](https://openebs.io/), the
[LVM-LocalPV CSI driver](https://github.com/openebs/lvm-localpv) and configure the
`openebs-lvm` storage class with volume expansion support.

Along with that it also installs
[kube-prometheus](https://github.com/prometheus-operator/kube-prometheus), which
provides Prometheus, Grafana and Alert Manager.

By default the minikube profile will be named `pvc-autoscaler`, which can be
configured via the `MINIKUBE_PROFILE` env variable, e.g.

``` shell
MINIKUBE_PROFILE=my-dev-env make minikube-start
```

The default [minikube driver](https://minikube.sigs.k8s.io/docs/drivers/) is set
to `qemu`, which can be configured via the `MINIKUBE_DRIVER` env variable.

For example, if you are on GNU/Linux and have
[KVM](https://en.wikipedia.org/wiki/Kernel-based_Virtual_Machine) you would set
the driver to `kvm2`, e.g.

``` shell
MINIKUBE_DRIVER=kvm2 make minikube-start
```

In order to build and load an image of `pvc-autoscaler` into your `minikube`
nodes execute the following command.

``` shell
make minikube-load-image
```

The following command will deploy `pvc-autoscaler` in the cluster.

``` shell
make deploy
```

If you want to run `pvc-autoscaler` outside of the Kubernetes cluster you can
run this command instead.

``` shell
make run
```

When done with the local environment you can delete it using the following
command.

``` shell
make minikube-stop
```

Also, make sure to check the help information about each Makefile target by
executing this command.

``` shell
make help
```

The local setup also supports Kind Kubernetes clusters. For convenience you can create a dev Kind cluster by using the following command.

```shell
make kind-up
```

You can also deploy the `pvc-autoscaler` in the kind cluster via this command.

```shell
make pvc-autoscaler-up
```

If you want automatic deploy on change in code, you can use this command.

```shell
make pvc-autoscaler-dev
```

When you're done with development, you can safely run the following command.

```shell
make kind-down
```

# Tests

Run the unit tests.

``` shell
make test
```

In order to run the end-to-end tests we need a clean test environment.  The
following command will create a new test environment, build and deploy the
operator image, run the e2e tests against it and finally destroy the test
environment.

``` shell
make test-e2e
```

If you need to create a clean environment for e2e tests and run the tests
manually, then you can run the following command which will setup the test
environment, load the Docker images into the minikube nodes and deploy the
operator in the cluster.

``` shell
make e2e-env-setup
```

Then you can manually run the e2e tests by executing the following script.

``` shell
./hack/run-e2e-tests.sh
```

In order to remove the test e2e environment execute the following.

``` shell
make e2e-env-teardown
```

# Contributing

`pvc-autoscaler` is hosted on
[Github](https://github.com/gardener/pvc-autoscaler). Please contribute by
reporting issues, suggesting features or by sending patches using pull requests.

# License

This project is Open Source and licensed under
[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
