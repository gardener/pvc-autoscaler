# pvc-autoscaler

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
annotate the respective PVCs, e.g.

``` shell
kubectl annotate pvc my-pvc pvc.autoscaling.gardener.cloud/is-enabled=true
kubectl annotate pvc my-pvc pvc.autoscaling.gardener.cloud/max-capacity=100Gi
```

Note, that specifying a max capacity is required.

The following annotations can be configured by an operator in order to control
the behaviour of `pvc-autoscaler`.

| Annotation                                    | Description                                          | Default |
|:---------------------------------------------:|:----------------------------------------------------:|:-------:|
| `pvc.autoscaling.gardener.cloud/is-enabled`   | Enable autoscaling when set to `true`                | N/A     |
| `pvc.autoscaling.gardener.cloud/increase-by`  | Specifies how much to increase the PVC in percentage | `10%`   |
| `pvc.autoscaling.gardener.cloud/threshold`    | Specify the threshold in percentage                  | `10%`   |
| `pvc.autoscaling.gardener.cloud/max-capacity` | Max capacity up to which a PVC can be resized        | N/A     |

The following additional annotations are populated by the controller, which
provide information about the latest observed state for a PVC.

| Annotation                                   | Description                               |
|:--------------------------------------------:|:-----------------------------------------:|
| `pvc.autoscaling.gardener.cloud/free-space`  | Latest observed free space as percentage  |
| `pvc.autoscaling.gardener.cloud/used-space`  | Latest observed used space as percentage  |
| `pvc.autoscaling.gardener.cloud/free-inodes` | Latest observed free inodes as percentage |
| `pvc.autoscaling.gardener.cloud/used-inodes` | Latest observed used inodes as percentage |
| `pvc.autoscaling.gardener.cloud/last-check`  | Last periodic check in Unix time          |
| `pvc.autoscaling.gardener.cloud/next-check`  | Next scheduled check in Unix time         |
| `pvc.autoscaling.gardener.cliud/prev-size`   | Previous known capacity before resize     |

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
