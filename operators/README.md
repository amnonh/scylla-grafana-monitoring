# Setting the monitoring stack using K8s operators
## Namespaces
Prometheus can run in one namespace and monitor all other namespaces. Grafana does not
play well with different namespaces. It would be installed in its own namespace.

## Prometheus
Tested with prometheus-operator-0.59.2. Instal prometheus and alertmanager

We assume that prometheus will be installed under the metrics namespace.

Before installing Prometheus create a metrics namespace:

```kubectl create namespace monitoring```

Use ```./run_prometheus.sh``` to install prometheus with an alert manager.

The following files are apply:

### bundle.yaml
Install the prometheus operator

### rbac.yaml
Set the security for the prometheus instance

### prometheus.yaml
Create a prometheus instance

### service-monitor.yaml
Adds a service monitor the find all scyllla to monitor.

### alertmanager-config.yaml
An example for alertmanager configuration

### prometheus-rule.yaml
An example for prometheus alert rule, this will add a rule that would fire unconditionally.

## Grafana
Tested with grafana-operator-4.6.0 it install Grafana, prometheus datasource and a simple dashboard.
You can use ```run_grafana.sh``` to install and configure Grafana instance.

because it uses Kustomize with a directory you will need to download the operator to get the ```deploy/manifests/``` directory.

It uses the following files:

### deploy/manifests/ directory
deploy the Grafana operator

### Grafana.yaml
deploy a Grafana instance

### Prometheus.yaml
configure a prometheus datasource, you will need to set the ip of the prometheus server.
I couldn't get it to use a DNS. I didn't tried to install Prometheus in the grafana namespace
maybe it would work and DNS will work.

### SimpleDashboard.yaml
Configure a base dashboard
 
