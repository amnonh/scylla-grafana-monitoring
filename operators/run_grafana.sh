#!/usr/bin/bash
DRY_RUN="0"
if [ $1 = "--dry-run" ]; then
    echo "dry run"
    DRY_RUN="1"
fi
VERSION="4.6.0"
NAMESPACE="-n grafana-operator-system"
if [ ! -d "grafana-operator-$VERSION" ]; then
    echo "Downloading the Grafana operator "
    wget https://github.com/grafana-operator/grafana-operator/archive/refs/tags/v$VERSION.tar.gz
fi

DIR="grafana-operator-$VERSION"
echo "deploying the operator using Kustomize, kubectl version should be 1.21 and higher"
echo kubectl apply -k "$DIR/deploy/manifests/"
if [ $DRY_RUN = "0" ]; then
    kubectl apply -k "$DIR/deploy/manifests/"
fi
echo "Checking CRDS"
kubectl get crds |grep grafana
CRD=`kubectl get crds|grep grafana|wc -l`
if [ "$CRD" != "4" ]; then
    echo "Something wronge with the CRDs"
    if [ $DRY_RUN = "0" ]; then
        exit
    else
        echo "fund $CRD"
    fi
fi

echo "Creating grafana"
echo kubectl create -f grafana_files/Grafana.yaml $NAMESPACE
if [ $DRY_RUN = "0" ]; then
    kubectl create -f grafana_files/Grafana.yaml $NAMESPACE
fi

kubectl get grafana $NAMESPACE

echo
echo "Getting Prometheus IP"
IP=`kubectl get pods -n monitoring -o wide|grep prometheus-prometheus|awk '{print $6}'`
sed "s/url: .*/url: http:\/\/$IP:9090/" grafana_files/Prometheus.yaml > Prometheus.yaml
echo "Configuring Prometheus datasource" 
echo "kubectl apply -f  Prometheus.yaml -n grafana-operator-system"
if [ $DRY_RUN = "0" ]; then
  kubectl apply -f  Prometheus.yaml -n grafana-operator-system
fi

echo
echo "Setting a dashbaord"
echo "kubectl apply -f grafana_files/SimpleDashboard.yaml -n grafana-operator-system"
if [ $DRY_RUN = "0" ]; then
   kubectl apply -f grafana_files/SimpleDashboard.yaml -n grafana-operator-system
fi

echo "to open the dashboard run: kubectl port-forward grafana-deployment-57c967d79c-l7hj6 3000:3000 $NAMESPACE"

