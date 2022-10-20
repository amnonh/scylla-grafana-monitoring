#!/usr/bin/bash
DRY_RUN="0"
if [ "$1" = "--dry-run" ]; then
    echo "dry run"
    DRY_RUN="1"
fi
echo
NS=`kubectl get namespaces |grep monitoring`
if [ "$NS" = "" ]; then
    echo "create monitoring namespace: kubectl create namespace monitoring"
    if [ $DRY_RUN = "0" ]; then
        exit
    fi
fi
NAMESPACE="-n monitoring"
cd prometheus-operator-0.59.2
kubectl apply --dry-run=server -f bundle.yaml --server-side $NAMESPACE
if [ $DRY_RUN = "0" ]; then
  kubectl apply -f bundle.yaml --server-side $NAMESPACE
else
    echo kubectl apply -f bundle.yaml --server-side $NAMESPACE
fi
echo
echo "Checking CRDS"
kubectl get crds $NAMESPACE |grep monitor
CRD=`kubectl get crds|grep monitor|wc -l`
if [ "$CRD" != "8" ]; then
    echo "Something wronge with the CRDs"
    if [ $DRY_RUN = "0" ]; then
        exit
    else
        echo "fund $CRD"
    fi
fi
echo
echo "Check deploy"
kubectl get deploy $NAMESPACE

echo
echo "Check pods"
kubectl get pods $NAMESPACE

echo
echo "Check service"
kubectl get service $NAMESPACE

echo
echo "configure rbac.yaml"
if [ $DRY_RUN = "0" ]; then
    kubectl apply -f rbac.yaml $NAMESPACE
else
    echo kubectl apply -f rbac.yaml $NAMESPACE
fi
echo "Check RBAC configuration"
kubectl describe clusterrolebinding prometheus $NAMESPACE
echo
echo "Creating the Prometheus service"
if [ $DRY_RUN = "0" ]; then
    kubectl apply -f prometheus.yaml $NAMESPACE
else
    echo kubectl apply -f prometheus.yaml $NAMESPACE
fi
kubectl get prometheus $NAMESPACE
kubectl get pods $NAMESPACE
echo
echo "Configuring Scylla scrap"
if [ $DRY_RUN = "0" ]; then
    kubectl apply -f service-monitor.yaml $NAMESPACE
else
    echo kubectl apply -f service-monitor.yaml $NAMESPACE
fi
echo
echo "Done"
echo "To search for logs: kubectl logs -f prometheus-prometheus-0 -c prometheus"
echo "to open the dashboard run: kubectl port-forward svc/prometheus-operated $NAMESPACE 9090:9090"

echo "Creating an alertmanager Configurating"
if [ $DRY_RUN = "0" ]; then
    kubectl apply -f alertmanager-config.yaml $NAMESPACE
else
    echo kubectl apply -f alertmanager-config.yaml $NAMESPACE
fi
echo "Creating an alertmanager"
if [ $DRY_RUN = "0" ]; then
    kubectl apply -f alertmanager.yaml $NAMESPACE
else
    echo kubectl apply -f alertmanager.yaml $NAMESPACE
fi
echo "To connect to the alert manager: kubectl port-forward svc/alertmanager-operated $NAMESPACE  9093:9093"

echo "Creating and alert rule"
if [ $DRY_RUN = "0" ]; then
    kubectl apply -f prometheus-rule.yaml $NAMESPACE
else
    echo kubectl apply -f prometheus-rule.yaml $NAMESPACE
fi
