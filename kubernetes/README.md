# RoxDB Kubernetes deployment

This guide demonstrates how to deploy RoxDB with:

- StatefulSet and PersistentVolumeClaim
- TLS encryption (self-signed certificates)
- OpenTelemetry collector
- Prometheus monitoring

Using minikube:

```bash
# start minikube
minikube start
# start the dashboard
minikube dashboard
```

Create RoxDB deployment and service with TLS enabled using self-signed certificate:

```bash
# change to kubernetes directory
cd kubernetes

# create OpenTelemetry and Prometheus configs
kubectl apply -f prometheus-config.yaml
kubectl apply -f otel-collector-config.yaml
# deploy OpenTelemetry and Prometheus services
kubectl apply -f otel-collector-deployment.yaml
kubectl apply -f prometheus-deployment.yaml

# Generate Private Key and Self-Signed Certificate
openssl req -x509 -sha256 -nodes -days 365 -newkey rsa:4096 -keyout private.key -out certificate.crt -subj "/C=PL/CN=roxdb.localhost/O=roxdb"
# create the secret TLS
kubectl create secret tls tls-auth --key private.key --cert certificate.crt
# create persistent volume claim
kubectl apply -f roxdb-pvc.yaml
# create RoxDB OpenTelemetry config
kubectl apply -f metrics-config.yaml
# create the stateful set and the service
kubectl apply -f roxdb-deployment.yaml

# check pods
kubectl get pods
# check services
kubectl get services
# describe the pod
kubectl describe pod -l app=roxdb
# view pod logs
kubectl logs -l app=roxdb
```

Get the URL to access RoxDB service:

```bash
minikube service roxdb --url
```

Access Prometheus service:

```bash
minikube service prometheus --url
```

When using gRPC client remember to disable certificate validation (we used the self-signed certification) or when using
grpcurl use the `-insecure` flag.

Delete the stateful set, service, persistent volume claim, and the secret:

```bash
kubectl delete -f roxdb-deployment.yaml
kubectl delete -f roxdb-pvc.yaml
kubectl delete -f prometheus-deployment.yaml
kubectl delete -f otel-collector-deployment.yaml
kubectl delete -f metrics-config.yaml
kubectl delete -f prometheus-config.yaml
kubectl delete -f otel-collector-config.yaml
kubectl delete secret tls-auth
```
