# RoxDB Kubernetes deployment

Using minikube:

```bash
# start minikube
minikube start
# load local roxdb image into minikube
minikube image load --overwrite=true roxdb:latest
# start the dashboard
minikube dashboard
```

Create RoxDB deployment and service with TLS enabled using self-signed certificate:

```bash
# change to kubernetes directory
cd kubernetes
# Generate Private Key and Self-Signed Certificate
openssl req -x509 -sha256 -nodes -days 365 -newkey rsa:4096 -keyout private.key -out certificate.crt -subj "/C=PL/CN=roxdb.localhost/O=roxdb"
# create the secret TLS
kubectl create secret tls auth-tls --key private.key --cert certificate.crt
# create the deployment and the service
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

When using gRPC client remember to disable certificate validation (we used the self-signed certification) or when using
grpcurl use the `-insecure` flag.

Delete the deployment and service:

```bash
kubectl delete -f roxdb-deployment.yaml
```
