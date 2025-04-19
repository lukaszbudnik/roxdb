# RoxDB Kubernetes deployment

Using minikube:

```bash
# start minikube
minikube start
# load local roxdb image into minikube
minikube image load roxdb
# start the dashboard
minikube dashboard
```

Create RoxDB deployment and service:

```bash
# change to kubernetes directory
cd kubernetes
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

Delete the deployment and service:

```bash
kubectl delete -f roxdb-deployment.yaml
```
