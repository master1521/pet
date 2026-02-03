# Airflow DE Minikube 
Тестовый стенд для локальной проверки образов, пакетов и дагов Airflow

### Установка 
Устанавливаем minikube, helm, kubectl на пк по инструкциям из интернета


### Проверка установки
~~~
minikube version
helm version
kubectl version
~~~



Собрать и загрузить образ в докер хаб 



### Запустить кластер
~~~
minikube start 
~~~

### Проверить контекст
~~~
kubectl config get-contexts
~~~

### Создать namespace
~~~
kubectl create namespace airflow-test
kubectl get ns
~~~



Собрать и загрузить образ в докер хаб 


# Создать секрет для скачики образа
kubectl create secret docker-registry docker-hub-creds \
  --docker-server=index.docker.io/v1/ \
  --docker-username='XXXXXXXXXXXX' \
  --docker-password='XXXXXXXXXXXX' \
  -n airflow-test
kubectl get secret docker-hub-creds -n airflow-test

kubectl create secret generic git-credentials \
  --namespace airflow-test \
  --from-file=gitSshKey=/Users/master1521/.ssh/airflow_gitSyncMac
kubectl get secret git-credentials -n airflow-test

Проверить секрет
kubectl get secret docker-hub-creds -n airflow-test 
-o jsonpath='{.data.\.dockerconfigjson}' | base64 -d



### Установка из локальной директории чарта (airflow)
~~~
helm install airflow-dev . \
  --namespace airflow-test \
  --values custom-values.yaml
~~~

### Проверка работы кластера
~~~
kubectl get all -n airflow-test
~~~

### UI airflow
LENS >> Network >> Services >> airflow-dev-webserver >> Кнопка Forward   

~~~
Логин и пароль     
airflow
airflow
~~~
или
~~~
kubectl port-forward -n airflow-test \
  $(kubectl get pod -n airflow-test -l component=webserver -o jsonpath='{.items[0].metadata.name}') \
  8080:8080
~~~
http://localhost:8080   





### Остановить кластер
~~~
minikube stop
~~~

### Удаление 
1)Самый простой способ удалить через LENS неймспейс airflow-test      

2)Удалить чарт
~~~
helm uninstall airflow-dev -n airflow-test    
~~~

3)В Самых крайних случаях (Осторожно проверяйте контекст перед удалением !)
~~~
kubectl delete all --all -n airflow-test --grace-period=0 --force
kubectl delete pvc --all -n airflow-test --grace-period=0 --force
kubectl delete pv --all -n airflow-test --grace-period=0 --force
kubectl delete job --all -n airflow-test --grace-period=0 --force
kubectl delete namespace airflow-test
kubectl create namespace airflow-test
kubectl get all -n airflow-test -o wide
~~~

# УСТАНОВКА ARGOCD
Создать неймспайс   
~~~
kubectl create namespace airflow-test
kubectl create namespace argocd
~~~

Установка ресурсов argocd  
~~~
kubectl apply -n argocd -f argo-install.yaml
~~~



Создать Application
Применить манифест Application
~~~
kubectl apply -f argocd-application.yaml
~~~



Проверяем после установки   
~~~
kubectl get all -n argocd -o wide
~~~


Делаем проброс портов для UI или через LENS
~~~
kubectl port-forward svc/argocd-server -n argocd 8080:443
~~~

Получи пароль UI Argo
Логин: admin
Пароль: можно посмотреть в argocd-initial-admin-secret или через команду
~~~
kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d && echo
~~~





Проверить запуск сервисов   
~~~
kubectl get all -n argocd -o wide
~~~



Удалить все в неймспейсе  
~~~
kubectl delete all --all -n argocd
kubectl delete all,configmap,secret,ingress,pvc,serviceaccount,role,rolebinding --all -n argocd
kubectl get all -n argocd -o wide
kubectl delete namespace argocd
kubectl get namespace argocd -o json | \
  jq '.spec.finalizers = []' | \
  kubectl replace --raw "/api/v1/namespaces/argocd/finalize" -f -

kubectl patch application airflow -n argocd -p '{"metadata":{"finalizers":[]}}' --type=merge
kubectl patch application airflow -n argocd -p '{"metadata":{"finalizers":null}}' --type=merge
kubectl delete application airflow -n argocd --force --grace-period=0
kubectl get application -n argocd


kubectl create namespace argocd
~~~