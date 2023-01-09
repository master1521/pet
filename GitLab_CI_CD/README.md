# GitLab CI/CD pipeline with Airflow

**Это НЕ инструкция по настройке!**
## Задача:
- Настроить CI/CD процесс добавления, проверки и деплоя дагов в GitLab 
- Подять сервер GitLab 
- Поднять сервер Airflow

### Схема хостов
![Alt-текст](img/a_shema1.png)

### Создаем docker-compose файл для GitLab
Перед запуском добавляем переменную из папки проекта
export GITLAB_HOME=$(pwd)

~~~
version: '3.6'
services:
  gitlab_web:
    image: 'gitlab/gitlab-ce:latest'
    hostname: 'gitlab.ivan.home'
    environment:
      GITLAB_HOME: '/home/ivan1/Desktop/GitLab1/'
      GITLAB_OMNIBUS_CONFIG: |
        external_url 'http://gitlab.ivan.home:8929'
        gitlab_rails['gitlab_shell_ssh_port'] = 2224
    ports:
      - '8929:8929'
      - '2224:22'
    volumes:
      - '/home/ivan1/Desktop/GitLab1/config:/etc/gitlab'
      - '/home/ivan1/Desktop/GitLab1/logs:/var/log/gitlab'
      - '/home/ivan1/Desktop/GitLab1/data:/var/opt/gitlab'
    shm_size: '256m'

  gitlab_runner:
    image: 'gitlab/gitlab-runner:latest'
    #restart: always
    volumes:
      - '/var/run/docker.sock:/var/run/docker.sock'
      - '/home/ivan1/Desktop/GitLab1/etc/gitlab-runner:/etc/gitlab-runner'
    depends_on:
      - 'gitlab_web'
    healthcheck:
      test: ["CMD", "gitlab-runner", "-v"]
      interval: 2s
      timeout: 3s
      retries: 10
~~~

![Alt-текст](img/a_docker1.png)


### Добавляем и настреваем GitLab Runner для группы
- Docker executer
- Shell executer

**Заметки:**  
1) При регистрации контейнер нужно поднимать в той же сети
~~~
docker run --rm --network gitlab1_default -it -v /home/ivan1/Desktop/GitLab1/etc/gitlab-runner:/etc/gitlab-runner gitlab/gitlab-runner register
~~~
2) Для docker executor добавляем network_mode = "gitlab1_default" (сеть) в GitLab1/etc/gitlab-runner/config.toml. Иначе из-за разных сетей executer не сможет клонировать репы
3) Добавляем домен gitlab.ivan.home в /etc/hosts c ip 192.168.0.104
4) Default Docker image: ubuntu:20.04

~~~
[[runners]]
  name = "Shell runner for test"
  url = "http://gitlab.ivan.home:8929/"
  id = 2
  token = $TOKEN_SHELL
  token_obtained_at = 2022-12-31T15:48:25Z
  token_expires_at = 0001-01-01T00:00:00Z
  executor = "shell"
  [runners.custom_build_dir]
  [runners.cache]
    MaxUploadedArchiveSize = 0
    [runners.cache.s3]
    [runners.cache.gcs]
    [runners.cache.azure]

[[runners]]
  name = "Docker runner for airflow group"
  url = "http://gitlab.ivan.home:8929/"
  id = 3
  token = $TOKEN_DOCKER
  token_obtained_at = 2023-01-04T06:50:23Z
  token_expires_at = 0001-01-01T00:00:00Z
  executor = "docker"
  [runners.custom_build_dir]
  [runners.cache]
    MaxUploadedArchiveSize = 0
    [runners.cache.s3]
    [runners.cache.gcs]
    [runners.cache.azure]
  [runners.docker]
    tls_verify = false
    image = "ubuntu:20.04"
    privileged = false
    disable_entrypoint_overwrite = false
    oom_kill_disable = false
    disable_cache = false
    volumes = ["/cache"]
    shm_size = 0
    network_mode = "gitlab1_default"
~~~

**На вкладке CI/CD проека или группы можно посмотреть доступные раннеры для запуска задач**  
![Alt-текст](img/a_runner1.png)


### Настройка .gitlab-ci.yml


**2 этапа в папйплайне:**  
- Тест
- Деплой

3 задачи для проверки на стадии тест:   
**flake8** - Проверяет синтаксис файлов *.py в папке с дагами   
**chek_file** - Проверяет есть ли файл с зависимостями   
**ping_airflow_host** - Проверяет доступин ли сервер для загрузки файлов  

1 задача для загрузки файлов на сервер   
**deploy** - Загружает папку с дагами на сервер

![Alt-текст](img/a_stage1.png)   
Логика такая:
- При коммите в main запускается 3 проверки и деплой папки с дагами
- При коммитах в других ветках всегда будут запускаться 3 проверки

При таком подходе если мы создадим feature/TI-999 ветку и добавим туда DAG
с ошибкой, тогда при коммите запустятся 3 проверки и пайплайн будет висеть с ошибкой,
а значит мы не сможем смержить feature/TI-999 в main пока там есть ошибки.  
Также если мы напрямую добавим DAG c ошибкой в main перед деплоем будут запускаться тесты проверок.

![Alt-текст](img/a_gitlab2.png)   

Если возникла проблема с развертыванием, можно повторить попытку или выполнить откат через **Deployments > Environments**
![Alt-текст](img/dev_airflow.png)   


### .gitlab-ci.yml
~~~
stages:
  - Test
  - Deploy

before_script:
  - chmod 600 $SSH_KEY

variables:
  APP_WORK_DIR: /home/app1/Desktop/Airflow1/

flake8:
  image: "python:3.8"
  stage: Test
  script:
    - python -V
    - pip install -r requirements.txt
    - flake8 --max-line-length=120 ./dags/*.py

chek_file:
  stage: Test
  rules:
    - exists:
      - requirements.txt
  script: echo "File wit requirements exists"

ping_airflow_host:
  stage: Test
  before_script:
    - apt update && apt -y upgrade && apt install -y iputils-ping iproute2 traceroute
  rules:
    - if: $CI_COMMIT_BRANCH == "main"
  script:
    - ip a
    - ip r
    - ping 192.168.0.103 -c 5
    - traceroute 192.168.0.103

deploy:
  stage: Deploy
  needs:
    - flake8
    - ping_airflow_host
  rules:
    - if: $CI_COMMIT_BRANCH == "main"
  script:
    - ls -l ./dags/*.py
    - apt update && apt -y upgrade
    - apt install -y openssh-client
    - scp -i $SSH_KEY -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -r ./dags app1@192.168.0.103:$APP_WORK_DIR
  environment:
    name: dev_airflow
    url: http://airflow.ivan.home:8080
  rules:
    - if: $CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH
      when: manual
~~~


## Используем стандартный docker-compose для Airflow из документации
~~~
version: '3'
x-airflow-common:
  &airflow-common
  image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.4.3}
  environment:
    &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: CeleryExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
    AIRFLOW__CELERY__BROKER_URL: redis://:@redis:6379/0
    AIRFLOW__CORE__FERNET_KEY: ''
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__API__AUTH_BACKENDS: 'airflow.api.auth.backend.basic_auth'
    _PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:-}
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
  user: "${AIRFLOW_UID:-50000}:0"
  depends_on:
    &airflow-common-depends-on
    redis:
      condition: service_healthy
    postgres:
      condition: service_healthy

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5
    restart: always

  redis:
    image: redis:latest
    expose:
      - 6379
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 30s
      retries: 50
    restart: always

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - 8080:8080
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 10s
      timeout: 10s
      retries: 5
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    healthcheck:
      test: ["CMD-SHELL", 'airflow jobs check --job-type SchedulerJob --hostname "$${HOSTNAME}"']
      interval: 10s
      timeout: 10s
      retries: 5
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully

  airflow-worker:
    <<: *airflow-common
    command: celery worker
    healthcheck:
      test:
        - "CMD-SHELL"
        - 'celery --app airflow.executors.celery_executor.app inspect ping -d "celery@$${HOSTNAME}"'
      interval: 10s
      timeout: 10s
      retries: 5
    environment:
      <<: *airflow-common-env
      DUMB_INIT_SETSID: "0"
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully

  airflow-triggerer:
    <<: *airflow-common
    command: triggerer
    healthcheck:
      test: ["CMD-SHELL", 'airflow jobs check --job-type TriggererJob --hostname "$${HOSTNAME}"']
      interval: 10s
      timeout: 10s
      retries: 5
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully

  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    # yamllint disable rule:line-length
    command:
      - -c
      - |
        function ver() {
          printf "%04d%04d%04d%04d" $${1//./ }
        }
        airflow_version=$$(AIRFLOW__LOGGING__LOGGING_LEVEL=INFO && gosu airflow airflow version)
        airflow_version_comparable=$$(ver $${airflow_version})
        min_airflow_version=2.2.0
        min_airflow_version_comparable=$$(ver $${min_airflow_version})
        if (( airflow_version_comparable < min_airflow_version_comparable )); then
          echo
          echo -e "\033[1;31mERROR!!!: Too old Airflow version $${airflow_version}!\e[0m"
          echo "The minimum Airflow version supported: $${min_airflow_version}. Only use this or higher!"
          echo
          exit 1
        fi
        if [[ -z "${AIRFLOW_UID}" ]]; then
          echo
          echo -e "\033[1;33mWARNING!!!: AIRFLOW_UID not set!\e[0m"
          echo "If you are on Linux, you SHOULD follow the instructions below to set "
          echo "AIRFLOW_UID environment variable, otherwise files will be owned by root."
          echo "For other operating systems you can get rid of the warning with manually created .env file:"
          echo "    See: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#setting-the-right-airflow-user"
          echo
        fi
        one_meg=1048576
        mem_available=$$(($$(getconf _PHYS_PAGES) * $$(getconf PAGE_SIZE) / one_meg))
        cpus_available=$$(grep -cE 'cpu[0-9]+' /proc/stat)
        disk_available=$$(df / | tail -1 | awk '{print $$4}')
        warning_resources="false"
        if (( mem_available < 4000 )) ; then
          echo
          echo -e "\033[1;33mWARNING!!!: Not enough memory available for Docker.\e[0m"
          echo "At least 4GB of memory required. You have $$(numfmt --to iec $$((mem_available * one_meg)))"
          echo
          warning_resources="true"
        fi
        if (( cpus_available < 2 )); then
          echo
          echo -e "\033[1;33mWARNING!!!: Not enough CPUS available for Docker.\e[0m"
          echo "At least 2 CPUs recommended. You have $${cpus_available}"
          echo
          warning_resources="true"
        fi
        if (( disk_available < one_meg * 10 )); then
          echo
          echo -e "\033[1;33mWARNING!!!: Not enough Disk space available for Docker.\e[0m"
          echo "At least 10 GBs recommended. You have $$(numfmt --to iec $$((disk_available * 1024 )))"
          echo
          warning_resources="true"
        fi
        if [[ $${warning_resources} == "true" ]]; then
          echo
          echo -e "\033[1;33mWARNING!!!: You have not enough resources to run Airflow (see above)!\e[0m"
          echo "Please follow the instructions to increase amount of resources available:"
          echo "   https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#before-you-begin"
          echo
        fi
        mkdir -p /sources/logs /sources/dags /sources/plugins
        chown -R "${AIRFLOW_UID}:0" /sources/{logs,dags,plugins}
        exec /entrypoint airflow version
    environment:
      <<: *airflow-common-env
      _AIRFLOW_DB_UPGRADE: 'true'
      _AIRFLOW_WWW_USER_CREATE: 'true'
      _AIRFLOW_WWW_USER_USERNAME: ${_AIRFLOW_WWW_USER_USERNAME:-airflow}
      _AIRFLOW_WWW_USER_PASSWORD: ${_AIRFLOW_WWW_USER_PASSWORD:-airflow}
      _PIP_ADDITIONAL_REQUIREMENTS: ''
    user: "0:0"
    volumes:
      - .:/sources

  airflow-cli:
    <<: *airflow-common
    profiles:
      - debug
    environment:
      <<: *airflow-common-env
      CONNECTION_CHECK_MAX_COUNT: "0"
    command:
      - bash
      - -c
      - airflow

  flower:
    <<: *airflow-common
    command: celery flower
    profiles:
      - flower
    ports:
      - 5555:5555
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:5555/"]
      interval: 10s
      timeout: 10s
      retries: 5
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully

volumes:
  postgres-db-volume:
~~~
![Alt-текст](img/a_docker2.png)   
