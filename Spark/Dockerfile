FROM apache/spark:3.5.0

USER root

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Установка системных утилит
RUN apt-get update && apt-get install -y \
    vim \
    git \
    tree \
    wget \
    htop \
    net-tools \
    iputils-ping \
    traceroute \
    curl \
    && rm -rf /var/lib/apt/lists/*


# Создание рабочих директорий
# Создаем и меняем права чтобы небыло ошибки при монтировании. Docker volumes монтируются от имени пользователя хоста, но Spark внутри контейнера работает от пользователя spark (UID 185).
RUN mkdir -p /opt/spark/work /opt/spark/logs /opt/spark/data /opt/spark/apps && \
    chown -R spark:spark /opt/spark/work /opt/spark/logs /opt/spark/data /opt/spark/apps


# Копирование конфигурации Spark
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark
WORKDIR /opt/spark

