FROM mirror.gcr.io/apache/airflow:2.9.1-python3.11

USER root
RUN apt-get update
RUN apt-get install -y git unixodbc htop vim tree


USER airflow
COPY airflow_requirements.txt .
RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --upgrade pip
RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org -r airflow_requirements.txt
ENV PYTHONPATH=/opt/airflow/associated 
ENV PATH="${PATH}:/usr/local/bin"

