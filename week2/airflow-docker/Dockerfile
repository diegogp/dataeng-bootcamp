
FROM apache/airflow

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq && apt-get install vim -qqq
# git gcc g++ -qqq
USER $AIRFLOW_UID
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
WORKDIR $AIRFLOW_HOME
