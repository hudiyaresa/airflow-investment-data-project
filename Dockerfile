FROM apache/airflow:2.9.2

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

USER root


RUN apt-get update && apt-get install -y \
    wget

RUN echo "airflow:airflow" | chpasswd && adduser airflow sudo

COPY start.sh /start.sh
RUN chmod +x /start.sh
USER airflow

ENTRYPOINT ["/bin/bash","/start.sh"]