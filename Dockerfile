FROM apache/airflow:2.10.2
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


USER airflow
COPY start.sh /start.sh
RUN chmod +x /start.sh
ENTRYPOINT ["/bin/bash","/start.sh"]