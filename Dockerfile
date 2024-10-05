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

    
# Download Hadoop AWS and AWS SDK JARs
RUN mkdir -p /opt/spark/jars/ && \
    curl -o /opt/spark/jars/hadoop-aws-3.3.1.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar && \
    curl -o /opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar

# Install the PostgreSQL JDBC driver
RUN curl -o /opt/spark/jars/postgresql-42.2.23.jar \
    https://jdbc.postgresql.org/download/postgresql-42.2.23.jar


COPY start.sh /start.sh
RUN chmod +x /start.sh

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_PACKAGES=org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.900,org.apache.hadoop:hadoop-common:3.3.1
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$JAVA_HOME/bin

# Create Hadoop directory with correct permissions
USER root
RUN mkdir -p $HADOOP_HOME/etc/hadoop && chown -R airflow:root $HADOOP_HOME && chmod -R 775 $HADOOP_HOME
USER airflow

# Set Spark classpath to include the downloaded JARs
ENV SPARK_CLASSPATH=/opt/spark/jars/*

ENTRYPOINT ["/bin/bash","/start.sh"]