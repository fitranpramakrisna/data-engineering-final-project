FROM apache/airflow:2.8.2-python3.9
USER root

# Install OpenJDK-17
RUN apt-get update \
    && apt-get install -y openjdk-17-jdk \
    && apt-get install -y ant  \
    && apt-get install -y procps \
    && apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME

USER airflow

RUN pip install \
    lxml \
    requests==2.31 \
    pandas==1.2.4 \
    beautifulsoup4==4.13.3 \
    apache-airflow-providers-google==10.13.1 \
    scikit-learn==1.6.1

COPY /dags /opt/airflow/dags
COPY resources /opt/airflow/resources

