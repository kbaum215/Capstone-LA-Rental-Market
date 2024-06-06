# Base Image
FROM spark-base:latest
LABEL maintainer="MarcLamberti"

# Arguments that can be set with docker build
ARG AIRFLOW_VERSION=2.1.4
ARG AIRFLOW_HOME=/opt/airflow

# Export the environment variable AIRFLOW_HOME where airflow will be installed
ENV AIRFLOW_HOME=${AIRFLOW_HOME}

# Install dependencies
RUN apt-get update -y && \
    apt-get upgrade -yqq && \
    apt-get install -yqq --no-install-recommends \
    python3-dev wget libczmq-dev curl libssl-dev git \
    inetutils-telnet bind9utils freetds-dev libkrb5-dev \
    libsasl2-dev libffi-dev libpq-dev freetds-bin build-essential \
    default-libmysqlclient-dev apt-utils rsync zip unzip gcc vim netcat gnupg2 \
    chromium chromium-driver libglib2.0-0 libnss3 libgconf-2-4 libfontconfig1 && \
    apt-get autoremove -yqq --purge && apt-get clean

# Upgrade pip, create airflow user, install airflow and its dependencies
RUN pip install --upgrade "pip==20.2.4" && \
    useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow && \
    pip install apache-airflow==${AIRFLOW_VERSION} --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.1.4/constraints-3.7.txt && \
    pip install apache-airflow-providers-apache-hdfs==2.1.0 apache-airflow-providers-apache-hive==2.0.2 \
    apache-airflow-providers-apache-spark==2.0.1 apache-airflow-providers-slack==4.0.1 \
    apache-airflow-providers-http==2.0.1 selenium \
    pip install googlemaps \
    pip install haversine
    

# Set the owner of the files in AIRFLOW_HOME to the user airflow
RUN chown -R airflow: ${AIRFLOW_HOME}

# Copy the entrypoint.sh from host to container (at path AIRFLOW_HOME)
COPY ./start-airflow.sh ./start-airflow.sh

# Set the entrypoint.sh file to be executable
RUN chmod +x ./start-airflow.sh

# Set the username to use
USER airflow

# Create the folder dags inside $AIRFLOW_HOME
RUN mkdir -p ${AIRFLOW_HOME}/dags

# Expose ports (just to indicate that this container needs to map port)
EXPOSE 8080

# Execute start-airflow.sh
CMD [ "./start-airflow.sh" ]
