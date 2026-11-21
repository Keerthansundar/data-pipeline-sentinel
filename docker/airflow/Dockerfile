# Custom Airflow image with OpenJDK 21, PySpark 3.5.4, and Delta Lake 3.3.0
FROM apache/airflow:3.4.0

USER root

# Install OpenJDK 21 headless and system utilities
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-21-jre-headless \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Configure JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Copy airflow-specific requirements and install
COPY docker/airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
