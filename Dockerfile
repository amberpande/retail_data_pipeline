# Use the official Airflow image as the base image
FROM apache/airflow:2.9.2

# Set environment variables to ensure that Python outputs everything to the console
ENV PYTHONUNBUFFERED=1

# Install system dependencies
USER root
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libpq-dev \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Download and install OpenJDK 22
RUN wget https://download.java.net/java/GA/jdk22/830ec9fcccef480bb3e73fb7ecafe059/36/GPL/openjdk-22_linux-aarch64_bin.tar.gz && \
    tar -xvf openjdk-22_linux-aarch64_bin.tar.gz && \
    mv jdk-22 /usr/local/ && \
    rm openjdk-22_linux-aarch64_bin.tar.gz

# Set JAVA_HOME environment variable
ENV JAVA_HOME="/usr/local/jdk-22"
ENV PATH="$JAVA_HOME/bin:$PATH"
ENV PYTHONPATH="/opt/airflow:/opt/airflow/scripts:/opt/airflow/config:${PYTHONPATH}"

# Verify Java installation
RUN java -version

# Copy the requirements file into the container
COPY requirements.txt /requirements.txt

# Switch back to airflow user before installing Python packages
USER airflow

# Install the dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Verify PySpark installation
RUN python -c "import pyspark; print(pyspark.__version__)"

# Copy the entire application folder into the container
COPY --chown=airflow:root . /opt/airflow/

# Set the default command to run Airflow webserver and scheduler
CMD ["sh", "-c", "airflow scheduler & airflow webserver"]