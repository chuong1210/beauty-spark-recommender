FROM python:3.11-bullseye
# RUN apt-get remove -y openjdk-21-jre-headless && apt-get autoremove -y

# Install Java (required for PySpark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


# Set Java home
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:${PATH}"
ENV SPARK_HOME="/opt/spark"
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV PATH="${JAVA_HOME}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
RUN mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}
RUN curl https://dlcdn.apache.org/spark/spark-4.1.0-preview2/spark-4.1.0-preview2-bin-hadoop3.tgz -o spark-4.1.0-preview2-bin-hadoop3.tgz \
    && tar xvzf spark-4.1.0-preview2-bin-hadoop3.tgz --directory ${SPARK_HOME} --strip-components 1 \
    && rm -rf spark-4.1.0-preview2-bin-hadoop3.tgz
# Set working directory
WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ /app/
COPY models/ /models/

# Expose port
EXPOSE 5000

# Run application
CMD ["python", "app.py"]