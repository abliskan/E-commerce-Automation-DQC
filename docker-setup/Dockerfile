# Base image
FROM apache/airflow:3.1.0-python3.13

# Set working directory
# WORKDIR /app

# Define environment variables
# ENV AIRFLOW_HOME=/home/airflow

# Switch to root user
# USER root

# Enable passwordless sudo for airflow user
# RUN echo "airflow ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

# Install necessary packages
# RUN apt-get update -qq && apt-get install -y vim wget libgomp1

# Copy requirements file
# COPY requirements.txt .

# Switch to airflow user
USER airflow

# Copy requirements file
COPY requirements.txt /opt/airflow/requirements.txt

# Install python dependency
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

USER airflow

# Expose necessary ports
EXPOSE 9091 9000 8000