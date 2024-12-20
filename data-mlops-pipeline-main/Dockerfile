# Use an appropriate base image
FROM python:3.11-slim

# Set up environment variables
ENV DAGSTER_HOME=/opt/dagster

# Install dependencies
RUN pip install dagster dagster-webserver dagster_duckdb 

# Set the working directory
WORKDIR /opt/dagster

# Copy your Dagster project code into the container
COPY . /opt/dagster

# Navigate to the project directory
WORKDIR /opt/dagster

# Copy requirements file
COPY requirements.txt /opt/dagster/tutorial/requirements.txt 

# Install dependencies from requirements file, excluding setfit and torch
RUN pip install -r requirements.txt 

# Navigate back to the root directory
WORKDIR /opt/dagster

# Expose ports if necessary (for Dagster UI)
EXPOSE 3000

# Command to start your Dagster server
CMD ["dagster","dev", "-h", "127.0.0.1", "-p", "4000"]
