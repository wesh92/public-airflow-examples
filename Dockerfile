# STAGE 1: Builder
# This stage's only job is to create the requirements.txt file.
FROM python:3.12-slim as builder

# Install uv, our package manager
RUN pip install uv

# Create a working directory
WORKDIR /build

# Copy only the pyproject.toml file. This helps with Docker layer caching.
# This step will only be re-run if pyproject.toml changes.
COPY pyproject.toml .

# Generate the requirements.txt file from pyproject.toml
RUN uv pip freeze > requirements.txt


# STAGE 2: Final Airflow Image
# This is the image that will actually be used by Kubernetes.
FROM apache/airflow:3.0.2-python3.12

# Switch to the root user to copy from the previous stage
USER root

# Copy the requirements.txt file generated in the 'builder' stage.
COPY --from=builder /build/requirements.txt /

USER airflow

# Install the Python packages from the generated requirements file.
RUN pip install --no-cache-dir -r /requirements.txt
