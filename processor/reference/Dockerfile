ARG PYTHON_IMAGE=python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c
FROM ${PYTHON_IMAGE}

ARG ZSTANDARD_VERSION=0.25.0

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/opt/taxi

RUN pip install --no-cache-dir "zstandard==${ZSTANDARD_VERSION}"

COPY taxi_reference /opt/taxi/taxi_reference

WORKDIR /workspace
ENTRYPOINT []
CMD ["python3", "-m", "taxi_reference.cli", "--help"]
