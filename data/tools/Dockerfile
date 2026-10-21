ARG GO_IMAGE=golang:1.25.6-bookworm@sha256:f4490d7b261d73af4543c46ac6597d7d101b6e1755bcdd8c5159fda7046b6b3e
ARG PYTHON_IMAGE=python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c
FROM ${GO_IMAGE} AS go-builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
RUN go test ./...
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/taxi-data ./cmd/taxi-data

FROM ${PYTHON_IMAGE}

ARG DUCKDB_VERSION=1.4.3
ARG ZSTANDARD_VERSION=0.25.0

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/opt/taxi

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates p7zip-full \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir \
        "duckdb==${DUCKDB_VERSION}" \
        "zstandard==${ZSTANDARD_VERSION}"

COPY --from=go-builder /out/taxi-data /usr/local/bin/taxi-data
COPY reconstruction /opt/taxi/reconstruction

WORKDIR /workspace
ENTRYPOINT []
CMD ["python3", "-m", "reconstruction.cli", "--help"]
