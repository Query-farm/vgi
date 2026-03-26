#!/bin/sh
export OTEL_SERVICE_NAME=vgi-python-worker
export OTEL_EXPORTER_OTLP_ENDPOINT=https://in-otel.hyperdx.io
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_EXPORTER_OTLP_HEADERS="Authorization=ee058169-beaf-4820-9dd0-bf20b9f59c14"
export OTEL_EXPORTER_OTLP_COMPRESSION=gzip
