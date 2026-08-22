#!/bin/bash

TOOLS_DIR="$(pwd)/tools"
PROTOC_ZIP="${TOOLS_DIR}/protoc.zip"
PROTOC_DIR="${TOOLS_DIR}/protobuf"
EXPECTED_VERSION="28.2"
PROTOC_BIN="${PROTOC_DIR}/bin/protoc"

# Check if we already have the right version
needs_download=false
if [[ -x "${PROTOC_BIN}" ]]; then
    ACTUAL_VERSION=$("${PROTOC_BIN}" --version 2>&1 | grep -o '[0-9]\+\.[0-9]\+')
    if [[ "${ACTUAL_VERSION}" != "${EXPECTED_VERSION}" ]]; then
        echo "Found protoc ${ACTUAL_VERSION}, expected ${EXPECTED_VERSION}. Redownloading..."
        needs_download=true
    else
        echo "Found protoc ${ACTUAL_VERSION} ✓"
        needs_download=false
    fi
else
    needs_download=true
fi

if [[ "${needs_download}" = true ]]; then
    echo "Downloading protoc ${EXPECTED_VERSION}..."
    mkdir -p "${PROTOC_DIR}"
    curl -L "https://github.com/protocolbuffers/protobuf/releases/download/v${EXPECTED_VERSION}/protoc-${EXPECTED_VERSION}-linux-x86_64.zip" -o "${PROTOC_ZIP}"
    unzip -q "${PROTOC_ZIP}" -d "${PROTOC_DIR}"
fi

# Compile proto files
echo "Compiling proto files..."
"${PROTOC_BIN}" -I=statefun_tasks/core/statefun --python_out=statefun_tasks/core/statefun kafka-egress.proto
"${PROTOC_BIN}" -I=statefun_tasks/core/statefun --python_out=statefun_tasks/core/statefun kinesis-egress.proto
"${PROTOC_BIN}" -I=statefun_tasks/core/statefun --python_out=statefun_tasks/core/statefun types.proto
"${PROTOC_BIN}" -I=statefun_tasks/core/statefun --python_out=statefun_tasks/core/statefun request-reply.proto
"${PROTOC_BIN}" -I=statefun_tasks --python_out=statefun_tasks messages.proto
"${PROTOC_BIN}" -I=tests --python_out=tests test_messages.proto
echo "Compiling proto files...done ✓"