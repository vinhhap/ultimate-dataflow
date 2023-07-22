ARG SDK_BASE_IMAGE=apache/beam_python3.9_sdk:latest

FROM $SDK_BASE_IMAGE

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt