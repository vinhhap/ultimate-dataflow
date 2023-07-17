# Set arguments
ARG BASE_IMAGE_TAG=latest
ARG PYTHON_VERSION=python39
ARG WORKDIR=/dataflow/template
ARG BEAM_VERSION=2.48.0

FROM gcr.io/dataflow-templates-base/${PYTHON_VERSION}-template-launcher-base:${TAG}

# Set environments
ENV PYTHONPATH=${WORKDIR}
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/main.py"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"
ENV PIP_NO_DEPS=True

# Create & set working directory
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

# Install apache-beam and other dependencies to launch the pipeline
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install apache-beam[gcp]==${BEAM_VERSION} && \
    pip install -U -r ${WORKDIR}/requirements.txt

# Copy setup file and source code
COPY ./setup.py .
ADD ./src/ .
RUN python setup.py install