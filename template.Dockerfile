# Set arguments
ARG BASE_IMAGE_TAG=latest
ARG PYTHON_VERSION=python39

FROM gcr.io/dataflow-templates-base/${PYTHON_VERSION}-template-launcher-base:${TAG}

ARG WORKDIR=/dataflow/template
ARG BEAM_VERSION=2.49.0

# Set environments
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/main.py"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"

# Create & set working directory
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

# Install apache-beam and other dependencies to launch the pipeline
COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE && \
    pip download --no-cache-dir --dest /tmp/dataflow-requirements-cache -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE
    
# Copy setup file and source code
COPY ./setup.py .
ADD ./src/ .
RUN python setup.py install

ENV PIP_NO_DEPS=True

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]
