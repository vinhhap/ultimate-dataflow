ARG TEMPLATE_BASE_IMAGE=gcr.io/dataflow-templates-base/python39-template-launcher-base:latest

FROM $TEMPLATE_BASE_IMAGE

# Set environments
ENV PYTHONPATH=${WORKDIR}

# Set working directory
ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

# Install dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --user -r ${WORKDIR}/requirements.txt

# Copy setup file and source code
COPY ./setup.py .
ADD ./src/ .
RUN python setup.py install

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/main.py"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"
ENV PIP_NO_DEPS=True