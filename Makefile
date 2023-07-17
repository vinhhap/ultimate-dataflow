PYTHON_VERSION=python39
BASE_IMAGE_TAG=latest
BEAM_VERSION=2.48.0
OUTPUT_IMAGE=asia-southeast1-docker.pkg.dev/quan-nguyen-data-learning/dataflow/ultimate-dataflow:latest
OUTPUT_TEMPLATE_PATH=gs://ultimate-dataflow-template/templates/ultimate-dataflow-template.json

create-env:
	pip install -U -r local-requirements.txt
	pip install -U -r requirements.txt

build-image:
	gcloud auth configure-docker asia-southeast1-docker.pkg.dev
	gcloud builds submit --config build_template.cloudbuild.yaml \
		--substitutions _PYTHON_VERSION=${PYTHON_VERSION},_BASE_IMAGE_TAG=${BASE_IMAGE_TAG},_BEAM_VERSION=${BEAM_VERSION},_OUTPUT_IMAGE=${OUTPUT_IMAGE}

build-template:
	gcloud auth configure-docker asia-southeast1-docker.pkg.dev
	gcloud dataflow flex-template build ${OUTPUT_TEMPLATE_PATH} \
     --image ${OUTPUT_IMAGE} \
     --sdk-language "PYTHON" \
     --metadata-file "metadata.json"