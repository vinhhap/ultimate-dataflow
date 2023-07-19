include .env
export

# Create Python environment
create-env:
	pip install -U -r local-requirements.txt
	pip install -U -r requirements.txt

# Create or delete secret on Google Cloud Secret Manager
create-secret:
	gcloud secrets create $(secret_name) --data-file=$(secret_file)

delete-secret:
	gcloud secrets delete $(secret_name)

# Oracle database related
start-oracle:
	ORACLE_SID=${ORACLE_SID} ORACLE_PDB=${ORACLE_PDB} ORACLE_PWD=${ORACLE_PWD} docker compose -f dev_environment/oracle/docker-compose.yaml up -d

stop-oracle:
	docker compose -f dev_environment/oracle/docker-compose.yaml down

# Run pipeline
run-direct:
	python ./src/main.py \
		--runner=DirectRunner \
		--staging_location=${STAGING_LOCATION} \
		--temp_location=${TEMP_LOCATION} \
		--run_param='$(shell jq -c . ./param.json)'

run-dataflow:
	curl -X POST \
		"https://dataflow.googleapis.com/v1b3/projects/${PROJECT_ID}/locations/${REGION}/flexTemplates:launch" \
		-H "Content-Type: application/json" \
		-H "Authorization: Bearer $(shell gcloud auth print-access-token)" \
		-d '{"launch_parameter": { "jobName": "data-integration-$(shell uuidgen)", "containerSpecGcsPath": "${OUTPUT_TEMPLATE_PATH}", "environment": { "zone": "${ZONE}", "tempLocation": "${TEMP_LOCATION}", "stagingLocation": "${STAGING_LOCATION}", "machineType": "${MACHINE_TYPE}" }, "parameters": { "run_param": $(shell jq @json -c param.json) } } }'

# Build template
build-template:
	gcloud auth configure-docker asia-southeast1-docker.pkg.dev
	gcloud builds submit --config build_template.cloudbuild.yaml \
		--substitutions _PYTHON_VERSION=${PYTHON_VERSION},_BASE_IMAGE_TAG=${BASE_IMAGE_TAG},_BEAM_VERSION=${BEAM_VERSION},_OUTPUT_IMAGE=${OUTPUT_IMAGE}
	gcloud dataflow flex-template build ${OUTPUT_TEMPLATE_PATH} \
     --image ${OUTPUT_IMAGE} \
     --sdk-language "PYTHON" \
     --metadata-file "metadata.json"