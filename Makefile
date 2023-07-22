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
	ORACLE_SID=${ORACLE_SID} ORACLE_PDB=${ORACLE_PDB} ORACLE_PWD=${ORACLE_PWD} \
		docker compose -f dev_environment/oracle/docker-compose.yaml up -d

stop-oracle:
	docker compose -f dev_environment/oracle/docker-compose.yaml down

# Postgres database related
start-postgres:
	POSTGRES_PASSWORD=${POSTGRES_PASSWORD} POSTGRES_USER=${POSTGRES_USER} POSTGRES_DB=${POSTGRES_DB} \
		docker compose -f dev_environment/postgres/docker-compose.yaml up -d

stop-postgres:
	docker compose -f dev_environment/postgres/docker-compose.yaml down

# MySQL database related
start-mysql:
	MYSQL_ROOT_PASSWORD=${MYSQL_ROOT_PASSWORD} MYSQL_DATABASE=${MYSQL_DATABASE} MYSQL_USER=${MYSQL_USER} MYSQL_PASSWORD=${MYSQL_PASSWORD} \
		docker compose -f dev_environment/mysql/docker-compose.yaml up -d

stop-mysql:
	docker compose -f dev_environment/mysql/docker-compose.yaml down

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
		-d '{"launch_parameter": { "jobName": "data-integration-$(shell uuidgen)", "containerSpecGcsPath": "${OUTPUT_TEMPLATE_PATH}", "environment": { "sdkContainerImage": "${OUTPUT_SDK_IMAGE}", "zone": "${ZONE}", "tempLocation": "${TEMP_LOCATION}", "stagingLocation": "${STAGING_LOCATION}", "machineType": "${MACHINE_TYPE}" }, "parameters": { "run_param": $(shell jq @json -c param.json) } } }'

# Build template
build-template:
	gcloud auth configure-docker asia-southeast1-docker.pkg.dev
	gcloud builds submit --config build_template.cloudbuild.yaml \
		--substitutions _TEMPLATE_BASE_IMAGE=${TEMPLATE_BASE_IMAGE},_OUTPUT_TEMPLATE_IMAGE=${OUTPUT_TEMPLATE_IMAGE}
	gcloud dataflow flex-template build ${OUTPUT_TEMPLATE_PATH} \
     --image ${OUTPUT_TEMPLATE_IMAGE} \
     --sdk-language "PYTHON" \
     --metadata-file "metadata.json"

build-sdk:
	gcloud auth configure-docker asia-southeast1-docker.pkg.dev
	gcloud builds submit --config build_sdk.cloudbuild.yaml \
		--substitutions _OUTPUT_SDK_IMAGE=${OUTPUT_SDK_IMAGE},_SDK_BASE_IMAGE=${SDK_BASE_IMAGE}