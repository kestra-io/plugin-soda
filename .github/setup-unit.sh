set -euo pipefail

echo $GOOGLE_SERVICE_ACCOUNT | base64 -d > src/test/resources/.gcp-service-account.json

# Authenticate gcloud with the decoded service account
gcloud auth activate-service-account --key-file=src/test/resources/.gcp-service-account.json

PROJECT_ID="kestra-unit-test"
DATASET="kestra_unit_test" # already created via terraform

gcloud config set project "${PROJECT_ID}" >/dev/null

echo "📋 Creating test tables in existing dataset ${PROJECT_ID}:${DATASET}..."

# orderDetail table: used in 'run' and 'error' tests
bq query --use_legacy_sql=false "
CREATE OR REPLACE TABLE \`${PROJECT_ID}.${DATASET}.orderDetail\` AS
SELECT 1 AS id, 10.5 AS unitPrice, TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) AS addedDate
UNION ALL
SELECT 2 AS id, 200.0 AS unitPrice, TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY) AS addedDate;
"

# territory table: used in 'failed' test
bq query --use_legacy_sql=false "
CREATE OR REPLACE TABLE \`${PROJECT_ID}.${DATASET}.territory\` AS
SELECT 1 AS id, 'North' AS name, 1 AS regionId
UNION ALL
SELECT 2 AS id, 'South' AS name, 4 AS regionId;
"

echo "✅ Dataset ${PROJECT_ID}:${DATASET} ready for tests"
