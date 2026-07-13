set -euo pipefail

echo $GOOGLE_SERVICE_ACCOUNT | base64 -d > src/test/resources/.gcp-service-account.json

# Authenticate gcloud with the decoded service account
gcloud auth activate-service-account --key-file=src/test/resources/.gcp-service-account.json

PROJECT_ID="kestra-unit-test"
DATASET="kestra_unit_test"

gcloud config set project "${PROJECT_ID}" >/dev/null

echo "🧹 Deleting test tables from ${PROJECT_ID}:${DATASET}..."

# List of tables created in setup-unit.sh
TABLES=(
  "orderDetail"
  "territory"
)

for TABLE in "${TABLES[@]}"; do
  echo "🗑️  Deleting table ${TABLE}..."
  bq rm --project_id="${PROJECT_ID}" -f -t "${PROJECT_ID}:${DATASET}.${TABLE}" || echo "⚠️ Table ${TABLE} not found, skipping."
done

echo "✅ Test tables cleaned up successfully"
