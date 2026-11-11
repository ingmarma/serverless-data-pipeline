#!/bin/bash
# ============================================================
# Run the full pipeline: upload → crawl → ETL → verify
# Author: @ingmarma | MarmaTech
# ============================================================

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

ENVIRONMENT="${1:-dev}"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
PROJECT_NAME="serverless-data-pipeline"
STACK_NAME="${PROJECT_NAME}-${ENVIRONMENT}"

get_output() {
    aws cloudformation describe-stacks \
        --stack-name "${STACK_NAME}" \
        --region "${REGION}" \
        --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" \
        --output text
}

RAW_BUCKET=$(get_output "RawBucketName")
ETL_JOB_NAME=$(get_output "ETLJobName")
CRAWLER_NAME="${PROJECT_NAME}-processed-crawler-${ENVIRONMENT}"
WORKGROUP="${PROJECT_NAME}-${ENVIRONMENT}"

echo -e "${CYAN}============================================================${NC}"
echo -e "${CYAN}  Running Full Pipeline${NC}"
echo -e "${CYAN}============================================================${NC}"
echo ""

# ============================================================
# Step 1: Generate sample data (if not exists)
# ============================================================

if [ ! -d "sample-data" ] || [ -z "$(ls -A sample-data/ 2>/dev/null)" ]; then
    echo -e "${YELLOW}[1/5]${NC} Generating sample data (1M records)..."
    python3 scripts/generate_sample_data.py --records 1000000 --output ./sample-data/
else
    echo -e "${YELLOW}[1/5]${NC} Sample data already exists, skipping generation"
fi

# ============================================================
# Step 2: Upload to S3
# ============================================================

echo -e "${YELLOW}[2/5]${NC} Uploading data to S3..."
aws s3 sync sample-data/ "s3://${RAW_BUCKET}/input/" --region "${REGION}"
echo -e "${GREEN}  ✓ Data uploaded${NC}"

# ============================================================
# Step 3: Run ETL Job
# ============================================================

echo -e "${YELLOW}[3/5]${NC} Starting Glue ETL job..."
JOB_RUN_ID=$(aws glue start-job-run \
    --job-name "${ETL_JOB_NAME}" \
    --region "${REGION}" \
    --query 'JobRunId' \
    --output text)
echo -e "  Job run ID: ${CYAN}${JOB_RUN_ID}${NC}"

echo -e "  Waiting for job to complete..."
while true; do
    STATUS=$(aws glue get-job-run \
        --job-name "${ETL_JOB_NAME}" \
        --run-id "${JOB_RUN_ID}" \
        --region "${REGION}" \
        --query 'JobRun.JobRunState' \
        --output text)

    if [ "${STATUS}" = "SUCCEEDED" ]; then
        ELAPSED=$(aws glue get-job-run \
            --job-name "${ETL_JOB_NAME}" \
            --run-id "${JOB_RUN_ID}" \
            --region "${REGION}" \
            --query 'JobRun.ExecutionTime' \
            --output text)
        echo -e "${GREEN}  ✓ ETL completed in ${ELAPSED}s${NC}"
        break
    elif [ "${STATUS}" = "FAILED" ] || [ "${STATUS}" = "ERROR" ] || [ "${STATUS}" = "TIMEOUT" ]; then
        echo -e "${RED}  ✗ ETL job ${STATUS}${NC}"
        aws glue get-job-run \
            --job-name "${ETL_JOB_NAME}" \
            --run-id "${JOB_RUN_ID}" \
            --region "${REGION}" \
            --query 'JobRun.ErrorMessage' \
            --output text
        exit 1
    fi

    echo -e "  Status: ${YELLOW}${STATUS}${NC} - waiting 30s..."
    sleep 30
done

# ============================================================
# Step 4: Run processed data crawler
# ============================================================

echo -e "${YELLOW}[4/5]${NC} Running crawler on processed data..."
aws glue start-crawler --name "${CRAWLER_NAME}" --region "${REGION}"

while true; do
    CSTATUS=$(aws glue get-crawler \
        --name "${CRAWLER_NAME}" \
        --region "${REGION}" \
        --query 'Crawler.State' \
        --output text)

    if [ "${CSTATUS}" = "READY" ]; then
        echo -e "${GREEN}  ✓ Crawler complete${NC}"
        break
    fi

    echo -e "  Crawler: ${YELLOW}${CSTATUS}${NC} - waiting 15s..."
    sleep 15
done

# ============================================================
# Step 5: Verify with a sample query
# ============================================================

echo -e "${YELLOW}[5/5]${NC} Running verification query..."
QUERY_EXEC_ID=$(aws athena start-query-execution \
    --query-string "SELECT COUNT(*) as total_records, ROUND(SUM(amount), 2) as total_amount FROM transactions" \
    --work-group "${WORKGROUP}" \
    --region "${REGION}" \
    --query 'QueryExecutionId' \
    --output text)

while true; do
    QSTATUS=$(aws athena get-query-execution \
        --query-execution-id "${QUERY_EXEC_ID}" \
        --region "${REGION}" \
        --query 'QueryExecution.Status.State' \
        --output text)

    if [ "${QSTATUS}" = "SUCCEEDED" ]; then
        echo -e "${GREEN}  ✓ Query complete${NC}"
        echo ""
        aws athena get-query-results \
            --query-execution-id "${QUERY_EXEC_ID}" \
            --region "${REGION}" \
            --output table
        break
    elif [ "${QSTATUS}" = "FAILED" ]; then
        echo -e "${RED}  ✗ Query failed${NC}"
        exit 1
    fi

    sleep 5
done

echo ""
echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}  Pipeline complete! Data is ready to query in Athena.${NC}"
echo -e "${GREEN}============================================================${NC}"
echo ""
echo -e "  Open Athena console → workgroup: ${CYAN}${WORKGROUP}${NC}"
echo -e "  Try the saved queries: total_amount_by_month, top_categories, daily_trend"
