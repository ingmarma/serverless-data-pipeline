#!/bin/bash
# ============================================================
# Serverless Data Pipeline - Deployment Script
# Author: @ingmarma | MarmaTech
# ============================================================

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# Defaults
ENVIRONMENT="${1:-dev}"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
PROJECT_NAME="serverless-data-pipeline"
STACK_NAME="${PROJECT_NAME}-${ENVIRONMENT}"

echo -e "${CYAN}============================================================${NC}"
echo -e "${CYAN}  Serverless Data Pipeline - Deploy${NC}"
echo -e "${CYAN}  Environment: ${YELLOW}${ENVIRONMENT}${NC}"
echo -e "${CYAN}  Region:      ${YELLOW}${REGION}${NC}"
echo -e "${CYAN}============================================================${NC}"
echo ""

# ============================================================
# Step 1: Validate template
# ============================================================

echo -e "${YELLOW}[1/5]${NC} Validating CloudFormation template..."
aws cloudformation validate-template \
    --template-body file://infra/template.yaml \
    --region "${REGION}" > /dev/null
echo -e "${GREEN}  ✓ Template valid${NC}"

# ============================================================
# Step 2: Deploy stack
# ============================================================

echo -e "${YELLOW}[2/5]${NC} Deploying CloudFormation stack: ${STACK_NAME}..."
aws cloudformation deploy \
    --template-file infra/template.yaml \
    --stack-name "${STACK_NAME}" \
    --parameter-overrides \
        ProjectName="${PROJECT_NAME}" \
        Environment="${ENVIRONMENT}" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "${REGION}" \
    --tags \
        Project="${PROJECT_NAME}" \
        Environment="${ENVIRONMENT}" \
        ManagedBy=CloudFormation
echo -e "${GREEN}  ✓ Stack deployed${NC}"

# ============================================================
# Step 3: Get outputs
# ============================================================

echo -e "${YELLOW}[3/5]${NC} Reading stack outputs..."

get_output() {
    aws cloudformation describe-stacks \
        --stack-name "${STACK_NAME}" \
        --region "${REGION}" \
        --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" \
        --output text
}

RAW_BUCKET=$(get_output "RawBucketName")
PROCESSED_BUCKET=$(get_output "ProcessedBucketName")
SCRIPTS_BUCKET=$(get_output "ScriptsBucketName")
ETL_JOB_NAME=$(get_output "ETLJobName")

echo -e "  Raw bucket:       ${CYAN}${RAW_BUCKET}${NC}"
echo -e "  Processed bucket: ${CYAN}${PROCESSED_BUCKET}${NC}"
echo -e "  Scripts bucket:   ${CYAN}${SCRIPTS_BUCKET}${NC}"
echo -e "  ETL job:          ${CYAN}${ETL_JOB_NAME}${NC}"

# ============================================================
# Step 4: Upload Glue script
# ============================================================

echo -e "${YELLOW}[4/5]${NC} Uploading Glue ETL script..."
aws s3 cp glue/etl_job.py "s3://${SCRIPTS_BUCKET}/glue/etl_job.py" \
    --region "${REGION}"
echo -e "${GREEN}  ✓ Script uploaded${NC}"

# ============================================================
# Step 5: Summary
# ============================================================

echo ""
echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}  Deployment complete!${NC}"
echo -e "${GREEN}============================================================${NC}"
echo ""
echo -e "Next steps:"
echo -e "  1. Upload data:  ${CYAN}aws s3 cp sample-data/ s3://${RAW_BUCKET}/input/ --recursive${NC}"
echo -e "  2. Run crawler:  ${CYAN}aws glue start-crawler --name ${PROJECT_NAME}-raw-crawler-${ENVIRONMENT}${NC}"
echo -e "  3. Run ETL:      ${CYAN}aws glue start-job-run --job-name ${ETL_JOB_NAME}${NC}"
echo -e "  4. Query:        ${CYAN}Open Athena → workgroup: ${PROJECT_NAME}-${ENVIRONMENT}${NC}"
echo ""
echo -e "  ${YELLOW}Or run everything at once:${NC}"
echo -e "  ${CYAN}bash scripts/run_pipeline.sh ${ENVIRONMENT}${NC}"
