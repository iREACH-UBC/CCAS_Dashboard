#!/usr/bin/env bash
#
# Download a single object from Cloudflare R2 inside a GitHub-Actions runner.
#
# Required environment variables (all upper-case, AWS-style):
#   AWS_ACCESS_KEY_ID        – your R2 “Access key ID”
#   AWS_SECRET_ACCESS_KEY    – your R2 “Secret access key”
#   R2_ACCOUNT_ID            – Cloudflare account ID
#   R2_BUCKET                – bucket name (e.g. calibration-models)
#   R2_OBJ_KEY               – object key (e.g. 2021/Calibration_Models.obj)
# Optional:
#   DEST_PATH                – output directory (default: .)
#   AWS_DEFAULT_REGION       – any string; R2 ignores it but the CLI wants *something*
#
set -euo pipefail

: "${AWS_ACCESS_KEY_ID?Missing AWS_ACCESS_KEY_ID}"
: "${AWS_SECRET_ACCESS_KEY?Missing AWS_SECRET_ACCESS_KEY}"
: "${R2_ACCOUNT_ID?Missing R2_ACCOUNT_ID}"
: "${R2_BUCKET?Missing R2_BUCKET}"
: "${R2_OBJ_KEY?Missing R2_OBJ_KEY}"

DEST_PATH="${DEST_PATH:-.}"
ENDPOINT="https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

echo "⇩  Downloading s3://${R2_BUCKET}/${R2_OBJ_KEY}"
aws s3 cp "s3://${R2_BUCKET}/${R2_OBJ_KEY}" "${DEST_PATH}/" \
  --endpoint-url "${ENDPOINT}"
echo "✔  Saved to ${DEST_PATH}/$(basename "${R2_OBJ_KEY}")"
