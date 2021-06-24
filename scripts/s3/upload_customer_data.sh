#!/bin/bash

set -e

# accountId=216535245878
# kmsId=cfc0cde1-4eb5-48a3-8bb8-fee164c2f9b3
# profile=foxtel

accountId=440678296209
kmsId=fba41f43-bbb8-432d-b103-5ad639a74b0e
profile=foxtel-dev

aws s3 cp \
    --profile $profile \
    --sse aws:kms \
    --sse-kms-key-id ${kmsId} \
    ./data/customer-001.csv.gpg \
    s3://dev-ftacel-ingest-${accountId}/ingest/
