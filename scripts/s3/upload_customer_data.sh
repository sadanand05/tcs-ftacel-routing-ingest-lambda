#!/bin/bash

accountId=216535245878
kmsId=cfc0cde1-4eb5-48a3-8bb8-fee164c2f9b3
profile=foxtel

#accountId=440678296209
#kmsId=b7b54d98-2e1f-4bce-9f71-af528fe33c15
#profile=foxtel-dev

aws s3 cp \
    --profile $profile \
    --sse aws:kms \
    --sse-kms-key-id ${kmsId} \
    ./data/customer-001.csv.gpg \
    s3://dev-ftacel-ingest-${accountId}/ingest/
