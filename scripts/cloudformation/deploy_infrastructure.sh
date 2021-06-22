#!/bin/bash

echo "Deploying infrastructure"

profile=foxtel

#profile=foxtel-dev


aws cloudformation deploy \
    --template-file cloudformation/Infrastructure.yaml \
    --profile $profile \
    --region ap-southeast-2 \
    --stack-name dev-ftacel-ingest-infrastructure \
    --parameter-overrides StageName=dev
