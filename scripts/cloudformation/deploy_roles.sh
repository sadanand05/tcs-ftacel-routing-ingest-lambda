#!/bin/bash

echo "Deploying roles"

profile=foxtel

#profile=foxtel-dev

aws cloudformation deploy \
    --template-file cloudformation/LambdaRoles.yaml \
    --profile $profile \
    --region ap-southeast-2 \
    --stack-name dev-ftacel-ingest-roles \
    --parameter-overrides StageName=dev \
    --capabilities CAPABILITY_NAMED_IAM
