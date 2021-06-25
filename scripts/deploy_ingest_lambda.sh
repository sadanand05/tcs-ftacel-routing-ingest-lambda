#!/bin/bash

echo "Deploying ingest lambda"

mvn clean package
# Local dev
accountId=216535245878
profile=foxtel
deploymentBucket=dev-foxtel-deployment-${accountId}

#Foxtel dev
#accountId=440678296209
#profile=foxtel-dev
#deploymentBucket=dev-ftacel-deployment-${accountId}

aws cloudformation package \
    --template-file cloudformation/IngestLambda.yaml \
    --output-template-file cloudformation/IngestLambda-prepared.yaml \
    --profile $profile \
    --s3-bucket $deploymentBucket \
    --s3-prefix applications/ftacel/ingest \
    --region ap-southeast-2 \
    --force-upload

aws cloudformation deploy \
    --template-file cloudformation/IngestLambda-prepared.yaml \
    --profile $profile \
    --region ap-southeast-2 \
    --stack-name dev-ftacel-ingest-lambda \
    --parameter-overrides StageName=dev \
        ThreadCount=5
