#!/bin/bash

profile=foxtel
accountId=216535245878

#profile=foxtel-dev
#accountId=440678296209

echo Removing bucket
aws s3 rb --profile $profile --force s3://dev-ftacel-ingest-${accountId}

echo Removing lambda
aws cloudformation delete-stack --profile $profile --stack-name dev-ftacel-ingest-lambda
aws cloudformation wait stack-delete-complete --profile $profile --stack-name dev-ftacel-ingest-lambda

echo Removing roles
aws cloudformation delete-stack --profile $profile --stack-name dev-ftacel-ingest-roles
aws cloudformation wait stack-delete-complete --profile $profile --stack-name dev-ftacel-ingest-roles

echo Removing infrastructure
aws cloudformation delete-stack --profile $profile --stack-name dev-ftacel-ingest-infrastructure
aws cloudformation wait stack-delete-complete --profile $profile --stack-name dev-ftacel-ingest-infrastructure


