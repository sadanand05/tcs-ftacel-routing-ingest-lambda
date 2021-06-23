# Foxtel customer data ingest

This project aims to provide the AWS infrastructure to securely ingest Foxtel 
customer data and make this data available to Amazon Connect for dynamic customer routing.

## Building

The system relies on the Java JDK 1.8 and Apache Maven for the build process, these must be installed on the build machine. 

## Deploying

The templates rely on a StageName parameter which can be set to dev, test or prod, this determines the naming for the assets deployed. 

The system relies on the Infrastructure and LambdaRoles templates and these must be deployed prior. 

It uses the AWS::Serverless-2016-10-31 transform and relies on the system being built with maven:

    mvn package

and packaged into S3:

    aws cloudformation package \
        --template-file cloudformation/IngestLambda.yaml \
        --output-template-file cloudformation/IngestLambda-prepared.yaml \
        --s3-bucket <deploymentBucket> \
        --s3-prefix applications/ftacel/ingest/lambda \
        --region ap-southeast-2 \
        --force-upload

And finally deployed with CloudFormation:

    aws cloudformation deploy \
        --template-file cloudformation/IngestLambda-prepared.yaml \
        --region ap-southeast-2 \
        --stack-name dev-ftacel-ingest-lambda \
        --parameter-overrides StageName=dev ThreadCount=5
    
 Sample deployment scripts are provided (remove the --profile references when deploying via Code Pipeline)
 
## Post deployment

A GPG public key is required to encrypt the data on the Foxtel side and the matching private key is 
required on teh AWS Lambda side to decrypt the incoming ingest data.

### Generate a GPG key pair

Generate a new GPG key using the following:

    gpg --gen-key
    
You will be prompted for a name, an email address (identity: <stage>-ftacel@foxtel.com.au) and a secure 
passphrase to protect the private key.

### Export the public key

    gpg --output <keyname>.gpg --export <identity>

### Export the secret key

    gpg --export-secret-key -a <identity> <keyname>.secret.gpg

### Update the AWS Secrets Manager secrets

To finalise deployment update the values for the following AWS Secrets Manager secrets:

    /<stage>/ftacel/ingest/<stage>-ftacel-ingest-gpgkey = The contents of the private key file
    /<stage>/ftacel/ingest/<stage>-ftacel-ingest-gpgsecret = The passphrase for the secret key

## Ingesting data into S3

To copy data to S3 the process which is putting the data must specify the correct KMS encryption key to use, for
example, using a named AWS profile in dev:

    #!/bin/bash
    
    accountId=<accountId>
    kmsId=<kmsId>
    profile=foxtel
    stage=dev
        
    aws s3 cp \
        --profile $profile \
        --sse aws:kms \
        --sse-kms-key-id ${kmsId} \
        ./data/customer-001.csv.gpg \
        s3://${stage}-ftacel-ingest-${accountId}/ingest/


The KMS id is exported from the CloudFormation template as:

    <stage>-ftacel-ingest-s3-kms-id
    
The uploading process on the Foxtel side will require IAM permissions for S3 PutObject and 
access to the KMS key for encryption.
