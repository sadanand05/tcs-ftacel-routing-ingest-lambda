#!/bin/bash

set -e

mvn clean

scripts/cloudformation/deploy_infrastructure.sh

scripts/cloudformation/deploy_roles.sh

scripts/cloudformation/deploy_ingest_lambda.sh


