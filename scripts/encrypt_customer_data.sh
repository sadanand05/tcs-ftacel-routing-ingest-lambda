#!/bin/bash

set -e

gpg -e -u dev-ftacel-infra@foxtel.com.au -r dev-ftacel-infra@foxtel.com.au -o ./data/customer-001.csv.gpg -z 6 ./data/customer-001.csv