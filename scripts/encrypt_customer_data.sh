#!/bin/bash

set -e

gpg -e -u jospas@amazon.com -r jospas@amazon.com -o ./data/customer-001.csv.gpg -z 6 ./data/customer-001.csv