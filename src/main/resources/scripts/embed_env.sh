#!/bin/bash

INP=../../../docker-compose-template.yml
OUT=../../../result.yml

set -a
source ../../../.env
envsubst < $INP > $OUT
set +a
