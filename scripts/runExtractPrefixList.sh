#!/bin/zsh

aws s3api list-objects-v2 \
  --bucket $1 \
  --prefix board/inquiry/ \
  --delimiter '/' \
  --query 'CommonPrefixes[].Prefix' \
  --output json