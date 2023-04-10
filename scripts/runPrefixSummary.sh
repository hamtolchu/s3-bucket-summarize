#!/bin/zsh

aws s3 ls \
  s3://$1/$2  \
  --summarize \
  --output json