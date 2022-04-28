#!/usr/bin/env sh

IP="172.29.0.4"

function aws_s3() {
  aws --endpoint "http://${IP}:4000/" s3api "$@"
}

aws_s3 list-buckets;
aws_s3 list-buckets --query "Buckets[].Name";
aws_s3 create-bucket --bucket "test";
aws_s3 create-bucket --bucket "test";
aws_s3 create-bucket --bucket "..";
aws_s3 list-buckets --query "Buckets[].Name";
aws_s3 delete-bucket --bucket "test";
aws_s3 list-buckets --query "Buckets[].Name";
