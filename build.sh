#!/usr/bin/env bash
set -e
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o main *.go
docker build -t tail-based-sampling:1 .
docker tag tail-based-sampling:1 registry.cn-hangzhou.aliyuncs.com/ljy2010a/tailf-based-sampling:36
docker push registry.cn-hangzhou.aliyuncs.com/ljy2010a/tailf-based-sampling:36