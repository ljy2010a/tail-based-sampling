#!/usr/bin/env bash
set -ex

ver=289

version="// Code generated by tool. DO NOT EDIT!\n
package main\n
const VERSION = \"$ver\"\n"
echo ${version} > version.go

CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o main *.go
#docker rmi -f  `docker images | grep 'ljy2010a' | awk '{print $3}'`
docker build -t tail-based-sampling:1 .
docker tag tail-based-sampling:1 registry.cn-hangzhou.aliyuncs.com/ljy2010a/tailf-based-sampling:${ver}
docker push registry.cn-hangzhou.aliyuncs.com/ljy2010a/tailf-based-sampling:${ver}

echo "registry.cn-hangzhou.aliyuncs.com/ljy2010a/tailf-based-sampling:${ver}" | pbcopy