FROM centos:centos7
RUN set -ex && cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
WORKDIR /usr/local/src
COPY main /usr/local/src
COPY start.sh /usr/local/src
RUN chmod +x /usr/local/src/start.sh
ENTRYPOINT ["/bin/bash", "/usr/local/src/start.sh"]