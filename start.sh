#!/usr/bin/env bash
echo "cat /proc/sys/net/ipv4/tcp_mem"
cat /proc/sys/net/ipv4/tcp_mem
#echo "cat /proc/sys/net/ipv4/tcp_window_scaling"
#cat /proc/sys/net/ipv4/tcp_window_scaling

#echo "cat /proc/sys/net/core/rmem_default"
#cat /proc/sys/net/core/rmem_default
#echo "cat /proc/sys/net/core/rmem_max"
#cat /proc/sys/net/core/rmem_max
#echo "cat /proc/sys/net/core/wmem_default"
#cat /proc/sys/net/core/wmem_default
#echo "cat /proc/sys/net/core/wmem_max"
#cat /proc/sys/net/core/wmem_max
#echo "cat /proc/sys/net/core/optmem_max"
#cat /proc/sys/net/core/optmem_max

#echo "8388608 12582912 16777216" > /proc/sys/net/ipv4/tcp_mem
#cat /proc/sys/net/ipv4/tcp_mem

/usr/local/src/main -p=$SERVER_PORT
#tail -f /usr/local/src/start.sh