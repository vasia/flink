#!/bin/sh

if [ "$#" -ne 1 ]; then
    printf "Usage: ./ssh.sh EXTERNAL_IP"
fi

IP=$1

ssh -L "8081:${IP}:8081" "ubuntu@$IP"
