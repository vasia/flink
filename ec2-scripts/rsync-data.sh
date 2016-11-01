#!/bin/sh

if [ "$#" -ne 2 ]; then
    printf "Usage: ./rsync-data.sh EXTERNAL_IP INTERNAL_IP\n\nBefore: copy all internal ips of the task managers to the slaves file!"
fi

IP=$1
INTERNAL_IP=$2

sed "s/JOBMANAGER_PLACEHOLDER/${INTERNAL_IP}/g" flink-conf.yaml > flink-conf-amazon.yaml

rsync -avz ../flink-dist/target/flink-1.2-WATERMARKS-bin/flink-1.2-WATERMARKS/ -e ssh "ubuntu@${IP}:watermarks"
scp flink-conf-amazon.yaml "ubuntu@${IP}:watermarks/conf/flink-conf.yaml"
scp slaves "ubuntu@${IP}:watermarks/conf/slaves"
scp slaves "ubuntu@${IP}:otherservers"

ssh "ubuntu@${IP}" ./copytoothers.sh
