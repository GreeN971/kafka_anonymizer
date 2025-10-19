#!/bin/sh

awk -v RS=';' -v outdir="/root/" '{if(NF){print $0 > outdir "schema_part" NR ".sql"}}' /root/schema.sql
ls /root/
rm /root/schema.sql
for file in /root/*.sql; do
    cat $file | clickhouse-client
    rm $file
done

unlink /docker-entrypoint-initdb.d/initdb.sh