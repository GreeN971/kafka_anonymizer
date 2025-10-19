FROM yandex/clickhouse-server
COPY http_log.capnp /var/lib/clickhouse/format_schemas/
COPY schema.sql /root/
COPY config.xml /etc/clickhouse-server/
COPY initdb.sh /docker-entrypoint-initdb.d
