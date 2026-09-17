#!/bin/sh
set -e

until (/usr/bin/mc alias set minio http://minio:9000/ $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD) do echo '...waiting...' && sleep 1; done;
/usr/bin/mc mb minio/warehouse || echo 'Warehouse already exists';
/usr/bin/mc anonymous set public minio/warehouse;
tail -f /dev/null