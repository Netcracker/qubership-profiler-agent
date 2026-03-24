echo "shutdown podman compose..."
podman-compose down 

sleep 3

#dos2unix clickhouse/init/init_db.sh

#export CLK_CONTAINER_NAME=clickhouse
#export CLK_TEST_USER=test
#export CLK_TEST_USER_PASSWORD=test
#export CLK_TEST_DATABASE=alarms_test
#
#export MINIO_ROOT_USER=miniotest
#export MINIO_ROOT_PASSWORD=miniotest

echo "start podman compose..."
podman-compose up -d

sleep 5
echo "done"