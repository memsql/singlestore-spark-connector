#If $MEMSQL_PASSWORD is empty run the setup script without password, otherwise run the setup script with password
if [ -z "$SINGLESTORE_PASSWORD" ]
then
      ./scripts/ensure-test-singlestore-cluster.sh
else
      ./scripts/ensure-test-singlestore-cluster-password.sh
fi