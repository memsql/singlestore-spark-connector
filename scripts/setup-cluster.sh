#If $MEMSQL_PASSWORD is empty run the setup script without password, otherwise run the setup script with password
if [ -z "$MEMSQL_PASSWORD" ]
then
      ./scripts/ensure-test-memsql-cluster.sh
else
      ./scripts/ensure-test-memsql-cluster-password.sh
fi