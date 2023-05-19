cd /home/amakarovych-ua/Projects/S2-JDBC-Connector
mvn package -DjavadocExecutable=/home/amakarovych-ua/Tools/openlogic-openjdk-8u372-b07-linux-x64/bin/javadoc -Dmaven.test.skip
cp ./target/singlestore-jdbc-client-1.1.5.jar /home/amakarovych-ua/Projects/singlestore-spark-connector/lib
