FROM psy3.memcompute.com/dockertest:memsql-32

RUN sudo apt-get update && \
    sudo apt-get install -y python-dev

# configure locale
RUN sudo locale-gen en_US.UTF-8 && sudo update-locale LANG=en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LANGUAGE en_US:en

# uninstall memsql 3.2 and mysql
RUN echo "" | sudo tee /var/lib/dpkg/info/memsql.prerm
RUN sudo update-rc.d -f mysql remove
RUN sudo apt-get remove -y memsql
RUN sudo rm -rf /var/lib/memsql /init_mysql.sql

# add entrypoint script
ADD scripts/dockertest_startup.sh /startup.sh
RUN sudo chmod +x /startup.sh

# set up virtualenv
RUN sudo pip install virtualenv
RUN sudo virtualenv /storage/testroot/venv
ADD conf/requirements.txt /tmp/requirements.txt
RUN sudo /bin/bash -c "source /storage/testroot/venv/bin/activate; pip install -r /tmp/requirements.txt"

# install zookeeper
RUN wget -q -O /tmp/zookeeper.tar.gz http://download.memsql.com/memsql-spark-testing/zookeeper-3.4.6.tar.gz
RUN sudo tar zxvf /tmp/zookeeper.tar.gz -C /storage/testroot

# install and configure kafka
RUN wget -q -O /tmp/kafka.tar.gz http://download.memsql.com/memsql-spark-testing/kafka-0.8.2.1.tar.gz
RUN sudo tar zxvf /tmp/kafka.tar.gz -C /storage/testroot
RUN sudo mkdir /data && sudo chown -R memsql:memsql /data

# install memsql ops
ENV MEMSQL_OPS_VERSION 4.1.10
RUN wget -q -O /tmp/memsql-ops.tar.gz http://download.memsql.com/memsql-ops-$MEMSQL_OPS_VERSION/memsql-ops-$MEMSQL_OPS_VERSION.tar.gz
RUN sudo tar zxvf /tmp/memsql-ops.tar.gz -C /tmp
RUN sudo /tmp/memsql-ops-$MEMSQL_OPS_VERSION/install.sh -n

# prepare memsql
ENV MEMSQL_VERSION 4.1.2
RUN wget -q -O /tmp/memsqlbin_amd64.tar.gz http://download.memsql.com/releases/version/$MEMSQL_VERSION/memsqlbin_amd64.tar.gz
RUN sudo rm -f /var/lib/memsql-ops/data/memsql-ops.pid && \
    sudo memsql-ops start && \
    sudo memsql-ops file-add -t memsql /tmp/memsqlbin_amd64.tar.gz && \
    sudo memsql-ops stop && \
    sudo rm -f /var/lib/memsql-ops/data/memsql-ops.pid

# clean up
RUN sudo apt-get clean && sudo rm -rf /var/lib/apt/lists/* /tmp/*

# download spark distribution and update spark interface jar
RUN sudo mkdir -p /storage/testroot/memsql-spark
RUN sudo wget -q -O /storage/testroot/memsql-spark/memsql-spark.tar.gz \
    http://download.memsql.com/memsql-spark-1.5.1-distribution-1.3.0-SNAPSHOT/memsql-spark-1.5.1-distribution-1.3.0-SNAPSHOT.tar.gz && \
    sudo tar zxvf /storage/testroot/memsql-spark/memsql-spark.tar.gz -C /storage/testroot/memsql-spark && \
    sudo rm /storage/testroot/memsql-spark/memsql-spark.tar.gz
ADD tests/target/scala-2.10/tests-assembly-1.3.0-SNAPSHOT.jar /storage/testroot/memsql-spark/interface/memsql_spark_interface.jar
ADD dockertest/sample_pipelines/target/scala-2.10/sample-pipelines-assembly-0.0.1.jar /storage/testroot/sample_pipelines.jar

# prepare java for tests
RUN sudo ln -sf /var/lib/memsql-ops/data/spark/install/jdk/bin/java /usr/bin/java
ENV JAVA_HOME /var/lib/memsql-ops/data/spark/install/jdk

# add test code
ADD scripts/dockertest.sh /storage/testroot/dockertest.sh
RUN sudo chmod +x /storage/testroot/dockertest.sh
RUN sudo mkdir -p /storage/testroot/memsql-spark-connector
ADD scripts /storage/testroot/memsql-spark-connector/scripts
ADD dockertest /storage/testroot/memsql-spark-connector/dockertest
