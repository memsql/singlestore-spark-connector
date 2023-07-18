FROM apache/zeppelin:0.9.0

ENV SPARK_VERSION=3.4.0

USER root

RUN wget https://apache.ip-connect.vn.ua/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz
RUN tar xf spark-${SPARK_VERSION}-bin-hadoop2.7.tgz -C /
RUN rm -rf spark-${SPARK_VERSION}-bin-hadoop2.7.tgz
ENV SPARK_HOME=/spark-${SPARK_VERSION}-bin-hadoop2.7
ENV ZEPPELIN_PORT=8082
RUN rm -rf /zeppelin/notebook/*

EXPOSE ${ZEPPELIN_PORT}/tcp
