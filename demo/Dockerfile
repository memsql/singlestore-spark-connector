FROM apache/zeppelin:0.9.0

USER root

RUN wget https://apache.ip-connect.vn.ua/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz
RUN tar xf spark-2.4.5-bin-hadoop2.7.tgz -C /
RUN rm -rf spark-2.4.5-bin-hadoop2.7.tgz
ENV SPARK_HOME=/spark-2.4.5-bin-hadoop2.7
ENV ZEPPELIN_PORT=8082
RUN rm -rf /zeppelin/notebook/*

EXPOSE 8082/tcp
