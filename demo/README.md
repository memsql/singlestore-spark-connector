## memsql-spark-connector demo

This is Dockerfile which uses the upstream [Zeppelin Image](https://hub.docker.com/r/apache/zeppelin/) as it's base
and has two notebooks with examples of memsql-spark-connector.

To run this docker with [MemSQL CIAB](https://hub.docker.com/r/memsql/cluster-in-a-box) follow the instructions

* Create a docker network to be able to connect zeppelin and memsql-ciab
```
docker network create zeppelin-ciab-network
```

* Pull memsql-ciab docker image
```
docker pull memsql/cluster-in-a-box
```

*  Run and start memsql-ciab docker container

```
docker run -i --init \
--name memsql-ciab \
-e LICENSE_KEY=[INPUT_YOUR_LICENSE_KEY] \
-p 3306:3306 -p 8080:8080 \
--net=zeppelin-ciab-network \
memsql/cluster-in-a-box
```
```
docker start memsql-ciab
```

* Build zeppelin docker image in `memsql-spark-connector/demo` folder
```
docker build -t zeppelin .
```

* Run zeppelin docker container
```
docker run -i --init \
--name zeppelin \
-p 8082:8082 \
--net=zeppelin-ciab-network \
-v $PWD/notebook:/zeppelin/notebook/memsql \
zeppelin
```

* open [zeppelin](http://localhost:8082/next) in your browser and try
[scala](http://localhost:8082/next/#/notebook/2F8XQUKFG),
[pyspark](http://localhost:8082/next/#/notebook/2F6Y3APTX)
and [spark sql](http://localhost:8082/next/#/notebook/2F7PZ81H6) notebooks

For setting up more powerful MemSQL trial cluster use [Helios](https://www.memsql.com/helios/)
