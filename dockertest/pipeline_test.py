import time
import json
from memsql.common import database

def setup_spark(ctx):
    ctx.run_ops()
    ctx.deploy_memsql_cluster(num_aggs=0, num_leaves=1)
    print "MemSQL up"
    ctx.deploy_spark()
    print "Spark up"

def setup_kafka(ctx, topic, num_partitions):
    ctx.run_kafka()
    time.sleep(5)
    ctx.create_kafka_topic(topic, num_partitions=num_partitions)
    print "Kafka up"
    print "sleep(15)"

def test_kafka_checkpointing_multiple_partitions(local_context):
    """
        Tests that loading data from Kafka will
            * start from the latest offsets if no checkpoint
            * start from the last checkpoint if it is available
            * run a batch until it succeeds

        Since there are multiple Kafka partitions, a failing batch will still insert data from all partitions but
        the one with malformed JSON
    """

    ctx = local_context()
    setup_spark(ctx)
    setup_kafka(ctx, topic="test", num_partitions=5)
    # wait for spark to be deployed
    time.sleep(15)
    topic = ctx.get_kafka_topic("test")
    producer = topic.get_producer()
    conn = database.connect(host="127.0.0.1", port=3306, user="root", password="", database="information_schema")

    print "Submitting pipeline"
    resp = ctx.pipeline_put(pipeline_id="test", batch_interval=1, config={
        "config_version": 1,
        "extract": {
            "kind": "ZookeeperManagedKafka",
            "config": {
                "zk_quorum": ["%s:2181" % ctx.external_ip],
                "topic": "test"
            }
        },
        "transform": {
            "kind": "Json",
            "config": {
                "column_name": "data",
            }
        },
        "load": {
            "kind": "MemSQL",
            "config": {
                "db_name": "db",
                "table_name": "t",
                "dry_run": False
            }
        },
        "enable_checkpointing": True
    })
    assert resp.status_code == 200, "pipeline put failed with %s" % resp.text
    print "Waiting for pipeline to start"
    # since we don't have any initial checkpoint data, we wait for the pipeline to be completely started
    # before adding data to kafka
    ctx.pipeline_wait_for_batches(pipeline_id="test", count=5)

    # add some data to kafka and wait
    producer.produce(['{"num": %d}' % i for i in range(10)])
    print "Waiting for pipeline to process batches"
    ctx.pipeline_wait_for_batches(pipeline_id="test", count=10, since_timestamp=time.time())

    print "Stopping pipeline"
    resp = ctx.pipeline_update(pipeline_id="test", active=False)
    assert resp.status_code == 200, "pipeline update failed with %s" % resp.text

    # verify that we inserted all the data from kafka
    rows = map(lambda x: int(x.num), conn.query("SELECT data::$num AS num FROM db.t"))
    assert sum(rows) == sum(range(10)), "incorrect row data: %d but expected %d" % (sum(rows), sum(range(10)))

    # verify that the kafka offsets and the checkpoint data match
    checkpoint_data = json.loads(conn.get("SELECT * FROM memsql_streamliner.checkpoints WHERE pipeline_id = 'test'").checkpoint_data)
    assert checkpoint_data['version'] == 1
    assert checkpoint_data['zookeeper'] == "%s:2181" % ctx.external_ip
    assert len(checkpoint_data['offsets']) == 5
    checkpoint_offsets = checkpoint_data['offsets']
    kafka_offsets = topic.latest_available_offsets()
    for checkpoint_offset in checkpoint_offsets:
        assert checkpoint_offset['topic'] == topic.name
        partition = checkpoint_offset['partition']
        kafka_offset_partition_response = kafka_offsets[partition]
        assert kafka_offset_partition_response.offset[0] == checkpoint_offset['offset']

    # now add more data to kafka
    producer.produce(['{"num": %d}' % i for i in range(10, 50)])

    # start the pipeline and check that we start from the last checkpoint, not the latest offsets
    print "Starting pipeline"
    resp = ctx.pipeline_update(pipeline_id="test", active=True)
    assert resp.status_code == 200, "pipeline update failed with %s" % resp.text

    print "Waiting for pipeline to process batches"
    ctx.pipeline_wait_for_batches(pipeline_id="test", count=10, since_timestamp=time.time())

    print "Stopping pipeline"
    resp = ctx.pipeline_update(pipeline_id="test", active=False)
    assert resp.status_code == 200, "pipeline update failed with %s" % resp.text

    # we should not have skipped any data and we should not have duplicated any data
    rows = map(lambda x: int(x.num), conn.query("SELECT data::$num AS num FROM db.t"))
    assert sum(rows) == sum(range(50)), "incorrect row data: %d but expected %d" % (sum(rows), sum(range(50)))

    # verify that the kafka offsets and the checkpoint data match
    checkpoint_data = json.loads(conn.get("SELECT * FROM memsql_streamliner.checkpoints WHERE pipeline_id = 'test'").checkpoint_data)
    assert checkpoint_data['version'] == 1
    assert checkpoint_data['zookeeper'] == "%s:2181" % ctx.external_ip
    assert len(checkpoint_data['offsets']) == 5
    checkpoint_offsets = checkpoint_data['offsets']
    kafka_offsets = topic.latest_available_offsets()
    for checkpoint_offset in checkpoint_offsets:
        assert checkpoint_offset['topic'] == topic.name
        partition = checkpoint_offset['partition']
        kafka_offset_partition_response = kafka_offsets[partition]
        assert kafka_offset_partition_response.offset[0] == checkpoint_offset['offset']

    # add a new kafka broker and add some data
    ctx.run_kafka(broker_id=1, port=9093)
    topic = ctx.get_kafka_topic("test")
    producer2 = topic.get_producer()
    producer2.produce(['{"num": %d}' % i for i in range(50, 100)])

    # start the pipeline and check that we start from the last checkpoint, not the latest offsets
    print "Starting pipeline"
    resp = ctx.pipeline_update(pipeline_id="test", active=True)
    assert resp.status_code == 200, "pipeline update failed with %s" % resp.text

    print "Waiting for pipeline to process batches"
    ctx.pipeline_wait_for_batches(pipeline_id="test", count=10, since_timestamp=time.time())

    print "Stopping pipeline"
    resp = ctx.pipeline_update(pipeline_id="test", active=False)
    assert resp.status_code == 200, "pipeline update failed with %s" % resp.text

    # we should not have skipped any data and we should not have duplicated any data
    rows = map(lambda x: int(x.num), conn.query("SELECT data::$num AS num FROM db.t"))
    assert sum(rows) == sum(range(100)), "incorrect row data: %d but expected %d" % (sum(rows), sum(range(100)))

    # verify that the kafka offsets and the checkpoint data match
    checkpoint_data = json.loads(conn.get("SELECT * FROM memsql_streamliner.checkpoints WHERE pipeline_id = 'test'").checkpoint_data)
    assert checkpoint_data['version'] == 1
    assert checkpoint_data['zookeeper'] == "%s:2181" % ctx.external_ip
    assert len(checkpoint_data['offsets']) == 5
    checkpoint_offsets = checkpoint_data['offsets']
    kafka_offsets = topic.latest_available_offsets()
    for checkpoint_offset in checkpoint_offsets:
        assert checkpoint_offset['topic'] == topic.name
        partition = checkpoint_offset['partition']
        kafka_offset_partition_response = kafka_offsets[partition]
        assert kafka_offset_partition_response.offset[0] == checkpoint_offset['offset']

    # NOTE: if you are using multi-partition kafka and there is malformed data, we will still load the correctly
    # formatted partitions.
    # insert some malformed json followed by valid data
    producer.produce(["this is invalid json"])
    producer.produce(['{"num": %d}' % i for i in range(100, 200)])

    # start the pipeline with the new kafka data
    print "Starting pipeline"
    resp = ctx.pipeline_update(pipeline_id="test", active=True)
    assert resp.status_code == 200, "pipeline update failed with %s" % resp.text

    print "Waiting for pipeline to process batches"
    ctx.pipeline_wait_for_batches(pipeline_id="test", count=10, since_timestamp=time.time())

    print "Stopping pipeline"
    resp = ctx.pipeline_update(pipeline_id="test", active=False)
    assert resp.status_code == 200, "pipeline update failed with %s" % resp.text

    # we have loaded new data because some of the kafka partitions were completely valid
    rows = map(lambda x: int(x.num), conn.query("SELECT data::$num AS num FROM db.t"))
    assert sum(rows) > sum(range(100)), "incorrect row data: %d but expected more than %d" % (sum(rows), sum(range(100)))

    # but the checkpoint data should be the same as before
    new_checkpoint_data = json.loads(conn.get("SELECT * FROM memsql_streamliner.checkpoints WHERE pipeline_id = 'test'").checkpoint_data)
    assert new_checkpoint_data == checkpoint_data

def test_kafka_checkpointing_single_partition(local_context):
    """
        Tests that loading data from Kafka will
            * start from the latest offsets if no checkpoint
            * start from the last checkpoint if it is available
            * run a batch until it succeeds

        Since there is only one Kafka partition, a failing batch due to malformed JSON will not insert any data
    """

    ctx = local_context()
    setup_spark(ctx)
    setup_kafka(ctx, topic="test", num_partitions=1)
    # wait for spark to be deployed
    time.sleep(15)
    topic = ctx.get_kafka_topic("test")
    producer = topic.get_producer()
    conn = database.connect(host="127.0.0.1", port=3306, user="root", password="", database="information_schema")

    print "Submitting pipeline"
    resp = ctx.pipeline_put(pipeline_id="test", batch_interval=1, config={
        "config_version": 1,
        "extract": {
            "kind": "ZookeeperManagedKafka",
            "config": {
                "zk_quorum": ["%s:2181" % ctx.external_ip],
                "topic": "test"
            }
        },
        "transform": {
            "kind": "Json",
            "config": {
                "column_name": "data",
            }
        },
        "load": {
            "kind": "MemSQL",
            "config": {
                "db_name": "db",
                "table_name": "t",
                "dry_run": False
            }
        },
        "enable_checkpointing": True
    })
    assert resp.status_code == 200, "pipeline put failed with %s" % resp.text
    print "Waiting for pipeline to start"
    # since we don't have any initial checkpoint data, we wait for the pipeline to be completely started
    # before adding data to kafka
    ctx.pipeline_wait_for_batches(pipeline_id="test", count=5)

    # add some data to kafka and wait
    producer.produce(['{"num": %d}' % i for i in range(10)])
    print "Waiting for pipeline to process batches"
    ctx.pipeline_wait_for_batches(pipeline_id="test", count=10, since_timestamp=time.time())

    print "Stopping pipeline"
    resp = ctx.pipeline_update(pipeline_id="test", active=False)
    assert resp.status_code == 200, "pipeline update failed with %s" % resp.text

    # verify that we inserted all the data from kafka
    rows = map(lambda x: int(x.num), conn.query("SELECT data::$num AS num FROM db.t"))
    assert sum(rows) == sum(range(10)), "incorrect row data: %d but expected %d" % (sum(rows), sum(range(10)))

    # verify that the kafka offsets and the checkpoint data match
    checkpoint_data = json.loads(conn.get("SELECT * FROM memsql_streamliner.checkpoints WHERE pipeline_id = 'test'").checkpoint_data)
    assert checkpoint_data['version'] == 1
    assert checkpoint_data['zookeeper'] == "%s:2181" % ctx.external_ip
    assert len(checkpoint_data['offsets']) == 1
    checkpoint_offsets = checkpoint_data['offsets']
    kafka_offsets = topic.latest_available_offsets()
    checkpoint_offset = checkpoint_offsets[0]
    assert checkpoint_offset['topic'] == topic.name
    partition = checkpoint_offset['partition']
    kafka_offset_partition_response = kafka_offsets[partition]
    assert kafka_offset_partition_response.offset[0] == checkpoint_offset['offset']

    # now add more data to kafka
    producer.produce(['{"num": %d}' % i for i in range(10, 50)])

    # start the pipeline and check that we start from the last checkpoint, not the latest offsets
    print "Starting pipeline"
    resp = ctx.pipeline_update(pipeline_id="test", active=True)
    assert resp.status_code == 200, "pipeline update failed with %s" % resp.text

    print "Waiting for pipeline to process batches"
    ctx.pipeline_wait_for_batches(pipeline_id="test", count=10, since_timestamp=time.time())

    print "Stopping pipeline"
    resp = ctx.pipeline_update(pipeline_id="test", active=False)
    assert resp.status_code == 200, "pipeline update failed with %s" % resp.text

    # we should not have skipped any data and we should not have duplicated any data
    rows = map(lambda x: int(x.num), conn.query("SELECT data::$num AS num FROM db.t"))
    assert sum(rows) == sum(range(50)), "incorrect row data: %d but expected %d" % (sum(rows), sum(range(50)))

    # verify that the kafka offsets and the checkpoint data match
    checkpoint_data = json.loads(conn.get("SELECT * FROM memsql_streamliner.checkpoints WHERE pipeline_id = 'test'").checkpoint_data)
    assert checkpoint_data['version'] == 1
    assert checkpoint_data['zookeeper'] == "%s:2181" % ctx.external_ip
    assert len(checkpoint_data['offsets']) == 1
    checkpoint_offsets = checkpoint_data['offsets']
    kafka_offsets = topic.latest_available_offsets()
    checkpoint_offset = checkpoint_offsets[0]
    assert checkpoint_offset['topic'] == topic.name
    partition = checkpoint_offset['partition']
    kafka_offset_partition_response = kafka_offsets[partition]
    assert kafka_offset_partition_response.offset[0] == checkpoint_offset['offset']

    # add a new kafka broker and add some data
    ctx.run_kafka(broker_id=1, port=9093)
    topic = ctx.get_kafka_topic("test")
    producer2 = topic.get_producer()
    producer2.produce(['{"num": %d}' % i for i in range(50, 100)])

    # start the pipeline and check that we start from the last checkpoint, not the latest offsets
    print "Starting pipeline"
    resp = ctx.pipeline_update(pipeline_id="test", active=True)
    assert resp.status_code == 200, "pipeline update failed with %s" % resp.text

    print "Waiting for pipeline to process batches"
    ctx.pipeline_wait_for_batches(pipeline_id="test", count=10, since_timestamp=time.time())

    print "Stopping pipeline"
    resp = ctx.pipeline_update(pipeline_id="test", active=False)
    assert resp.status_code == 200, "pipeline update failed with %s" % resp.text

    # we should not have skipped any data and we should not have duplicated any data
    rows = map(lambda x: int(x.num), conn.query("SELECT data::$num AS num FROM db.t"))
    assert sum(rows) == sum(range(100)), "incorrect row data: %d but expected %d" % (sum(rows), sum(range(100)))

    # verify that the kafka offsets and the checkpoint data match
    checkpoint_data = json.loads(conn.get("SELECT * FROM memsql_streamliner.checkpoints WHERE pipeline_id = 'test'").checkpoint_data)
    assert checkpoint_data['version'] == 1
    assert checkpoint_data['zookeeper'] == "%s:2181" % ctx.external_ip
    assert len(checkpoint_data['offsets']) == 1
    checkpoint_offsets = checkpoint_data['offsets']
    kafka_offsets = topic.latest_available_offsets()
    checkpoint_offset = checkpoint_offsets[0]
    assert checkpoint_offset['topic'] == topic.name
    partition = checkpoint_offset['partition']
    kafka_offset_partition_response = kafka_offsets[partition]
    assert kafka_offset_partition_response.offset[0] == checkpoint_offset['offset']

    # NOTE: since there is a single partition, we not load any data because the entire partition will fail.
    # insert some malformed json followed by valid data
    producer.produce(["this is invalid json"])
    producer.produce(['{"num": %d}' % i for i in range(100, 200)])

    # start the pipeline with the new kafka data
    print "Starting pipeline"
    resp = ctx.pipeline_update(pipeline_id="test", active=True)
    assert resp.status_code == 200, "pipeline update failed with %s" % resp.text

    print "Waiting for pipeline to process batches"
    ctx.pipeline_wait_for_batches(pipeline_id="test", count=10, since_timestamp=time.time())

    print "Stopping pipeline"
    resp = ctx.pipeline_update(pipeline_id="test", active=False)
    assert resp.status_code == 200, "pipeline update failed with %s" % resp.text

    # we should not have loaded any new data because the first batch never succeeded
    rows = map(lambda x: int(x.num), conn.query("SELECT data::$num AS num FROM db.t"))
    assert sum(rows) == sum(range(100)), "incorrect row data: %d but expected %d" % (sum(rows), sum(range(100)))

    # and the checkpoint data should be the same as before
    new_checkpoint_data = json.loads(conn.get("SELECT * FROM memsql_streamliner.checkpoints WHERE pipeline_id = 'test'").checkpoint_data)
    assert new_checkpoint_data == checkpoint_data
