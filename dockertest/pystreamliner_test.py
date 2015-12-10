import os
import time
import json
import textwrap
from memsql.common import database
from dockertest.conftest import SPARK_ROOT

INTERFACE_LOG = os.path.join(SPARK_ROOT, "interface/interface.log")

def setup_spark(ctx, num_aggs=0, num_leaves=1):
    ctx.run_ops()
    ctx.deploy_memsql_cluster(num_aggs=num_aggs, num_leaves=num_leaves)
    print "MemSQL up"
    ctx.deploy_spark()
    print "Spark up"
    ctx.wait_for_interface()
    print "Interface ready"

PYSTREAMLINER_BASE_PATH = "dockertest/pystreamliner_pipelines"

def pystreamliner_config(extractor="default_extractor.py", transformer="default_transformer.py"):
    extractor_name_parts = os.path.splitext(extractor)[0].split("_")
    extractor_class = "".join(map(lambda x: x.capitalize(), extractor_name_parts))
    formatted_extractor_code = open(os.path.join(PYSTREAMLINER_BASE_PATH, extractor)).read()

    transformer_name_parts = os.path.splitext(transformer)[0].split("_")
    transformer_class = "".join(map(lambda x: x.capitalize(), transformer_name_parts))
    formatted_transformer_code = open(os.path.join(PYSTREAMLINER_BASE_PATH, transformer)).read()

    return {
        "config_version": 1,
        "extract": {
            "kind": "Python",
            "config": {
                "class_name": extractor_class,
                "code": formatted_extractor_code
            }
        },
        "transform": {
            "kind": "Python",
            "config": {
                "class_name": transformer_class,
                "code": formatted_transformer_code
            }
        },
        "load": {
            "kind": "MemSQL",
            "config": {
                "db_name": "db",
                "table_name": "t",
                "dry_run": False
            }
        }
    }

def test_pystreamliner_sanity(local_context):
    """
        Tests if a basic pypeline works
    """
    ctx = local_context()
    setup_spark(ctx)
    config = pystreamliner_config(extractor="sanity_extractor.py", transformer="sanity_transformer.py")

    ctx.pipeline_put(pipeline_id="pypeline", batch_interval=1, config=config)

    # enable tracing and check that log messages are recorded appropriately
    ctx.pipeline_update(pipeline_id="pypeline", active=True, trace_batch_count=20)

    # wait for a couple more batches before stopping the pipeline
    batch_end = ctx.pipeline_wait_for_batches(pipeline_id="pypeline", count=10, timeout=120)
    assert batch_end["success"]
    assert batch_end["load"]["count"] == 1
    assert batch_end["batch_type"] == "Traced"
    assert len(batch_end["extract"]["logs"]) == 1
    assert "extractor info message" in batch_end["extract"]["logs"][0]
    assert len(batch_end["transform"]["logs"]) == 1
    assert "transformer error message" in batch_end["transform"]["logs"][0]

    ctx.pipeline_update(pipeline_id="pypeline", active=False)

    conn = database.connect(host="127.0.0.1", port=3306, user="root", password="", database="db")
    resp = conn.get("SELECT COUNT(*) AS count FROM t")
    assert resp.count >= 10
    num_rows = resp.count

    resp = conn.get("SELECT SUM(foo_int_doubled) AS f FROM t")
    assert resp.f == 2 * sum(range(1, num_rows + 1))

def test_pystreamliner_multiple_tables(local_context):
    """
        Tests if a pypeline reading and writing additional tables works
    """
    ctx = local_context()
    setup_spark(ctx)
    config = pystreamliner_config(extractor="multiple_tables_extractor.py", transformer="multiple_tables_transformer.py")

    # create some reference data
    conn = database.connect(host="127.0.0.1", port=3306, user="root", password="", database="information_schema")
    conn.execute("CREATE DATABASE reference")
    conn.execute("CREATE TABLE reference.data (foo int primary key)")
    conn.query("INSERT INTO reference.data VALUES (%s)", 1)

    ctx.pipeline_put(pipeline_id="pypeline", batch_interval=1, config=config)

    # wait for a couple more batches before stopping the pipeline
    batch_end = ctx.pipeline_wait_for_batches(pipeline_id="pypeline", count=5, timeout=300)
    assert batch_end["success"]
    assert batch_end["load"]["count"] == 1

    ctx.pipeline_update(pipeline_id="pypeline", active=False)

    conn = database.connect(host="127.0.0.1", port=3306, user="root", password="", database="db")
    resp = conn.get("SELECT COUNT(*) AS count FROM t")
    assert resp.count >= 5
    num_rows = resp.count

    resp = conn.get("SELECT SUM(foo) AS f FROM t")
    assert resp.f == num_rows

    for i in range(num_rows):
        table = "pystreamliner.table%d" % i
        resp = conn.get("SELECT SUM(foo) AS f FROM " + table)
        assert resp.f == 1

# EXTRACTOR TESTS
def test_pystreamliner_extractor_syntax_error(local_context):
    """
        Tests if extractor syntax errors are handled
    """
    ctx = local_context()
    setup_spark(ctx)
    config = pystreamliner_config(extractor="syntax_error_extractor.py")

    ctx.pipeline_put(pipeline_id="pypeline", batch_interval=1, config=config)

    time.sleep(30) # wait for extractor to exit

    data = ctx.pipeline_get("pypeline")
    assert data["state"] == "ERROR"
    assert "Process terminated with exit code" in data["error"]
    assert "this is not valid python" in data["error"]
    assert "SyntaxError: invalid syntax" in data["error"]

def test_pystreamliner_extractor_register_timeout(local_context):
    """
        Tests if an extractor failing to register is handled
    """
    ctx = local_context()
    setup_spark(ctx)
    config = pystreamliner_config(extractor="timeout_extractor.py")

    ctx.pipeline_put(pipeline_id="pypeline", batch_interval=1, config=config)

    time.sleep(60) # wait for extractor to timeout

    data = ctx.pipeline_get("pypeline")
    assert data["state"] == "ERROR"
    assert data["error"] == "Python process TimeoutExtractor took longer than 30 seconds to be registered"

def test_pystreamliner_extractor_initialize_exception(local_context):
    """
        Tests if an extractor raising an exception in the call to initialize is handled
    """
    ctx = local_context()
    setup_spark(ctx)
    config = pystreamliner_config(extractor="initialize_exception_extractor.py")

    ctx.pipeline_put(pipeline_id="pypeline", batch_interval=1, config=config)

    time.sleep(60) # wait for extractor to timeout

    data = ctx.pipeline_get("pypeline")
    assert data["state"] == "ERROR"
    assert "Encountered an exception in Python" in data["error"]
    assert "Exception: initialize is raising an exception" in data["error"]

def test_pystreamliner_extractor_next_exception(local_context):
    """
        Tests if an extractor raising an exception in the call to next is handled
    """
    ctx = local_context()
    setup_spark(ctx)
    config = pystreamliner_config(extractor="next_exception_extractor.py")

    ctx.pipeline_put(pipeline_id="pypeline", batch_interval=1, config=config)

    # every batch should fail but pipeline should not crash
    batch_end = ctx.pipeline_wait_for_batches(pipeline_id="pypeline", count=1)
    assert not batch_end["success"]
    assert "Encountered an exception in Python" in batch_end["extract"]["error"]
    assert "Exception: next is raising an exception" in batch_end["extract"]["error"]

    batch_end = ctx.pipeline_wait_for_batches(pipeline_id="pypeline", count=1)
    assert not batch_end["success"]
    assert "Encountered an exception in Python" in batch_end["extract"]["error"]
    assert "Exception: next is raising an exception" in batch_end["extract"]["error"]

    data = ctx.pipeline_get("pypeline")
    assert data["state"] == "RUNNING"

def test_pystreamliner_extractor_cleanup_exception(local_context):
    """
        Tests if an extractor raising an exception in the call to cleanup is handled
    """
    ctx = local_context()
    setup_spark(ctx)
    config = pystreamliner_config(extractor="cleanup_exception_extractor.py")

    ctx.pipeline_put(pipeline_id="pypeline", batch_interval=1, config=config)

    # every batch should succeed
    batch_end = ctx.pipeline_wait_for_batches(pipeline_id="pypeline", count=5)
    assert batch_end["success"]

    # the raised exception should be logged but not affect the pipeline or the interface
    ctx.pipeline_update(pipeline_id="pypeline", active=False)

    time.sleep(10)

    interface_log = open(INTERFACE_LOG).read()
    assert "Exception in pipeline pypeline while stopping extractor" in interface_log
    assert "Encountered an exception in Python" in interface_log
    assert "Exception: cleanup is raising an exception" in interface_log
    data = ctx.pipeline_get("pypeline")
    assert data["state"] == "STOPPED"

# TRANSFORMER TESTS
def test_pystreamliner_transformer_syntax_error(local_context):
    """
        Tests if transformer syntax errors are handled
    """
    ctx = local_context()
    setup_spark(ctx)
    config = pystreamliner_config(transformer="syntax_error_transformer.py")

    ctx.pipeline_put(pipeline_id="pypeline", batch_interval=1, config=config)

    time.sleep(30) # wait for transformer to exit

    data = ctx.pipeline_get("pypeline")
    assert data["state"] == "ERROR"
    assert "Process terminated with exit code" in data["error"]
    assert "this is not valid python" in data["error"]
    assert "SyntaxError: invalid syntax" in data["error"]

def test_pystreamliner_transformer_register_timeout(local_context):
    """
        Tests if an transformer failing to register is handled
    """
    ctx = local_context()
    setup_spark(ctx)
    config = pystreamliner_config(transformer="timeout_transformer.py")

    ctx.pipeline_put(pipeline_id="pypeline", batch_interval=1, config=config)

    time.sleep(60) # wait for transformer to timeout

    data = ctx.pipeline_get("pypeline")
    assert data["state"] == "ERROR"
    assert data["error"] == "Python process TimeoutTransformer took longer than 30 seconds to be registered"

def test_pystreamliner_transformer_initialize_exception(local_context):
    """
        Tests if an transformer raising an exception in the call to initialize is handled
    """
    ctx = local_context()
    setup_spark(ctx)
    config = pystreamliner_config(transformer="initialize_exception_transformer.py")

    ctx.pipeline_put(pipeline_id="pypeline", batch_interval=1, config=config)

    time.sleep(60) # wait for transformer to timeout

    data = ctx.pipeline_get("pypeline")
    assert data["state"] == "ERROR"
    assert "Encountered an exception in Python" in data["error"]
    assert "Exception: initialize is raising an exception" in data["error"]

def test_pystreamliner_transformer_transform_exception(local_context):
    """
        Tests if an transformer raising an exception in the call to transform is handled
    """
    ctx = local_context()
    setup_spark(ctx)
    config = pystreamliner_config(transformer="transform_exception_transformer.py")

    ctx.pipeline_put(pipeline_id="pypeline", batch_interval=1, config=config)

    # every batch should fail but pipeline should not crash
    batch_end = ctx.pipeline_wait_for_batches(pipeline_id="pypeline", count=1)
    assert not batch_end["success"]
    assert "Encountered an exception in Python" in batch_end["transform"]["error"]
    assert "Exception: transform is raising an exception" in batch_end["transform"]["error"]

    batch_end = ctx.pipeline_wait_for_batches(pipeline_id="pypeline", count=1)
    assert not batch_end["success"]
    assert "Encountered an exception in Python" in batch_end["transform"]["error"]
    assert "Exception: transform is raising an exception" in batch_end["transform"]["error"]

    data = ctx.pipeline_get("pypeline")
    assert data["state"] == "RUNNING"

def test_pystreamliner_transformer_cleanup_exception(local_context):
    """
        Tests if an transformer raising an exception in the call to cleanup is handled
    """
    ctx = local_context()
    setup_spark(ctx)
    config = pystreamliner_config(transformer="cleanup_exception_transformer.py")

    ctx.pipeline_put(pipeline_id="pypeline", batch_interval=1, config=config)

    # every batch should succeed
    batch_end = ctx.pipeline_wait_for_batches(pipeline_id="pypeline", count=5)
    assert batch_end["success"]

    # the raised exception should be logged but not affect the pipeline or the interface
    ctx.pipeline_update(pipeline_id="pypeline", active=False)

    time.sleep(10)

    interface_log = open(INTERFACE_LOG).read()
    assert "Exception in pipeline pypeline while stopping transformer" in interface_log
    assert "Encountered an exception in Python" in interface_log
    assert "Exception: cleanup is raising an exception" in interface_log
    data = ctx.pipeline_get("pypeline")
    assert data["state"] == "STOPPED"
