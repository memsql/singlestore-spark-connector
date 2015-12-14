import time
import sys
import subprocess

def do_test_spark_submit(local_context, test_name, num_aggs=0, num_leaves=1, args=[]):
    ctx = local_context()
    ctx.run_ops()
    ctx.deploy_memsql_cluster(num_aggs=num_aggs, num_leaves=num_leaves)
    print "MemSQL up!"
    ctx.deploy_spark()
    print "sleep(30)"
    # wait for spark to be deployed
    time.sleep(30)
    # and then kill the spark interface so that we have spark resources to run a job
    ctx.stop_ops()
    ctx.kill_spark_interface()

    print "Running the job"
    resp = ctx.spark_submit(test_name, extra_args=args)
    print("STDOUT: \n%s" % resp.output)
    print("STDERR: \n%s" % resp.stderr_output)

    assert resp.return_code == 0, "the test failed with return code %d" % resp.return_code

def test_memsql_query_expressions_binary_operators(local_context):
    do_test_spark_submit(local_context, "com.memsql.spark.TestMemSQLQueryExpressionsBinaryOperators")

def test_types(local_context):
    do_test_spark_submit(local_context, "com.memsql.spark.TestTypes")

def test_memsql_query_pushdown_basic(local_context):
    do_test_spark_submit(local_context, "com.memsql.spark.TestMemSQLQueryPushdown")

def test_relation_provider(local_context):
    do_test_spark_submit(local_context, "com.memsql.spark.TestRelationProvider")

def test_save_errors(local_context):
    do_test_spark_submit(local_context, "com.memsql.spark.TestSaveToMemSQLErrors")

def test_create_table_default_columns(local_context):
    do_test_spark_submit(local_context, "com.memsql.spark.TestCreateTableDefaultColumns")

def test_conn_close_provider(local_context):
    do_test_spark_submit(local_context, "com.memsql.spark.TestConnCloseProvider")
