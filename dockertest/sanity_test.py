import time
from memsql.common import database

def test_sanity(local_context):
    ctx = local_context()
    ctx.run_ops()
    ctx.deploy_memsql_cluster(num_aggs=0, num_leaves=1)
    ctx.deploy_spark()
    # wait for spark to be deployed
    time.sleep(30)
    # and then kill the superapp so that we have spark resources to run a job
    ctx.stop_ops()
    ctx.kill_superapp()

    print "Running the job"
    resp = ctx.spark_submit("com.memsql.spark.examples.WriteToMemSQLApp")
    print("STDOUT: \n%s" % resp.output)
    print("STDERR: \n%s" % resp.stderr_output)

    conn = database.connect(host="127.0.0.1", port=3306, user="root", password="", database="memsqlrdd_db")
    assert conn.get("SELECT count(*) AS c FROM output").c == 1000
