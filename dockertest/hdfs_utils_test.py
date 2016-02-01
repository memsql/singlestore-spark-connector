import os
import json

def run_hdfs_utils(ctx, path, hdfs_server=None, hdfs_user="memsql", expected_return_code=0):
    print "Querying HDFS"
    if hdfs_server is None:
        hdfs_server = ctx.get_hdfs_server()
    classpath = ctx.MEMSQL_JAR_PATH + os.pathsep + os.path.join(ctx.ROOT_PATH, "memsql-spark/lib/*")
    resp = ctx._shell.run([
        os.path.join(ctx.ROOT_PATH, "memsql-spark/jdk/bin/java"),
        "-cp", classpath,
        "com.memsql.hdfs_utils.HDFSUtils",
        "ls",
        "--hdfs-server", hdfs_server,
        "--user", hdfs_user,
        path
    ], allow_error=True)
    print("STDOUT: \n%s" % resp.output)
    print("STDERR: \n%s" % resp.stderr_output)

    assert resp.return_code == expected_return_code, "HDFS querying failed"
    return json.loads(resp.output)

def test_hdfs_utils_sanity(local_context):
    ctx = local_context()

    data = run_hdfs_utils(ctx, "/loader-tests/working_files/file0")
    assert data['success']
    assert len(data["files"]) == 1
    assert set(f["path"] for f in data["files"]) == {
        "/loader-tests/working_files/file0"
    }

    data = run_hdfs_utils(ctx, "/loader-tests/working_files/file*")
    assert data['success']
    assert len(data["files"]) == 10
    assert set(f["path"] for f in data["files"]) == {
        "/loader-tests/working_files/file0",
        "/loader-tests/working_files/file1",
        "/loader-tests/working_files/file2",
        "/loader-tests/working_files/file3",
        "/loader-tests/working_files/file4",
        "/loader-tests/working_files/file5",
        "/loader-tests/working_files/file6",
        "/loader-tests/working_files/file7",
        "/loader-tests/working_files/file8",
        "/loader-tests/working_files/file9"
    }

    data = run_hdfs_utils(ctx, "/loader-tests/working_files/file[1-3]")
    assert data['success']
    assert len(data["files"]) == 3
    assert set(f["path"] for f in data["files"]) == {
        "/loader-tests/working_files/file1",
        "/loader-tests/working_files/file2",
        "/loader-tests/working_files/file3"
    }

def test_hdfs_utils_errors(local_context):
    ctx = local_context()

    data = run_hdfs_utils(ctx, "/loader-tests/working_files/file0", hdfs_server="hdfs://fakehost.local:9000", expected_return_code=1)
    assert not data['success']
    assert data["files"] == []
    assert "UnknownHostException" in data["error"]

    data = run_hdfs_utils(ctx, "/loader-tests/not_a_real_file")
    assert data['success']
    assert data["files"] == []

    data = run_hdfs_utils(ctx, "/loader-tests/restricted_files/file0", hdfs_user="not_a_real_user", expected_return_code=1)
    assert not data['success']
    assert data["files"] == []
    assert "does not have permission" in data["error"]

    assert 0
