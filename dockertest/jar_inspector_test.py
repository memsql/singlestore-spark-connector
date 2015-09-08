import os
import json

def test_jar_inspector(local_context):
    ctx = local_context()
    print "Inspecting JAR"
    resp = ctx._shell.run([
        os.path.join(ctx.ROOT_PATH, "memsql-spark/jdk/bin/java"),
        "-cp", ctx.MEMSQL_JAR_PATH,
        "com.memsql.jar_inspector.JarInspector",
        "--target", ctx.MEMSQL_JAR_PATH
    ], allow_error=True)
    print("STDOUT: \n%s" % resp.output)
    print("STDERR: \n%s" % resp.stderr_output)

    assert resp.return_code == 0, "jar inspector failed to find classes"

    data = json.loads(resp.output)
    assert data['success']
    assert set(data['extractors']) == {
        "com.memsql.spark.Extractor1",
        "com.memsql.spark.Extractor2",
        "com.memsql.spark.Extractor3",
        "com.memsql.spark.Extractor4",
        "com.memsql.spark.Extractor5",
        "com.memsql.spark.Extractor6",
        "com.memsql.spark.Extractor7",
        "com.memsql.spark.AbstractExtractor4",
        "com.memsql.spark.AbstractExtractor5",
        "com.memsql.spark.AbstractExtractor6",
        "com.memsql.spark.NestedAbstractExtractor"
    }
    assert set(data['transformers']) == {
        "com.memsql.spark.Transformer1",
        "com.memsql.spark.Transformer2",
        "com.memsql.spark.Transformer3",
        "com.memsql.spark.Transformer4",
        "com.memsql.spark.Transformer5",
        "com.memsql.spark.Transformer6",
        "com.memsql.spark.Transformer7",
        "com.memsql.spark.AbstractTransformer4",
        "com.memsql.spark.AbstractTransformer5",
        "com.memsql.spark.AbstractTransformer6",
        "com.memsql.spark.NestedAbstractTransformer"
    }
