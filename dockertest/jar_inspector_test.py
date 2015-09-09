import os
import json

def run_jar_inspector(ctx, jar):
    print "Inspecting JAR"
    resp = ctx._shell.run([
        os.path.join(ctx.ROOT_PATH, "memsql-spark/jdk/bin/java"),
        "-cp", ctx.MEMSQL_JAR_PATH,
        "com.memsql.jar_inspector.JarInspector",
        "--target", jar
    ], allow_error=True)
    print("STDOUT: \n%s" % resp.output)
    print("STDERR: \n%s" % resp.stderr_output)

    assert resp.return_code == 0, "jar inspector failed to find classes"
    return json.loads(resp.output)

def test_jar_inspector_sanity(local_context):
    ctx = local_context()
    data = run_jar_inspector(ctx, ctx.MEMSQL_JAR_PATH)

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

def test_jar_inspector_external_jar(local_context):
    ctx = local_context()
    data = run_jar_inspector(ctx, os.path.join(ctx.ROOT_PATH, "sample_pipelines.jar"))

    # NOTE: we have to filter out the extra classes contained in the test build of the interface jar
    sample_extractors = filter(lambda x: "com.memsql.spark" not in x, data['extractors'])
    sample_transformers = filter(lambda x: "com.memsql.spark" not in x, data['transformers'])

    assert set(sample_extractors) == {
        "com.memsql.streamliner.starter.MyExtractor",
        "com.memsql.streamliner.starter.MySimpleExtractor",
        "com.memsql.streamliner.starter.DStreamExtractor",
        "com.memsql.streamliner.starter.SequenceExtractor",
        "com.memsql.streamliner.starter.ConstantExtractor",
        "com.memsql.streamliner.starter.ConfigurableConstantExtractor"
    }

    assert set(sample_transformers) == {
        "com.memsql.streamliner.starter.MySimpleTransformer",
        "com.memsql.streamliner.starter.EvenNumbersOnlyTransformer",
        "com.memsql.streamliner.starter.ConfigurableNumberParityTransformer"
    }
