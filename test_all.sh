function msc_submit() {
    $SPARK_HOME/bin/spark-submit --class $1 target/scala-2.10/MemSQL-assembly-0.1.2.jar $2
}
msc_submit test.TestMemSQLTypes keyless && \
msc_submit test.TestMemSQLDataFrameVeryBasic && \
msc_submit test.TestSaveToMemSQLVeryBasic && \
msc_submit test.TestMemSQLTypes && \
msc_submit test.TestMemSQLContextVeryBasic && \
msc_submit test.TestCreateWithKeys