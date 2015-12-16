import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution._
import com.memsql.spark.pushdown.MemSQLPushdownStrategy
import com.memsql.spark.connector._
import com.memsql.spark.connector.util.JDBCImplicits._

object Prelude {
  var cached: Boolean = false

  val msc = new MemSQLContext(sc)
  msc.setDatabase("bigdata")

  def patch: Unit = MemSQLPushdownStrategy.patchSQLContext(msc)
  def unpatch: Unit = MemSQLPushdownStrategy.unpatchSQLContext(msc)

  def cacheTables: Unit = {
    msc.sql("select * from rankings").registerTempTable("rankings_spark")
    msc.sql("cache table rankings_spark")
    msc.sql("select * from uservisits").registerTempTable("uservisits_spark")
    msc.sql("cache table uservisits_spark")
    cached = true
  }

  def time(block: => Array[Row]): Double = {
    val s = System.nanoTime
    val res = block
    val diff = (System.nanoTime-s) / 1e6
    println("Result:")
    println(res.map("\t" + _.toString).mkString("\n"))
    println("---")
    diff
  }

  def timeUnpatched(query: String): Unit = {
    val q = if (cached) {
      query.replace("rankings", "rankings_spark")
           .replace("uservisits", "uservisits_spark")
    } else {
      query
    }

    unpatch
    val t = time { msc.sql(q).collect }
    println(s"UNPATCHED: ${t}ms")
  }

  def timePatched(query: String): Unit = {
    patch
    val t = time { msc.sql(query).collect }
    println(s"PATCHED: ${t}ms")
  }
}

val msc = Prelude.msc

object Queries {
  def q1: Unit = {
    val q = """
      select count(*) from(
        select pageURL, pageRank from rankings where pageRank > 1000
      ) x
    """

    Prelude.timeUnpatched(q)
    Prelude.timePatched(q)
  }

  def q2: Unit = {
    val q = """
      select searchWord, sum(adRevenue) as totalRevenue
      from uservisits uv, rankings r
      where uv.destURL = r.pageURL
        and searchWord like "north%"
      group by searchWord
      order by totalRevenue desc
      limit 10
    """

    Prelude.timeUnpatched(q)
    Prelude.timePatched(q)
  }

  def q3: Unit = {
    val q = """
      select destURL, count(*) as numVisitors, sum(adRevenue) as adTotal, sum(duration) as spentOnPage
      from uservisits uv
      where uv.destURL LIKE "uh%"
      group by destURL
      order by adTotal desc
      limit 10
    """

    Prelude.timeUnpatched(q)
    Prelude.timePatched(q)
  }

  def q4: Unit = {
    val q = """
      select
        destURL, count(*) as numVisitors, sum(adRevenue) as adTotal, sum(duration) as spentOnPage
      from uservisits uv inner join rankings r on uv.destURL = r.pageURL
      where uv.destURL LIKE "uh%"
      group by destURL
      order by adTotal desc
      limit 10
    """

    Prelude.timeUnpatched(q)
    Prelude.timePatched(q)
  }
}
