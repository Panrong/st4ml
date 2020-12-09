package examples

import org.apache.spark.sql.SparkSession

object RangeQuery {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder()
      .master("local[4]")
      .appName("RangeQuery")
      .config("simba.join.partitions", "20")
      .getOrCreate()

    runRangeQuery(ss)


    ss.stop()
  }

  private def runRangeQuery(stt: SparkSession): Unit = ???

}
