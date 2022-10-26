package experiments

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import st4ml.operators.selector.SelectionUtils.E

object GenToyDataSet {
  case class TE(shape: String, timestamp: Long, map: Map[String, String])

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("gentoydata")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val dataType = args(0)
    val size = args(1).toInt // may only get rough number
    val dataDir = args(2)
    val res = args(3)

    import spark.implicits._

    if (dataType == "event") {
      val events = spark.read.parquet(dataDir).as[E]
      println(events.count())
      val samplingRate = size / events.count.toDouble
      val sampledEvents = events.sample(samplingRate)
      println(sampledEvents.count)
      val ds = sampledEvents.map { e =>
        val shape = e.shape
        val timestamp = e.timeStamp(0)
        val map = e.d.drop(4).dropRight(1).split(",").map(x => (x.split("->").head.stripMargin, x.split("->").last.stripMargin)).toMap
        TE(shape, timestamp, map)
      }
      var df = ds.toDF("shape", "timestamp", "map")
      for (k <- ds.take(1).head.map.keys) {
        df = df.withColumn(k, col("map").getItem(k))
      }
      df = df.drop("map")
      df.show(2,false)
      df.write.option("header", true).csv(res)
    }
    else if (dataType == "traj") {

    }
    else if (dataType == "map") {

    }
    else throw new Exception("Currently only support event, traj and map")

    sc.stop()
  }
}
