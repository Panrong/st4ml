package experiments

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, map}
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import st4ml.operators.selector.SelectionUtils.E

object GenToyDataSet {
  case class TE(shape: String, timestamp: Long, map: Map[String, String])

  case class TT(shape: String, timestamps: String, TRIP_ID: String, CALL_TYPE: String, ORIGIN_CALL: String,
                ORIGIN_STAND: String, TAXI_ID: String, DAY_TYPE: String, MISSING_DATA: String)

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
      df.show(2, false)
      df.write.option("header", true).csv(res)
    }
    else if (dataType == "traj") {
      import spark.implicits._
      val trajs = spark.read.option("header", true).csv(dataDir)
      println(trajs.count())
      val ds = trajs.rdd.filter(x => x.getString(8).dropRight(1).drop(1).split("],").map(i => i.replace("[", "").replace("]", "").stripMargin).length > 1)
      val resDf = ds.map { x =>
        val gf = new GeometryFactory()
        val points = x.getString(8).dropRight(1).drop(1).split("],").map(i => i.replace("[", "").replace("]", "").stripMargin)
          .map { x =>
            val c = x.split(",").map(_.toDouble)
            new Coordinate(c(0), c(1))
          }
        val timestamps = points.indices.map(i => i * 15 + x.getString(5).toLong).toArray
        val shape = gf.createLineString(points).toString
        val TRIP_ID = x.getString(0)
        val CALL_TYPE = x.getString(1)
        val ORIGIN_CALL = x.getString(2)
        val ORIGIN_STAND = x.getString(3)
        val TAXI_ID = x.getString(4)
        val DAY_TYPE = x.getString(6)
        val MISSING_DATA = x.getString(7)
        TT(shape, timestamps.mkString(", "), TRIP_ID, CALL_TYPE, ORIGIN_CALL,
          ORIGIN_STAND, TAXI_ID, DAY_TYPE, MISSING_DATA)
      }.toDS
      resDf.show(2, false)
      resDf.write.option("header", true).csv(res)
      //      val samplingRate = size / events.count.toDouble
      //      val sampledEvents = events.sample(samplingRate)
      //      println(sampledEvents.count)
      //      val ds = sampledEvents.map { e =>
      //        val shape = e.shape
      //        val timestamp = e.timeStamp(0)
      //        val map = e.d.drop(4).dropRight(1).split(",").map(x => (x.split("->").head.stripMargin, x.split("->").last.stripMargin)).toMap
      //        TE(shape, timestamp, map)
      //      }
      //      var df = ds.toDF("shape", "timestamp", "map")
      //      for (k <- ds.take(1).head.map.keys) {
      //        df = df.withColumn(k, col("map").getItem(k))
      //      }
      //      df = df.drop("map")
      //      df.show(2,false)
      //      df.write.option("header", true).csv(res)
    }
    else if (dataType == "map") {

    }
    else if (dataType == "raster") {
      val lonMin = -8.7
      val lonMax = -7.5
      val latMin = 41
      val latMax = 41.5
      val tMin = 1408032143
      val tMax = 1419172198
      val sSplit = 5
      val tSplit = 864000
      val cells = for (lon <- (BigDecimal(lonMin) to BigDecimal(lonMax) by (lonMax - lonMin) / sSplit).sliding(2);
                       lat <- (BigDecimal(latMin) to BigDecimal(latMax) by (latMax - latMin) / sSplit).sliding(2);
                       t <- Range(tMin, tMax, tSplit).sliding(2)
                       ) yield (lon, lat, t)
      val arr = cells.toArray.map(x => (x._1(0).formatted("%.4f"), x._2(0).formatted("%.4f"), x._1(1).formatted("%.4f"),
        x._2(1).formatted("%.4f"), x._3(0), x._3(1)))
      val df = spark.createDataFrame(arr)
      println(df.count)
      df.show(2)
      df.coalesce(1).write.csv("raster")
    }
    else throw new Exception("Currently only support event, traj and map")

    sc.stop()
  }
}
