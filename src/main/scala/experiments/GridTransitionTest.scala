package experiments

import instances._
import operatorsNew.converter.Traj2RasterConverter
import operatorsNew.selector.DefaultLegacySelector
import org.apache.spark.sql.SparkSession
import utils.Config

import java.lang.System.nanoTime

object GridTransitionTest {
  case class E(lon: Double, lat: Double, t: Long)

  case class T(id: String, entries: Array[E])

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RegionalSpeedTest")
      .master(Config.get("master"))
      //      .master("local")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val fileName = args(0)
    val spatialRange = args(1).split(",").map(_.toDouble)
    val temporalRange = args(2).split(",").map(_.toLong)
    val numPartitions = args(3).toInt
    val tInterval = args(4).toInt
    // datasets/traj_example_parquet "-8.65, 41.13, -8.57, 41.17" "1380585600,1401580800" 16 86400

    val sQuery = new Extent(spatialRange(0), spatialRange(1), spatialRange(2), spatialRange(3))
    val tQuery = Duration(temporalRange(0), temporalRange(1))

    // read parquet
    val readDs = spark.read.parquet(fileName)
    import spark.implicits._
    val trajRDD = readDs.as[T].rdd.map(x => {
      val entries = x.entries.map(p => (Point(p.lon, p.lat), Duration(p.t), None))
      Trajectory(entries, x.id)
    })

    val selector = new DefaultLegacySelector[Trajectory[None.type, String]](sQuery, tQuery, numPartitions)
    val res = selector.query(trajRDD)

    //    val res = sc.parallelize(Array(
    //      Trajectory(Array(Point(5, 5), Point(5, 10), Point(5, 15)), Array(Duration(0), Duration(1), Duration(3)), Array(None, None, None), "1"),
    //      Trajectory(Array(Point(5, 15), Point(10, 15), Point(15, 15), Point(16, 16)), Array(Duration(0), Duration(1), Duration(3), Duration(4)), Array(None, None, None, None), "1")
    //    ))

    val xArray = (sQuery.xMin until sQuery.xMax by (sQuery.xMax - sQuery.xMin) / 11).sliding(2).toArray
    val yArray = (sQuery.yMin until sQuery.yMax by (sQuery.yMax - sQuery.yMin) / 11).sliding(2).toArray
    val sArray = xArray.flatMap(x => yArray.map(y => (x, y))).map(x => Extent(x._1(0), x._2(0), x._1(1), x._2(1)).toPolygon)
    val tArray = (tQuery.start until tQuery.end by tInterval).sliding(2).map(x => Duration(x(0), x(1))).toArray


    //    val sArray = Array(Extent(0, 0, 10, 10), Extent(10, 0, 20, 10), Extent(10, 10, 20, 20), Extent(0, 10, 10, 20)).map(_.toPolygon)
    //    val tArray = Array(Duration(0, 2), Duration(2, 4), Duration(4, 6))

    val f: Array[Trajectory[None.type, String]] => Array[Trajectory[None.type, String]] = x => x

    val stArray = for {
      s <- sArray
      t <- tArray
    } yield (s, t)

    val converter = new Traj2RasterConverter(f, stArray.map(_._1), stArray.map(_._2))
    val rasterRDD = converter.convert(res)
    //    rasterRDD.collect.foreach(x => println(x.mapValue(x => x.length)))
    rasterRDD.cache()
    rasterRDD.count()

    val t = nanoTime
    val transitionRDD = rasterRDD.map(raster => {
      val newEntries = raster.entries.map(entry => {
        val t = entry.temporal
        val nextT = t.plusSeconds(tInterval, tInterval)
        val content = entry.value.map(traj => {
          val lineStrings = traj.spatialSliding(2).map(x => LineString(x(0), x(1))).toArray
          val durations = traj.temporalSliding(2).map(x => Duration(x(0).start, x(1).end)).toArray
          (lineStrings zip durations).find(_._2.intersects(nextT))
        })
          .filter(_.isDefined).flatMap(e => sArray.zipWithIndex.filter(_._1.intersects(e.get._1)).map(_._2))

        new Entry(entry.spatial, entry.temporal, content)
      })
      new Raster(newEntries, raster.data)
    })


    def vCombine(x: Array[Int], y: Array[Int]): Array[Int] = x ++ y

    def dCombine(x: None.type, y: None.type): None.type = None

    val emptyRaster = Raster.empty[Array[Int]](stArray.map(_._1), stArray.map(_._2)).mapValue(_ => new Array[Int](0))

    val resRDD = transitionRDD.aggregate(emptyRaster)((x, y) => x.merge(y, vCombine, dCombine), (x, y) => x.merge(y, vCombine, dCombine))

    val combined = resRDD
      .mapValue(arr => arr.map((_, 1))
        .groupBy(_._1).map(x => (x._1, x._2.length)))
    val t2 = nanoTime()
    println(combined.entries.filter(_.value.nonEmpty).deep)
    println((t2 - t) * 1e-9)
    sc.stop()
  }
}

