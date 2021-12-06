package examples

import instances.{Duration, Event, Extent, Point, TimeSeries}
import operatorsNew.converter.Event2TimeSeriesConverter
import operatorsNew.selector.MultiSTRangeLegacySelector
import operatorsNew.selector.partitioner.STRPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.Config

/**
 * divide the whole spatial range into grids and find how many points inside each grid
 * for every hour
 */
object flowTs {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("flowExp")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    /**
     * "C:\\Users\\kaiqi001\\Documents\\GitHub\\geomesa-fs_2.12-3.2.0\\face-point\\09_W964092771efa4a4b87c1c75d5c79d6ec.parquet" "-9.137,38.715,-7.740,41.523" "1372639359,1372755736" "5" "3600"
     */
    val sQuery = args(1).split(",").map(_.toDouble)
    val tQuery = args(2).split(",").map(_.toLong)
    val sSize = args(3).toInt
    val tSplit = args(4).toInt
    val grids = genGrids(sQuery, sSize)
    val stGrids = genSTGrids(grids, (tQuery(0), tQuery(1)), tSplit)
    val pointFile = args(0)

    val pointRDD = readGeoMesaParquet(pointFile)

    //    val pointRDD = ReadParquet.ReadFaceParquet(pointFile)

    //    val countRDD = pointRDD.map(p => (utils.TimeParsing.getDate(p.timeStamp._1), 1)).reduceByKey(_ + _)
    //    println(countRDD.collect.sortBy(_._1).deep)


    val selector = new MultiSTRangeLegacySelector[Event[Point, None.type, String]](stGrids.map(x => new Extent(x._1(0), x._1(1), x._1(2), x._1(3)).toPolygon),
      stGrids.map(x => Duration(x._2(0), x._2(1))),
      numPartitions = grids.length)

    val rdd1 = selector.query(pointRDD)

    val partitioner = new STRPartitioner(numPartitions = sSize * sSize)
    val partitionMap = grids.map(x => Extent(x(0), x(1), x(2), x(3))).zipWithIndex.map(_.swap).toMap
    val pRdd1 = partitioner.partition(rdd1, partitionMap)

    val f: Array[Event[Point, None.type, String]] => Int = _.length
    val tArray = ((tQuery(0) until tQuery(1) by tSplit.toLong).toArray :+ tQuery(1)).sliding(2).toArray.map(x => Duration(x(0), x(1)))
    val converter = new Event2TimeSeriesConverter(tArray)
    val rdd2 = converter.convert(pRdd1,f)
    val resRdd = rdd2.map(x => x.entries.map(_.value).zipWithIndex.map(_.swap)).zipWithIndex().flatMap(x => x._1.map(y => (y, x._2)))
    resRdd.collect.foreach(x => println(grids(x._2.toInt).mkString("Array(", ", ", ")"), tArray(x._1._1), x._1._2))
    println(resRdd.map(_._1._2).sum)


    sc.stop()
  }

  def genGrids(range: Array[Double], size: Int): Array[Array[Double]] = {
    val lonMin = range(0)
    val latMin = range(1)
    val lonMax = range(2)
    val latMax = range(3)
    val lons = ((lonMin until lonMax by (lonMax - lonMin) / size) :+ lonMax).sliding(2).toArray
    val lats = ((latMin until latMax by (latMax - latMin) / size) :+ latMax).sliding(2).toArray
    lons.flatMap(x => lats.map(y => Array(x(0), y(0), x(1), y(1))))
  }

  def genSTGrids(grids: Array[Array[Double]], tRange: (Long, Long), tSplit: Int): Array[(Array[Double], Array[Long])] = {
    val tSlots = ((tRange._1 until tRange._2 by tSplit.toLong).toArray :+ tRange._2).sliding(2).toArray
    grids.flatMap(grid => tSlots.map(t => (grid, t)))
  }

  def readGeoMesaParquet(file: String): RDD[Event[Point, None.type, String]] = {
    val spark = SparkSession.builder().getOrCreate()
    val gmDf = spark.read.parquet(file)
    val pointDf = gmDf.select("fid", "timestamp", "geom.x", "geom.y")
    val pointRDD = pointDf.rdd.map(row => {
      val id = row.getString(0)
      val t = row.getLong(1)
      val x = row.getDouble(2)
      val y = row.getDouble(3)
      Event(s = Point(x, y), t = Duration(t), d = id)
    })
    pointRDD
  }
}

