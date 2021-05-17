package examples

import geometry.{Rectangle, SpatialMap, Trajectory}
import operators.OperatorSet
import operators.convertion.{Point2SpatialMapConverter, Traj2PointConverter}
import operators.extraction.SpatialMapExtractor
import operators.selection.DefaultSelector
import org.apache.spark.sql.SparkSession
import preprocessing.ReadTrajJson
import utils.Config
import utils.SpatialProcessing.gridPartition
import utils.TimeParsing.parseTemporalRange

import java.lang.System.nanoTime

object SpatialMapDev {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master(Config.get("master"))
      .appName("SpatialMapDev")
      .getOrCreate()

    val trajectoryFile = Config.get("hzData")
    val numPartitions = Config.get("numPartitions").toInt
    val sQuery = Rectangle(args(0).split(",").map(_.toDouble))
    val tQuery = parseTemporalRange(args(1))
    val tStart = tQuery._1
    val tEnd = tQuery._2
    val partitionRange = gridPartition(sQuery.coordinates, args(2).toInt)
    val timeInterval = args(3).toInt
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    /** initialize operators */
    val operator = new OperatorSet {
      override type I = Trajectory
      override type O = SpatialMap[Trajectory]
      override val selector =  new DefaultSelector[I](sQuery, tQuery)
      override val converter = new Traj2PointConverter
      override val extractor = new SpatialMapExtractor[O]
    }

    /** read input data */
    val trajRDD = ReadTrajJson(trajectoryFile, numPartitions)

    /** step 1: Selection */
    val rdd1 = operator.selector.query(trajRDD)
    println(s"--- ${rdd1.count} trajectories")

    /** step 2: Conversion */
    val pointRDD = operator.converter.convert(rdd1)
      .filter(p => p.inside(sQuery) && p.timeStamp._1 >= tStart && p.timeStamp._2 <= tEnd)
    println(s"    <- debug: num of points before conversion: ${pointRDD.count}")

    var t = nanoTime()
    //    println(partitionRange)
    val spatialMapRDD = new Point2SpatialMapConverter(tStart, tEnd, partitionRange, Some(timeInterval)).convert(pointRDD).cache()
    println(s"    <- debug: num of points after conversion: ${spatialMapRDD.flatMap(_.contents).flatMap(x => x._2).count}")
    spatialMapRDD.take(1)
    println(s"... Conversion takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

    //    /** step 3.1: Extraction.GenHeatMap */
    //    t = nanoTime()
    //    //    spatialMapRDD.map(_.printInfo()).foreach(println(_))
    //    val countPerRegion = operator.extractor.countPerRegion(spatialMapRDD, tQuery).collect
    //    println(countPerRegion.map(_._2).sum)
    //    countPerRegion.sortBy(_._2).foreach(println(_))
    //    println(s"... Getting aggregation info takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
    //
    //    t = nanoTime()
    //    val resB = pointRDD.filter(_.temporalOverlap(tQuery)).flatMap(point => {
    //      partitionRange.filter {
    //        case (_, rectangle) => point.inside(rectangle)
    //      }.keys.map((_, 1)).toList
    //    }).reduceByKey(_ + _).map {
    //      case (k, v) => (partitionRange(k), v)
    //    }
    //    println(resB.map(_._2).sum)
    //    resB.collect.sortBy(_._2).foreach(println(_))
    //    println(s"... Benchmark takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

    //    /** step 3.2: Extraction.RangeQuery */
    //    val extractedRDD = operator.extractor.rangeQuery(spatialMapRDD, sQuery, tQuery)
    //    t = nanoTime()
    //    println(s"... Total ${extractedRDD.count} points")
    //    println(s"... Extraction takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
    //
    //    t = nanoTime()
    //    val gt = pointRDD.filter(p => p.inside(sQuery) && p.t <= tQuery._2 && p.t >= tQuery._1).count
    //    println(s"... (Benchmark) Total $gt points")
    //    println(s"... Benchmark takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

    /** step 3.1: Extraction.GenHeatMap */
    t = nanoTime()
    val heatMaps = operator.extractor.genHeatMap(spatialMapRDD).collect.map(x=> (x._1, x._2.toMap))
    println(heatMaps.minBy(_._1._1))
    println(s"... Generating heat maps takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

    val timeSlots = spatialMapRDD.map(_.timeStamp).collect
    t = nanoTime()
    val elementRDD = pointRDD.map(x => {
      val ts = timeSlots.filter(t => x.temporalOverlap(t)).head
      val ss = partitionRange.filter(_._2.intersect(x)).head._1
      (ts, (ss, 1))
    })

    val bc = elementRDD.groupByKey.mapValues(x => x.toArray.groupBy(_._1).map(x => (partitionRange(x._1), x._2.length))).collect
    println(bc.minBy(_._1._1))
    println(s"... Benchmark takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

    sc.stop()
  }


}
