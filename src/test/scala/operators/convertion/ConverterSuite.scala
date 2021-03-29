package operators.convertion

import geometry.Rectangle
import geometry.road.RoadGrid
import operators.extraction.PointsAnalysisExtractor
import operators.selection.partitioner.{FastPartitioner, HashPartitioner, QuadTreePartitioner}
import operators.selection.selectionHandler.RTreeHandler
import org.scalatest.funsuite.AnyFunSuite
import preprocessing.{ReadTrajFile, ReadTrajJson}
import setup.SharedSparkSession

class ConverterSuite extends AnyFunSuite with SharedSparkSession {

  test("test traj2point") {
    val trajRDD = ReadTrajJson("datasets/traj_100000_converted.json", 4)
    val sQuery = Rectangle(Array(115, 25, 125, 35))

    val partitioner = new HashPartitioner(4)
    val pRDD = partitioner.partition(trajRDD).cache()
    val partitionRange = partitioner.partitionRange
    val selector = RTreeHandler(partitionRange, Some(500))
    val queriedRDD = selector.query(pRDD)(sQuery)
    println(s"--- ${queriedRDD.count} trajectories")

    val converter = new Converter()
    val pointRDD = converter.traj2Point(queriedRDD)

    println(s"--- converted to ${pointRDD.count} points")

    val extractor = new PointsAnalysisExtractor()
    val filteredPointRDD = pointRDD.filter(x => x.coordinates(0) > 115 && x.coordinates(1) > 25 && x.coordinates(0) < 125 && x.coordinates(1) < 35)
    println(" ... Top 5 most frequent:")
    extractor.extractMostFrequentPoints("tripID", 5)(pointRDD).foreach(x => println(s" ....  $x"))
    println(s" ... Spatial range: ${extractor.extractSpatialRange(filteredPointRDD).mkString("Array(", ", ", ")")}")
    println(s" ... Temporal range: ${extractor.extractTemporalRange(filteredPointRDD).mkString("Array(", ", ", ")")}")

    println(s" ... Temporal median (approx.): " +
      s"${extractor.extractTemporalQuantile(0.5)(filteredPointRDD).toLong}")
    println(s" ... Temporal 25% and 75% (approx.): " +
      s"${extractor.extractTemporalQuantile(Array(0.25, 0.75))(filteredPointRDD).map(_.toLong).mkString(", ")}")

    val newMoveIn = extractor.extractNewMoveIn(1598176021, 10)(filteredPointRDD)
    println(s" ... Number of new move-ins after time 1598176021 : ${newMoveIn.length}")

    val pr = extractor.extractPermanentResidents((1596882269, 1598888976), 200)(filteredPointRDD)
    println(s" ... Number of permanent residences : ${pr.length}")

    val abnormity = extractor.extractAbnormity()(filteredPointRDD)
    println(s" ... Number of abnormal ids : ${abnormity.length}")

  }
  test("test point to trajectory") {
    val trajRDD = ReadTrajJson("datasets/traj_100000_converted.json", 8)
    println(trajRDD.count)
    println(s"Average length:  ${trajRDD.map(_.points.length).aggregate(0)(_ + _, _ + _) / trajRDD.count}")
    val converter = new Converter
    val pointRDD = converter.traj2Point(trajRDD.map((0, _)))
    val convertedTrajRDD = converter.point2Traj(pointRDD.map((0, _)))
    println(convertedTrajRDD.count)
    println(s"Average length:  ${convertedTrajRDD.map(_.points.length).aggregate(0)(_ + _, _ + _) / trajRDD.count}")
  }

  test("test point to spatial map") {
    val trajRDD = ReadTrajFile("preprocessing/traj_short.csv", 10, 16, limit = true)
    val sQuery = Rectangle(Array(-9, 40, -8, 42))

    val partitioner = new FastPartitioner(16)
    val pRDD = partitioner.partition(trajRDD)
    val partitionRange = partitioner.partitionRange
    val selector = RTreeHandler(partitionRange, Some(500))
    val queriedRDD = selector.query(pRDD)(sQuery)
    println(s"--- ${queriedRDD.count} trajectories")

    val converter = new Converter()
    val pointRDD = converter.traj2Point(queriedRDD)
      .zipWithUniqueId()
      .map(x => x._1.setID(x._2.toString))
    println(s"--- converted to ${pointRDD.count} points")

    val smRDD = converter.point2RoadMap(pointRDD.map((0, _)), RoadGrid("preprocessing/porto.csv"))
    smRDD.take(5).foreach(x =>
      println(x.roadID, x.attributes.length))
    assert(smRDD.map(x => x.attributes.length).reduce(_ + _) == pointRDD.count())
  }

  //  currently not working
  //  test("test traj to time series") {
  //    val spark = SparkSession.builder().master("local").getOrCreate()
  //    val sc = spark.sparkContext
  //    sc.setLogLevel("ERROR")
  //    val trajRDD = ReadTrajFile("preprocessing/traj_short.csv", 10, 16, limit = true)
  //    val sQuery = Rectangle(Array(-9, 40, -8, 42))
  //
  //    val partitioner = new FastPartitioner(16)
  //    val pRDD = partitioner.partition(trajRDD)
  //    val partitionRange = partitioner.partitionRange
  //    val selector = RTreeHandler(partitionRange, Some(500))
  //    val queriedRDD = selector.query(pRDD)(sQuery)
  //    println(s"--- ${queriedRDD.count} trajectories")
  //
  //    val converter = new Converter()
  //    val timeSeriesRDD = converter.traj2TimeSeries(queriedRDD, 1370000000, 15*60*1000)
  //    timeSeriesRDD.take(5).foreach(x => println(x))
  //  }

  test("test point to time series and time series to point") {
    val trajRDD = ReadTrajFile("preprocessing/traj_short.csv", 10, 16, limit = true)
    val sQuery = Rectangle(Array(-9, 40, -8, 42))

    val partitioner = new FastPartitioner(8)
    val pRDD = partitioner.partition(trajRDD)
    val partitionRange = partitioner.partitionRange
    val selector = RTreeHandler(partitionRange, Some(500))
    val queriedRDD = selector.query(pRDD)(sQuery)
    println(s"--- ${queriedRDD.count} trajectories")

    val converter = new Converter()

    val pointRDD = converter.traj2Point(queriedRDD).map((0, _))

    val tsRDD = converter.point2TimeSeries(pointRDD, 1372636854, 100, new QuadTreePartitioner(8, samplingRate = Some(1)))
    println(tsRDD.count)
    tsRDD.take(5).foreach(x => println(s"${x.id}, ${x.startTime}, ${x.timeInterval}, ${x.series.map(i => i.map(j => j.t)).deep}"))
    assert(tsRDD.map(_.series.flatten.length).reduce(_ + _) == pointRDD.count)
    val convertBack = converter.timeSeries2Point(tsRDD.map((0, _))).count
    assert(convertBack == pointRDD.count)
  }

  test("test Point to SpatialMap and SpatialMap to Point") {
    val trajRDD = ReadTrajFile("preprocessing/traj_short.csv", 100, 16, limit = true)
    val sQuery = Rectangle(Array(-180, -180, 180, 180))

    val partitioner = new QuadTreePartitioner(8, Some(0.5))
    val pRDD = partitioner.partition(trajRDD)
    val partitionRange = partitioner.partitionRange
    val selector = RTreeHandler(partitionRange, Some(500))
    val queriedRDD = selector.query(pRDD)(sQuery)
    println(s"--- ${queriedRDD.count} trajectories")

    val converter = new Converter()

    val pointRDD = converter.traj2Point(queriedRDD).map((0, _))
    val tStart = pointRDD.map(_._2.timeStamp).min._1
    val tEnd = pointRDD.map(_._2.timeStamp).max._2
    val spatialMapRDD = converter.point2SpatialMap(pointRDD, tStart, tEnd, partitionRange)
    val totalPoints = spatialMapRDD.flatMap(sm => sm.contents).map(_._2.length).reduce(_ + _)
    assert(totalPoints == pointRDD.count)
    val convertedBack = converter.spatialMap2Point(spatialMapRDD.map((0, _))).count
    assert(convertedBack == totalPoints)
  }

  test("test spatial map to raster") {
    val trajRDD = ReadTrajFile("preprocessing/traj_short.csv", 100, 16, limit = true)
    val sQuery = Rectangle(Array(-180, -180, 180, 180))

    val partitioner = new QuadTreePartitioner(8, Some(0.5))
    val pRDD = partitioner.partition(trajRDD)
    val partitionRange = partitioner.partitionRange
    val selector = RTreeHandler(partitionRange, Some(500))
    val queriedRDD = selector.query(pRDD)(sQuery)
    println(s"--- ${queriedRDD.count} trajectories")

    val converter = new Converter()

    val pointRDD = converter.traj2Point(queriedRDD).map((0, _))
    val tStart = pointRDD.map(_._2.timeStamp).min._1
    val tEnd = pointRDD.map(_._2.timeStamp).max._2
    val spatialMapRDD = converter.point2SpatialMap(pointRDD, tStart, tEnd, partitionRange)
    val totalPoints = spatialMapRDD.flatMap(sm => sm.contents).map(_._2.length).reduce(_ + _)
    assert(totalPoints == pointRDD.count)
    val convertedBack = converter.spatialMap2Point(spatialMapRDD.map((0, _))).count
    assert(convertedBack == totalPoints)

    val rasterRDD = converter.spatialMap2Raster(spatialMapRDD.map((0, _)))
    for (raster <- rasterRDD.collect) {
      //      println("Raster : " + raster.id)
      //      println(" spatial range: " + raster.spatialRange())
      //      println(" temporal range: " + raster.temporalRange())
      //      println(" contents" + raster.contents.deep)
      assert(raster.contents.length == 1)
      assert(raster.contents.head.spatialRange.area == raster.spatialRange().area)
      assert(raster.contents.head.temporalRange == raster.temporalRange())
    }
    val convertedTSRDD = rasterRDD.flatMap(x => x.contents)
    val convertBack = converter.timeSeries2Point(convertedTSRDD.map((0, _))).count
    assert(convertBack == totalPoints)
  }

  test("test time series to raster") {
    val trajRDD = ReadTrajFile("preprocessing/traj_short.csv", 10, 16, limit = true)
    val sQuery = Rectangle(Array(-9, 40, -8, 42))

    val partitioner = new FastPartitioner(8)
    val pRDD = partitioner.partition(trajRDD)
    val partitionRange = partitioner.partitionRange
    val selector = RTreeHandler(partitionRange, Some(500))
    val queriedRDD = selector.query(pRDD)(sQuery)
    println(s"--- ${queriedRDD.count} trajectories")

    val converter = new Converter()

    val pointRDD = converter.traj2Point(queriedRDD).map((0, _))

    val tsRDD = converter.point2TimeSeries(pointRDD, 1372636854, 100, new QuadTreePartitioner(8, samplingRate = Some(1)))
    println(tsRDD.count)
    tsRDD.take(5).foreach(x => println(s"${x.id}, ${x.startTime}, ${x.timeInterval}, ${x.series.map(i => i.map(j => j.t)).deep}"))
    assert(tsRDD.map(_.series.flatten.length).reduce(_ + _) == pointRDD.count)
    val convertBack = converter.timeSeries2Point(tsRDD.map((0, _))).count
    assert(convertBack == pointRDD.count)

    val rasterRDD = converter.timeSeries2Raster(tsRDD.map((0, _)), 100)
    for (raster <- rasterRDD.collect) {
      //      println("Raster : " + raster.id)
      //      println(" spatial range: " + raster.spatialRange())
      //      println(" temporal range: " + raster.temporalRange())
      //      println(" contents" + raster.contents.deep)
      assert(raster.contents.length == 1)
      assert(raster.contents.head.spatialRange.area == raster.spatialRange().area)
      assert(raster.contents.head.temporalRange == raster.temporalRange())
    }
    val convertedTSRDD = rasterRDD.flatMap(x => x.contents)
    val convertBack2 = converter.timeSeries2Point(convertedTSRDD.map((0, _))).count
    assert(convertBack2 == pointRDD.count)
  }
}
