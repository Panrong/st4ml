package examples

import operators.convertion.Converter
import operators.extraction.PointsAnalysisExtractor
import geometry.Rectangle
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import preprocessing.ReadTrajJson
import operators.selection.partitioner.HashPartitioner
import operators.selection.selectionHandler.{RTreeHandler, TemporalSelector}

import java.lang.System.nanoTime
import java.text.SimpleDateFormat
import java.util.Date
import scala.io.Source
import scala.math.{max, sqrt}

object PointAnalysisTest {
  def main(args: Array[String]): Unit = {
    var t = nanoTime()
    /** set up Spark environment */
    var config: Map[String, String] = Map()
    val f = Source.fromFile("config")
    f.getLines
      .filterNot(_.startsWith("//"))
      .filterNot(_.startsWith("\n"))
      .foreach(l => {
        val p = l.split(" ")
        config = config + (p(0) -> p(1))
      })
    f.close()
    val spark = SparkSession
      .builder()
      .master(config("master"))
      .appName(config("appName"))
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val trajectoryFile = args(0)
    val numPartitions = args(1).toInt

    val trajRDD = ReadTrajJson(trajectoryFile, numPartitions)
      .persist(StorageLevel.MEMORY_AND_DISK)
    val sQuery = Rectangle(Array(118.116, 29.061, 120.167, 30.184))

    val tQuery = (1597000000L, 1598000000L)
    val rTreeCapacity = max(sqrt(trajRDD.count / numPartitions).toInt, 100)

    println("\n*-*-*-*-*-*-*-*-*-*-*-*")

    /** benchmark */
    t = nanoTime()
    val fullSRDD = trajRDD.filter(x => x.intersect(sQuery))
    println(s"*- Full scan S: ${fullSRDD.count} -*")
    val fullSTRDD = fullSRDD.filter(x => {
      val (ts, te) = x.timeStamp
      ts <= tQuery._2 && ts >= tQuery._1 || te <= tQuery._2 && te >= tQuery._1
    })
    println(s"*- Full scan ST: ${fullSTRDD.count} -*")
    println("*-*-*-*-*-*-*-*-*-*-*-*")
    println(s"... Full scanning takes ${(nanoTime() - t) * 1e-9} s.\n")

    /** test hash partitioner */
    println("==== HASH ====")

    t = nanoTime()
    val hashPartitioner = new HashPartitioner(numPartitions)
    val pRDDHash = hashPartitioner.partition(trajRDD).cache()
    val partitionRangeHash = hashPartitioner.partitionRange

    val selectorHash = new RTreeHandler(partitionRangeHash, Some(rTreeCapacity))
    pRDDHash.count()
    println(s"... Partitioning takes ${(nanoTime() - t) * 1e-9} s.")

    t = nanoTime()
    val queriedRDD2Hash = selectorHash.query(pRDDHash)(sQuery).cache()
    println(s"==== Queried dataset contains ${queriedRDD2Hash.count} entries (RTree)")
    println(s"... Querying with index takes ${(nanoTime() - t) * 1e-9} s.")

    t = nanoTime()
    val temporalSelectorH = new TemporalSelector
    val queriedRDD3Hash = temporalSelectorH.query(queriedRDD2Hash)(tQuery)
    println(s"==== Queried dataset contains ${queriedRDD3Hash.count} entries (ST)")
    println(s"... Temporal querying takes ${(nanoTime() - t) * 1e-9} s.")

    println("\n==== Start Conversion")
    t = nanoTime()
    val convertor = new Converter()
    val pointRDD = convertor.traj2Point(queriedRDD3Hash)

    println(s"==== converted to ${pointRDD.count} points")
    println(s"... Conversion takes ${(nanoTime() - t) * 1e-9} s.")

    println("\n==== Start Extraction")
    t = nanoTime()
    val extractor = new PointsAnalysisExtractor()
    val filteredPointRDD = pointRDD.filter(x => x.coordinates(0) > 115 && x.coordinates(1) > 25 && x.coordinates(0) < 125 && x.coordinates(1) < 35)
    println(" ... Top 5 most frequent:")
    extractor.extractMostFrequentPoints("tripID", 5)(pointRDD).foreach(x => println(s" ....  $x"))
    println(s" ... Spatial range: ${extractor.extractSpatialRange(filteredPointRDD).mkString("(", ", ", ")")}")
    val temporalRange = extractor.extractTemporalRange(filteredPointRDD)
    println(s" ... Temporal range: ${temporalRange.mkString("(", ", ", ")")}")
    println(s" ...               : ${temporalRange.map(timeLong2String).mkString("(", ", ", ")")}")

    val temporalMedian = extractor.extractTemporalQuantile(0.5)(filteredPointRDD).toLong
    println(s" ... Temporal median (approx.): " +
      s"$temporalMedian (${timeLong2String(temporalMedian)})")
    val temporalQuantiles = extractor.extractTemporalQuantile(Array(0.25, 0.75))(filteredPointRDD).map(_.toLong)
    println(s" ... Temporal 25% and 75% (approx.): " +
      s"${temporalQuantiles.mkString(", ")}")
    println(s" ...                               : " +
      s"${temporalQuantiles.map(timeLong2String).mkString(", ")}")
    val newMoveIn = extractor.extractNewMoveIn(1598176021, 10)(filteredPointRDD)
    println(s" ... Number of new move-ins after time ${timeLong2String(1598176021)} : ${newMoveIn.length}")

    val pr = extractor.extractPermanentResidents((1596882269, 1598888976), 200)(filteredPointRDD)
    println(s" ... Number of permanent residences : ${pr.length}")

    val abnormity = extractor.extractAbnormity()(filteredPointRDD)
    println(s" ... Number of abnormal ids : ${abnormity.length}")

    println(s"... All extractions takes ${(nanoTime() - t) * 1e-9} s.")

    sc.stop()
  }

  def timeLong2String(tm: Long): String = {
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val tim = fm.format(new Date(tm * 1000))
    tim
  }
}
