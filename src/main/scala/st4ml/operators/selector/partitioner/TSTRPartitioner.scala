package st4ml.operators.selector.partitioner

import st4ml.instances.{Duration, Event, Extent, Instance}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.collection.mutable
import scala.math.{floor, sqrt}
import scala.reflect.ClassTag

class TSTRPartitioner(tNumPartition: Int,
                      sNumPartition: Int,
                      override var samplingRate: Option[Double] = None,
                      ref: String = "center",
                      sThreshold: Double = 0,
                      tThreshold: Int = 0)
  extends STPartitioner {
  val spark: SparkSession = SparkSession.builder.getOrCreate()
  override val numPartitions: Int = tNumPartition * sNumPartition

  override def partition[T <: Instance[_, _, _] : ClassTag](dataRDD: RDD[T]): RDD[T] = {
    val temporalPartitioner = new TemporalPartitioner(tNumPartition, samplingRate, ref)
    val tPartitionRdd = if (tThreshold == 0) temporalPartitioner.partition(dataRDD)
    else temporalPartitioner.partitionWithOverlap(dataRDD, tThreshold)
    val tRanges = temporalPartitioner.slots.zipWithIndex.map(_.swap).toMap
    val stRanges = mutable.Map.empty[Int, (Duration, Extent)]
    val tSplitRdd = tPartitionRdd.mapPartitionsWithIndex {
      case (idx, iter) => iter.map(x => (idx, x))
    }
    val sr = samplingRate.getOrElse(getSamplingRate(dataRDD))
    for (i <- 0 until tNumPartition) {
      val samples = tSplitRdd.filter(_._1 == i).sample(withReplacement = false, sr).map(_._2).collect
        .map(x => Event(x.spatialCenter, Duration(0))) // the duration is not used, just put 0 to make it instance
      val sRanges = getPartitionRange(samples)
      for (s <- sRanges)
        stRanges += ((i * sNumPartition + s._1) -> (tRanges(i), s._2))
    }
    val idxRDD = dataRDD.map(x => {
      //      val idxs = stRanges.filter(st => x.intersects(st._2._2, st._2._1))
      //      val idx = if (idxs.isEmpty) {
      //        stRanges.filter(st => st._2._1.intersects(x.duration))
      //          .mapValues(st => st._2.distance(x.spatialCenter))
      //          .minBy(_._2)._1
      //      }
      //      else idxs.head._1
      val idxs = stRanges.filter(st => {
        val centroid = x.center
        Event(centroid._1, Duration(centroid._2)).intersects(st._2._2, st._2._1)
      })
      val idx = if (idxs.isEmpty) {
        stRanges.filter(st => st._2._1.intersects(x.duration))
          .mapValues(st => st._2.distance(x.spatialCenter))
          .minBy(_._2)._1
      }
      else idxs.head._1
      (idx, x)
    })
    idxRDD.partitionBy(new KeyPartitioner(numPartitions))
      .map(_._2)
  }

  override def partitionWDup[T <: Instance[_, _, _] : ClassTag](dataRDD: RDD[T]): RDD[T] = {
    val temporalPartitioner = new TemporalPartitioner(tNumPartition, samplingRate, ref)
    val tPartitionRdd = temporalPartitioner.partitionWithOverlap(dataRDD, tThreshold)
    val tRanges = temporalPartitioner.slots.zipWithIndex.map(_.swap).toMap
    val stRanges = mutable.Map.empty[Int, (Duration, Extent)]
    val tSplitRdd = tPartitionRdd.mapPartitionsWithIndex {
      case (idx, iter) => iter.map(x => (idx, x))
    }
    val sr = samplingRate.getOrElse(getSamplingRate(dataRDD))
    for (i <- 0 until tNumPartition) {
      val samples = tSplitRdd.filter(_._1 == i).sample(withReplacement = false, sr).map(_._2).collect
      val sRanges = getPartitionRange(samples)
      for (s <- sRanges)
        stRanges += ((i * sNumPartition + s._1) -> (tRanges(i), s._2))
    }
//    stRanges.toArray.sortBy(_._1).foreach(println)
    val idxRDD = dataRDD.flatMap(x => {
      stRanges.filter(st => x.intersects(st._2._2, st._2._1)).keys
        .map(i => (i, x))
    })
    idxRDD.partitionBy(new KeyPartitioner(numPartitions))
      .map(_._2)
  }

  def getPartitionRange[T <: Instance[_, _, _] : ClassTag](instances: Array[T]): Map[Int, Extent] = {
    def getBoundary(df: DataFrame, n: Int, column: String): Array[Double] = {
      val interval = 1.0 / n
      var t: Double = 0
      var q = new Array[Double](0)
      while (t < 1 - interval) {
        t += interval
        q = q :+ t
      }
      q = 0.0 +: q
      if (q.length < n + 1) q = q :+ 1.0
      df.stat.approxQuantile(column, q, 0.001)
    }

    def getStrip(df: DataFrame, range: List[Double], column: String): DataFrame = {
      df.filter(functions.col(column) >= range.head && functions.col(column) < range(1))
    }

    def genBoxes(x_boundaries: Array[Double], y_boundaries: Array[Array[Double]]):
    (Array[List[Double]], Map[List[Double], Array[(List[Double], Int)]]) = {
      var metaBoxes = new Array[List[Double]](0)
      for (x <- 0 to x_boundaries.length - 2) metaBoxes = metaBoxes :+
        List(x_boundaries(x), y_boundaries(x)(0), x_boundaries(x + 1), y_boundaries(0).last)
      var boxMap: Map[List[Double], Array[(List[Double], Int)]] = Map()
      var boxes = new Array[List[Double]](0)
      var n = 0
      for (i <- 0 to x_boundaries.length - 2) {
        var stripBoxes = new Array[(List[Double], Int)](0)
        val x_min = x_boundaries(i)
        val x_max = x_boundaries(i + 1)
        for (j <- 0 to y_boundaries(i).length - 2) {
          val y_min = y_boundaries(i)(j)
          val y_max = y_boundaries(i)(j + 1)
          val box = List(x_min, y_min, x_max, y_max)
          boxes = boxes :+ box
          stripBoxes = stripBoxes :+ (box, n)
          n += 1
        }
        boxMap += metaBoxes(i) -> stripBoxes
      }
      (boxes, boxMap)
    }

    def getWholeRange(df: DataFrame, column: List[String]): Array[Double] = {
      implicit def toDouble: Any => Double = {
        case i: Int => i
        case f: Float => f
        case d: Double => d
      }

      val x_min = df.select(functions.min(column.head)).collect()(0)(0)
      val x_max = df.select(functions.max(column.head)).collect()(0)(0)
      val y_min = df.select(functions.min(column(1))).collect()(0)(0)
      val y_max = df.select(functions.max(column(1))).collect()(0)(0)
      val x_border = (x_max - x_min) * 0.001
      val y_border = (y_max - y_min) * 0.001
      Array(x_min - x_border, y_min - y_border, x_max + x_border, y_max + y_border)
    }

    def replaceBoundary(x_boundaries: Array[Double], y_boundaries: Array[Array[Double]], wholeRange: Array[Double]):
    (Array[Double], Array[Array[Double]]) = {
      val n_x_boundaries = x_boundaries
      var n_y_boundaries = new Array[Array[Double]](0)
      n_x_boundaries(0) = wholeRange(0)
      n_x_boundaries(n_x_boundaries.length - 1) = wholeRange(2)
      for (y <- y_boundaries) {
        val n_y_boundary = wholeRange(1) +: y.slice(1, y.length - 1) :+ wholeRange(3)
        n_y_boundaries = n_y_boundaries :+ n_y_boundary
      }
      (n_x_boundaries, n_y_boundaries)
    }

    def STR(df: DataFrame, columns: List[String], coverWholeRange: Boolean):
    (Array[List[Double]], Map[List[Double], Array[(List[Double], Int)]]) = {
      // columns: sequence of partitioning columns, e.g. List("x", "y") means partition on x first then y
      // return boxes
      val s = floor(sqrt(sNumPartition)).toInt
      val n = floor(sNumPartition / s.toDouble).toInt
      var x_boundaries = getBoundary(df, s, columns.head)
      assert(s * n <= sNumPartition)
      var y_boundaries = new Array[Array[Double]](0)
      for (i <- 0 to x_boundaries.length - 2) {
        val range = List(x_boundaries(i), x_boundaries(i + 1))
        val stripRDD = getStrip(df, range, columns.head)
        val y_boundary = getBoundary(stripRDD, n, columns(1))
        y_boundaries = y_boundaries :+ y_boundary
      }
      if (coverWholeRange) {
        val wholeRange = getWholeRange(df, List("x", "y"))
        val new_boundaries = replaceBoundary(x_boundaries, y_boundaries, wholeRange)
        x_boundaries = new_boundaries._1
        y_boundaries = new_boundaries._2
      }
      genBoxes(x_boundaries, y_boundaries)
    }

    val xy = instances.map(x => (x.spatialCenter.getX, x.spatialCenter.getY))
    val df = spark.createDataFrame(xy).toDF("x", "y").cache()
    val res = STR(df, List("x", "y"), coverWholeRange = true)
    val boxes = res._1
    var boxesWIthID: Map[Int, Extent] = Map()
    for (i <- boxes.indices) {
      val lonMin = boxes(i).head
      val latMin = boxes(i)(1)
      val lonMax = boxes(i)(2)
      val latMax = boxes(i)(3)
      boxesWIthID += (i -> Extent(lonMin, latMin, lonMax, latMax))
    }
    if (sThreshold != 0) boxesWIthID.mapValues(rectangle => rectangle.expandBy(sThreshold)).map(identity)
    else boxesWIthID
  }
}

object TSTRPartitioner {
  def apply(numPartition: Int, samplingRate: Option[Double] = None, ref: String = "center", threshold: Double = 0): TSTRPartitioner =
    new TSTRPartitioner(sqrt(numPartition).toInt, numPartition / sqrt(numPartition).toInt, samplingRate, ref, threshold)
}