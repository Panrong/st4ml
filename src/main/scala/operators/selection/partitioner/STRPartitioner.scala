package operators.selection.partitioner

import geometry.{Point, Rectangle, Shape}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import scala.math.{floor, max, min, sqrt}
import scala.reflect.ClassTag

class STRPartitioner(numPartitions: Int,
                     override var samplingRate: Option[Double] = None,
                     threshold: Double = 0)
  extends SpatialPartitioner {
  var partitionRange: Map[Int, Rectangle] = Map()

  /**
   * Generate ranges using STR for partitioning
   *
   * @param dataRDD : data RDD
   * @tparam T : type of spatial dataRDD, extending geometry.Shape
   * @return : map of partitionID --> rectangle
   */

  def getPartitionRange[T <: Shape : ClassTag](dataRDD: RDD[T]): Map[Int, Rectangle] = {
    def getBoundary(df: DataFrame, n: Int, column: String): List[Double] = {
      val q = (BigDecimal(0.0) to BigDecimal(1.0) by 1 / n.toDouble).toArray.map(_.toDouble)
      df.stat.approxQuantile(column, q, 0.001).toList
    }

    def getStrip(df: DataFrame, range: List[Double], column: String): DataFrame = {
      df.filter(functions.col(column) >= range.head && functions.col(column) < range(1))
    }

    def genBoxes(xBoundaries: List[Double], yBoundaries: Array[List[Double]]):
    (Array[List[Double]], Map[List[Double], Array[(List[Double], Int)]]) = {
      val metaBoxes = xBoundaries.sliding(2).zip(yBoundaries.toIterator).map {
        case (List(x1, x2), y) => List(x1, y.head, x2, y.last)
      }.toArray
      val xPairs = xBoundaries.sliding(2)
      val yPairs = yBoundaries.map(_.sliding(2)).toIterator
      val boxMap = xPairs.zip(yPairs).map {
        case (x, y) => y.map(yPrime => List(x.head, yPrime.head, x(1), yPrime(1)))
      }.map(_.toArray.zipWithIndex).toArray
        .zip(metaBoxes)
        .map(_.swap)
        .toMap
      val boxes = boxMap.values.toArray.flatMap(x => x.map(_._1))
      (boxes, boxMap)
    }

    def getWholeRange(df: DataFrame, column: List[String]): List[Double] = {
      implicit def toDouble: Any => Double = {
        case i: Int => i
        case f: Float => f
        case d: Double => d
      }

      val xMin = df.select(functions.min(column.head)).collect()(0)(0)
      val xMax = df.select(functions.max(column.head)).collect()(0)(0)
      val yMin = df.select(functions.min(column(1))).collect()(0)(0)
      val yMax = df.select(functions.max(column(1))).collect()(0)(0)
      val xBorder = (xMax - xMin) * 0.001
      val yBorder = (yMax - yMin) * 0.001
      List(xMin - xBorder, yMin - yBorder, xMax + xBorder, yMax + yBorder)
    }

    def replaceBoundary(xBoundaries: List[Double], yBoundaries: Array[List[Double]],
                        wholeRange: List[Double]):
    (List[Double], Array[List[Double]]) = {
      val xBoundariesNew = wholeRange.head +:
        xBoundaries.drop(1).dropRight(1) :+ wholeRange(2)
      val yBoundariesNew = yBoundaries.map(y =>
        wholeRange(1) +: y.drop(1).dropRight(1) :+ wholeRange(3))
      (xBoundariesNew, yBoundariesNew)
    }

    def STR(df: DataFrame, columns: List[String], coverWholeRange: Boolean):
    (Array[List[Double]], Map[List[Double], Array[(List[Double], Int)]]) = {
      // columns: sequence of partitioning columns, e.g. List("x", "y") means partition on x first then y
      // return boxes
      val s = floor(sqrt(numPartitions)).toInt
      val n = floor(numPartitions / s.toDouble).toInt
      val xBoundaries = getBoundary(df, s, columns.head)
      val yBoundaries = xBoundaries.sliding(2).map(range => {
        val stripDf = getStrip(df, range, columns.head)
        getBoundary(stripDf, n, columns(1))
      }).toArray
      if (coverWholeRange) {
        //        val wholeRange = getWholeRange(df, List("x", "y"))
        val wholeRange: List[Double] = List(-180, -90, 180, 90)
        val new_boundaries = replaceBoundary(xBoundaries, yBoundaries, wholeRange)
        genBoxes(new_boundaries._1, new_boundaries._2)
      }
      else
        genBoxes(xBoundaries, yBoundaries)
    }

    val spark = SparkSession.builder().getOrCreate()
    val sr = samplingRate.getOrElse(getSamplingRate(dataRDD))
    val rectangleRDD = dataRDD.sample(withReplacement = false, sr, seed = 1)
      .map(x => (x.mbr.center().lon, x.mbr.center().lat))
    val df = spark.createDataFrame(rectangleRDD).toDF("x", "y").cache()
    val res = STR(df, List("x", "y"), coverWholeRange = true)
    val boxes = res._1
    var boxesWIthID: Map[Int, Rectangle] = Map()
    for (i <- boxes.indices) {
      val lonMin = boxes(i).head
      val latMin = boxes(i)(1)
      val lonMax = boxes(i)(2)
      val latMax = boxes(i)(3)
      boxesWIthID += (i -> Rectangle(Array(lonMin, latMin, lonMax, latMax)))
    }
    //    val areaSum = partitionRange.values.map(_.area).sum.formatted("%.5f")
    //    val rangeArea = Rectangle(Array(
    //      partitionRange.values.map(_.xMin).min,
    //      partitionRange.values.map(_.yMin).min,
    //      partitionRange.values.map(_.xMax).max,
    //      partitionRange.values.map(_.yMax).max)).area.formatted("%.5f")
    //    assert(areaSum == rangeArea,
    //      s"Range boundary error, whole range area $rangeArea, area sum $areaSum")
    if (threshold != 0) boxesWIthID.mapValues(rectangle => rectangle.dilate(threshold)).map(identity)
    else boxesWIthID
  }

  /**
   * Partition spatial dataRDD using STR algorithm
   *
   * @param dataRDD :data RDD
   * @tparam T : type of spatial dataRDD, extending geometry.Shape
   * @return partitioned RDD of [(partitionNumber, dataRDD)]
   */
  override def partition[T <: geometry.Shape : ClassTag](dataRDD: RDD[T]): RDD[T] = {
    val partitionMap = getPartitionRange(dataRDD)
    val partitioner = new KeyPartitioner(numPartitions)
    val boundary = genBoundary(partitionMap)
    val pRDD = assignPartition(dataRDD, partitionMap, boundary)
      .partitionBy(partitioner)
    this.partitionRange = calPartitionRanges(pRDD)
    pRDD.map(_._2)
  }

  /**
   * Partition spatial dataRDD with given Partition ranges
   *
   * @param dataRDD      : data RDD
   * @param partitionMap : Map (id -> range as rectangle)
   * @tparam T : type of spatial dataRDD, extending geometry.Shape
   * @return partitioned RDD of [(partitionNumber, dataRDD)]
   */
  def partition[T <: geometry.Shape : ClassTag](dataRDD: RDD[T], partitionMap: Map[Int, Rectangle]): RDD[(Int, T)] = {
    val partitioner = new KeyPartitioner(numPartitions)
    val boundary = genBoundary(partitionMap)

    val pRDD = assignPartition(dataRDD, partitionMap, boundary)
      .partitionBy(partitioner)
    pRDD
  }

  /**
   * Partition spatial dataRDD and queries simultaneously
   *
   * @param dataRDD  : data RDD
   * @param queryRDD : query RDD
   * @tparam T : type of spatial dataRDD, extending geometry.Shape
   * @return tuple of (RDD[(partitionNumber, dataRDD)], RDD[(partitionNumber, queryRectangle)])
   */
  def copartition[T <: geometry.Shape : ClassTag, R <: geometry.Shape : ClassTag]
  (dataRDD: RDD[T], queryRDD: RDD[R]):
  (RDD[(Int, T)], RDD[(Int, R)]) = {
    val partitionMap = getPartitionRange(dataRDD)
    val partitioner = new KeyPartitioner(numPartitions)
    val boundary = genBoundary(partitionMap)
    val pRDD = assignPartition(dataRDD, partitionMap, boundary)
      .partitionBy(partitioner)
    val pQueryRDD = assignPartition(queryRDD, partitionMap, boundary)
      .partitionBy(partitioner)
    (pRDD, pQueryRDD)
  }

  /**
   * Generate the whole boundary of the sampled objects
   *
   * @param partitionMap : map of partitionID --> rectangle
   * @return : List(xMin, yMin, xMax, yMax)
   */
  def genBoundary(partitionMap: Map[Int, Rectangle]): List[Double] = {
    val boxes: Iterable[Array[Double]] = partitionMap.values.map(_.coordinates)
    val minLon: Double = boxes.map(_.head).min
    val minLat: Double = boxes.map(_ (1)).min
    val maxLon: Double = boxes.map(_ (2)).max
    val maxLat: Double = boxes.map(_.last).max
    List(minLon, minLat, maxLon, maxLat)
  }

  /**
   * Assign partition to each object
   *
   * @param dataRDD      : data RDD
   * @param partitionMap : map of partitionID --> rectangle
   * @param boundary     : the whole boundary of the sampled objects
   * @tparam T :  type pf spatial dataRDD, extending geometry.Shape
   * @return : partitioned RDD of [(partitionNumber, dataRDD)]
   */
  //  def assignPartition[T <: Shape : ClassTag](dataRDD: RDD[T],
  //                                             partitionMap: Map[Int, Rectangle],
  //                                             boundary: List[Double]): RDD[(Int, T)] = {
  //    dataRDD.take(1).head match {
  //      case _: Point =>
  //        val rddWithIndex = dataRDD
  //          .map(x => {
  //            val pointShrink = Point(Array(
  //              min(max(x.asInstanceOf[Point].x, boundary.head), boundary(2)),
  //              min(max(x.asInstanceOf[Point].y, boundary(1)), boundary(3))
  //            ))
  //            (x, partitionMap.filter {
  //              case (_, v) => pointShrink.inside(v)
  //            })
  //          })
  //        if (threshold != 0) rddWithIndex.flatMap(x => x._2.keys.map(y => (y, x._1)))
  //        else rddWithIndex.map(x => (x._2.keys.head, x._1))
  //      case _ =>
  //        val rddWithIndex = dataRDD
  //          .map(x => {
  //            val mbr = x.mbr
  //            val mbrShrink = Rectangle(Array(
  //              min(max(mbr.xMin, boundary.head), boundary(2)),
  //              min(max(mbr.yMin, boundary(1)), boundary(3)),
  //              max(min(mbr.xMax, boundary(2)), boundary.head),
  //              max(min(mbr.yMax, boundary(3)), boundary(1))))
  //            (x, partitionMap.filter {
  //              case (_, v) => v.intersect(mbrShrink)
  //            })
  //          })
  //        rddWithIndex.map(x => (x._1, x._2.keys))
  //          .flatMapValues(x => x)
  //          .map(_.swap)
  //    }
  //  }

  def assignPartition[T <: Shape : ClassTag](dataRDD: RDD[T],
                                             partitionMap: Map[Int, Rectangle],
                                             boundary: List[Double]): RDD[(Int, T)] = {
    dataRDD.take(1).head match {
      case _: Point =>
        val rddWithIndex = dataRDD
          .map(x => {
            (x, partitionMap.filter {
              case (_, v) => x.inside(v)
            })
          })
        if (threshold != 0) rddWithIndex.flatMap(x => x._2.keys.map(y => (y, x._1)))
        else rddWithIndex.map(x => (x._2.keys.head, x._1))
      case _ =>
        val rddWithIndex = dataRDD
          .map(x => {
            val mbr = x.mbr
            (x, partitionMap.filter {
              case (_, v) => v.intersect(mbr)
            })
          })
        rddWithIndex.map(x => (x._1, x._2.keys))
          .flatMapValues(x => x)
          .map(_.swap)
    }
  }

  def getSamplingRate[T <: Shape : ClassTag](dataRDD: RDD[T]): Double = {
    val dataSize = dataRDD.count
    max(min(1000 / dataSize.toDouble, 0.5), 100 * numPartitions / dataSize.toDouble)
  }
}