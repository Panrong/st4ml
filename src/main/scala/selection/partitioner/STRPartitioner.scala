package selection.partitioner

import geometry.{Rectangle, Shape, Point}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.math.{floor, max, min, sqrt}
import scala.reflect.ClassTag

class STRPartitioner(numPartitions: Int, override var samplingRate: Option[Double] = None)
  extends SpatialPartitioner with Serializable {
  var partitionRange: Map[Int, Rectangle] = Map()

  /**
   * Generate ranges using STR for partitioning
   *
   * @param dataRDD : data RDD
   * @tparam T : type of spatial dataRDD, extending geometry.Shape
   * @return : map of partitionID --> rectangle
   */

  def getPartitionRange[T <: Shape : ClassTag](dataRDD: RDD[T]): Map[Int, Rectangle] = {
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
      val s = floor(sqrt(numPartitions)).toInt
      val n = floor(numPartitions / s.toDouble).toInt
      var x_boundaries = getBoundary(df, s, columns.head)
      assert(s * n <= numPartitions)
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

    val spark = SparkSession.builder().getOrCreate()
    val sr = samplingRate.getOrElse(getSamplingRate(dataRDD))
    println(sr)
    val rectangleRDD = dataRDD.sample(withReplacement = false, sr, seed = 1)
      .map(x => (x.mbr.center().lon, x.mbr.center().lat))
    val df = spark.createDataFrame(rectangleRDD).toDF("x", "y")
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
    this.partitionRange = boxesWIthID
    val areaSum = partitionRange.values.map(_.area).sum.formatted("%.5f")
    val rangeArea = Rectangle(Array(
      partitionRange.values.map(_.xMin).min,
      partitionRange.values.map(_.yMin).min,
      partitionRange.values.map(_.xMax).max,
      partitionRange.values.map(_.yMax).max)).area.formatted("%.5f")
    assert(areaSum == rangeArea,
      s"Range boundary error, whole range area $rangeArea, area sum $areaSum")
    boxesWIthID
  }

  /**
   * Partition spatial dataRDD using STR algorithm
   *
   * @param dataRDD :data RDD
   * @tparam T : type of spatial dataRDD, extending geometry.Shape
   * @return partitioned RDD of [(partitionNumber, dataRDD)]
   */
  override def partition[T <: geometry.Shape : ClassTag](dataRDD: RDD[T]): RDD[(Int, T)] = {
    val partitionMap = getPartitionRange(dataRDD)
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
   * @tparam T : type pf spatial dataRDD, extending geometry.Shape
   * @return tuple of (RDD[(partitionNumber, dataRDD)], RDD[(partitionNumber, queryRectangle)])
   */
  def copartition[T <: geometry.Shape : ClassTag]
  (dataRDD: RDD[T], queryRDD: RDD[Rectangle]):
  (RDD[(Int, T)], RDD[(Int, Rectangle)]) = {
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
  def assignPartition[T <: Shape : ClassTag](dataRDD: RDD[T],
                                             partitionMap: Map[Int, Rectangle],
                                             boundary: List[Double]): RDD[(Int, T)] = {
    dataRDD.take(1).head match {
      case p: Point => {
        println("... Dealing with point data")
        val rddWithIndex = dataRDD
          .map(x => {
            val pointShrink = Point(Array(
              min(max(x.asInstanceOf[Point].x, boundary.head), boundary(2)),
              min(max(x.asInstanceOf[Point].y, boundary(1)), boundary(3))
            ))
            (x, partitionMap.filter {
              case (_, v) => pointShrink.inside(v)
            }.head)
          })
          .map(x => (x._1, x._2._1))
          .map {
            case (k, v) => (v, k)
          }
        rddWithIndex
      }
      case _ => {
        val rddWithIndex = dataRDD
          .map(x => {
            val mbr = x.mbr
            val mbrShrink = Rectangle(Array(
              min(max(mbr.xMin, boundary.head), boundary(2)),
              min(max(mbr.yMin, boundary(1)), boundary(3)),
              max(min(mbr.xMax, boundary(2)), boundary.head),
              max(min(mbr.yMax, boundary(3)), boundary(1))))
            (x, partitionMap.filter {
              case (_, v) => v.intersect(mbrShrink)
            })
          })
        rddWithIndex.map(x => (x._1, x._2.keys))
          .flatMapValues(x => x)
          .map {
            case (k, v) => (v, k)
          }
      }
    }

  }

  def getSamplingRate[T <: Shape : ClassTag](dataRDD:RDD[T]): Double = {
    val dataSize = dataRDD.count
    max(min(1000/dataSize.toDouble, 0.5), 1e-3)
  }
}
