package partitioner

import geometry.{Point, Rectangle, Shape}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.math.{floor, max, min, sqrt}
import scala.reflect.ClassTag

object STRPartitioner {
  /**
   * STR partitioner
   *
   * @param r            : input RDD
   * @param numPartition : number of partitions
   * @tparam T : type extending Shape
   * @param samplingRate : sampling rate
   * @return : repartitioned RDD and map(partitionNum -> boundary)
   */
  def apply[T <: Shape : ClassTag](r: RDD[T], numPartition: Int, samplingRate: Double): (RDD[T], Map[Int, Rectangle]) = {
    val spark = SparkSession.builder().getOrCreate()
    // gen dataframeRDD on MBR
    val rectangleRDD = r.sample(withReplacement = false, samplingRate)
      .map(x => (x.center().lat, x.center().lon))
    val df = spark.createDataFrame(rectangleRDD).toDF("x", "y")
    val res = STR(df, numPartition, List("x", "y"), coverWholeRange = true)
    val boxes = res._1
    val boxMap = res._2
    var boxesWIthID: Map[Int, Rectangle] = Map()
    for (i <- boxes.indices) {
      val xMin = boxes(i).head
      val yMin = boxes(i)(1)
      val xMax = boxes(i)(2)
      val yMax = boxes(i)(3)
      boxesWIthID += (i -> Rectangle(Point(xMin, yMin), Point(xMax, yMax)))
    }
    val partitioner = new STRPartitioner(boxesWIthID.size, boxMap)
    (new ShuffledRDD[T, T, T](r.map(x => (x, x)), partitioner).map(x => x._1), boxesWIthID)
  }

  def getBoundary(df: DataFrame, numPartitions: Int, column: String): Array[Double] = {
    val interval = 1.0 / numPartitions
    var t = 0.0
    var q = new Array[Double](0)
    while (t < 1 - interval) {
      t += interval
      q = q :+ t
    }
    q = 0.0 +: q
    if (q.length < numPartitions + 1) q = q :+ 1.0
    df.stat.approxQuantile(column, q, 0.0001)
  }

  def getStrip(df: DataFrame, range: List[Double], column: String): DataFrame = {
    df.filter(functions.col(column) >= range.head && functions.col(column) < range(1))
  }

  def gen_boxes(x_boundaries: Array[Double], y_boundaries: Array[Array[Double]]): (Array[List[Double]], Map[List[Double], Array[(List[Double], Int)]]) = {
    var metaBoxes = new Array[List[Double]](0)
    for (x <- 0 to x_boundaries.length - 2) metaBoxes = metaBoxes :+ List(x_boundaries(x), y_boundaries(x)(0), x_boundaries(x + 1), y_boundaries(0).last)
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
    def toDouble: Any => Double = {
      case i: Int => i
      case f: Float => f
      case d: Double => d
    }

    val x_min = toDouble(df.select(functions.min(column.head)).collect()(0)(0))
    val x_max = toDouble(df.select(functions.max(column.head)).collect()(0)(0))
    val y_min = toDouble(df.select(functions.min(column(1))).collect()(0)(0))
    val y_max = toDouble(df.select(functions.max(column(1))).collect()(0)(0))
    val x_border = (x_max - x_min) * 0.001.toFloat
    val y_border = (y_max - y_min) * 0.001.toFloat
    Array(x_min - x_border, y_min - y_border, x_max + x_border, y_max + y_border)
  }

  def replaceBoundary(x_boundaries: Array[Double], y_boundaries: Array[Array[Double]], wholeRange: Array[Double]): (Array[Double], Array[Array[Double]]) = {
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

  def STR(df: DataFrame, numPartitions: Int, columns: List[String], coverWholeRange: Boolean): (Array[List[Double]], Map[List[Double], Array[(List[Double], Int)]]) = {
    // columns: sequence of partitioning columns, e.g. List("x", "y") means partition on x first then y
    // return boxes
    val s = floor(sqrt(numPartitions)).toInt
    val n = floor(numPartitions / s.toDouble).toInt
    var x_boundaries = getBoundary(df, s, columns.head)
    assert(s * n < numPartitions)
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
    gen_boxes(x_boundaries, y_boundaries)
  }

}

class STRPartitioner(num: Int, boxMap: Map[List[Double], Array[(List[Double], Int)]]) extends Partitioner {
  override def numPartitions: Int = num

  val boxes: Array[List[Double]] = boxMap.keys.toArray
  val minLon: Double = boxes.map(_.head).min
  val minLat: Double = boxes.map(_ (1)).min
  val maxLon: Double = boxes.map(_ (2)).max
  val maxLat: Double = boxes.map(_.last).max

  override def getPartition(key: Any): Int = {
    val c = key.asInstanceOf[Shape].center()
    val K = Point(max(min(c.lon, maxLon), minLon), max(min(c.lat, maxLat), minLat))
    var flag = false
    for (k <- boxMap.keys) {
      if (K.lon >= k.head && K.lon <= k(2) && K.lat >= k(1) && K.lat <= k(3)) {
        flag = true
        for (v <- boxMap(k)) {
          if (K.lon >= v._1.head && K.lon <= v._1(2) && K.lat >= v._1(1) && K.lat <= v._1(3)) {
            return v._2
          }
        }
      }
    }
    num - 1
  }
}


