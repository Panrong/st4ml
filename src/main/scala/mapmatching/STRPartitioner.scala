package STRPartition

import org.apache.spark.Partitioner
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import main.scala.mapmatching.SpatialClasses.{Line, Point, Rectangle}

import scala.math.{ceil, sqrt}

object STRTest extends App {
  override def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val fileName = "C://Users//kaiqi001//Desktop//taxi_log_2008_by_id//2.txt"
    var numPartition = 6
    val lines = sc.textFile(fileName, numPartition)
    val points = lines.map(line => Point(line.split(",")(2).toDouble, line.split(",")(3).toDouble))
    val pnts = points.collect
    var l = new Array[Line](0)
    for (i <- 0 to pnts.length - 2) l = l :+ Line(pnts(i), pnts(i + 1))
    val lineRDD = sc.parallelize(l).map(x => (x.mbr, x))
    val indexedRDD = STRPartitioner(lineRDD, numPartition, spark)


    //print content of each partition
    val p = indexedRDD.map(x => x._1)
    val pointsWithIndex = p.mapPartitionsWithIndex {
      (index, partitionIterator) => {
        val partitionsMap = scala.collection.mutable.Map[Int, List[Rectangle]]()
        var partitionList = List[Rectangle]()
        while (partitionIterator.hasNext) {
          //println(partitionIterator.next())
          partitionList = partitionIterator.next() :: partitionList
        }
        partitionsMap(index) = partitionList
        partitionsMap.iterator
      }
    }
    //points.foreach(point=>println("("+point(0)+","+point(1)+")"))
    //pointsWithIndex.foreach(point=>println(point))

    // convert RDD to format suitable for printing
    val RDDToPrint = pointsWithIndex.map(line => {
      val i = line._1
      val v = line._2
      var r = i.toString()
      for (p <- v) {
        r = r + ' ' + p.bottomLeft.lat.toString + ' ' + p.bottomLeft.long.toString + ' ' + p.topRight.lat.toString + ' ' + p.topRight.long.toString + ","
      }
      r
    })
    //RDDToPrint.saveAsObjectFile("file:///C://Users//kaiqi001//Desktop//out") // cannot work, dont know why

    RDDToPrint.collect.foreach(point => println(point))

    sc.stop()

  }
}

object STRPartitioner {

  def apply(origin: RDD[(Rectangle, Line)], numPartition: Int, spark: SparkSession): ShuffledRDD[Rectangle, Line, Line] = {
    // gen dataframeRDD on MBR
    val rectangleRDD = origin.map(x => (x._1.center.lat, x._1.center.long))
    val df = spark.createDataFrame(rectangleRDD).toDF("x", "y")
    val res = STR(df, numPartition, List("x", "y"), true)
    val boxes = res._1
    val boxMap = res._2
    for (i <- boxes) println(i)
    var boxesWIthID: Map[Int, List[Double]] = Map()
    for (i <- 0 to boxes.length - 1) {
      val box = boxes(i)
      boxesWIthID += (i -> box)
    }
    val partitioner = new STRPartitioner(boxesWIthID.size, boxMap)
    new ShuffledRDD[Rectangle, Line, Line](origin, partitioner)
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
    df.filter(functions.col(column) >= range(0) && functions.col(column) < range(1))
  }

  def gen_boxes(x_boundaries: Array[Double], y_boundaries: Array[Array[Double]]): (Array[List[Double]], Map[List[Double], Array[(List[Double], Int)]]) = {
    var metaBoxes = new Array[List[Double]](0)
    for (x <- 0 to x_boundaries.length - 2) metaBoxes = metaBoxes :+ List(x_boundaries(x), y_boundaries(x)(0), x_boundaries(x + 1), y_boundaries(0).last)
    var boxMap: Map[List[Double], Array[(List[Double], Int)]] = Map()
    var boxes = new Array[List[Double]](0)
    var n = 0
    for (i <- 0 to x_boundaries.length - 2) {
      var stripBoxes = new Array[(List[Double], Int)](0)
      var x_min = x_boundaries(i)
      var x_max = x_boundaries(i + 1)
      for (j <- 0 to y_boundaries(i).length - 2) {
        var y_min = y_boundaries(i)(j)
        var y_max = y_boundaries(i)(j + 1)
        var box = List(x_min, y_min, x_max, y_max)
        boxes = boxes :+ box
        stripBoxes = stripBoxes :+ (box, n)
        n += 1
        //println(box)
      }
      boxMap += metaBoxes(i) -> stripBoxes
    }
    (boxes, boxMap)
  }

  def getWholeRange(df: DataFrame, column: List[String]): Array[Double] = {
    def toDouble: (Any) => Double = {
      case i: Int => i
      case f: Float => f
      case d: Double => d
    }

    val x_min = toDouble(df.select(functions.min(column(0))).collect()(0)(0))
    val x_max = toDouble(df.select(functions.max(column(0))).collect()(0)(0))
    val y_min = toDouble(df.select(functions.min(column(1))).collect()(0)(0))
    val y_max = toDouble(df.select(functions.max(column(1))).collect()(0)(0))
    val x_border = (x_max - x_min) * 0.001.toFloat
    val y_border = (y_max - y_min) * 0.001.toFloat
    Array(x_min - x_border, y_min - y_border, x_max + x_border, y_max + y_border)
  }

  def replaceBoundary(x_boundaries: Array[Double], y_boundaries: Array[Array[Double]], wholeRange: Array[Double]): (Array[Double], Array[Array[Double]]) = {
    var n_x_boundaries = x_boundaries
    var n_y_boundaries = new Array[Array[Double]](0)
    n_x_boundaries(0) = wholeRange(0)
    n_x_boundaries(n_x_boundaries.length - 1) = wholeRange(2)
    for (y <- y_boundaries) {
      var n_y_boundary = wholeRange(1) +: y.slice(1, y.length - 1) :+ wholeRange(3)
      n_y_boundaries = n_y_boundaries :+ n_y_boundary
    }
    (n_x_boundaries, n_y_boundaries)
  }

  def STR(df: DataFrame, numPartitions: Int, columns: List[String], coverWholeRange: Boolean): (Array[List[Double]], Map[List[Double], Array[(List[Double], Int)]]) = {
    // columns: sequence of partitioning columns, e.g. List("x", "y") means partition on x first then y
    // return boxes
    val s = ceil(sqrt(numPartitions)).toInt
    val n = ceil(numPartitions / s.toDouble).toInt
    println(s, n)
    var x_boundaries = getBoundary(df, s, columns(0))
    var y_boundaries = new Array[Array[Double]](0)
    for (i <- 0 to x_boundaries.length - 2) {
      var range = List(x_boundaries(i), x_boundaries(i + 1))
      var stripRDD = getStrip(df, range, columns(0))
      var y_boundary = getBoundary(stripRDD, n, columns(1))
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
  // calculate number of partitions - partition x and y dimension both into numPartitions partitions (totally numPartitions^2 partitions)
  override def numPartitions: Int = num

  // the way to implement grid partitioning
  override def getPartition(key: Any): Int = {
    val K = key.asInstanceOf[Rectangle].center
    for (k <- boxMap.keys) {
      if (K.lat > k(0) && K.lat <= k(2) && K.long > k(1) && K.long <= k(3)) {
        for (v <- boxMap(k)) {
          if (K.lat > v._1(0) && K.lat <= v._1(2) && K.long > v._1(1) && K.long <= v._1(3)) {
            return v._2
          }
        }
      }
    }
    num - 1
  }
}

