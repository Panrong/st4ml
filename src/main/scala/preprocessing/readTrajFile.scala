package preprocessing

import java.lang.System.nanoTime
import geometry.{Point, Trajectory}
import org.apache.spark.sql.Dataset

object readTrajFile extends SparkSessionWrapper{
  /**
   *
   * @param filename : path to the data file
   * @param num : number of trajectories to read
   * @return : Dataset[Trajectory]
   *         +-------------------+--------+----------+--------------------+
   *         |             tripID|  taxiID| startTime|              points|
   *         +-------------------+--------+----------+--------------------+
   *         |1372636858620000589|20000589|1372636858|[[-8.618643, 41.1...|
   *         |1372637303620000596|20000596|1372637303|[[-8.639847, 41.1...|
   *         |1372636951620000320|20000320|1372636951|[[-8.612964, 41.1...|
   *         |1372636854620000520|20000520|1372636854|[[-8.574678, 41.1...|
   *         |1372637091620000337|20000337|1372637091|[[-8.645994, 41.1...|
   *         +-------------------+--------+----------+--------------------+
   *
   */
    def apply(filename: String, num: Int): Dataset[Trajectory] = {
      val timeCount = true
      val t = nanoTime
      import spark.implicits._
      val df = spark.read.option("header", "true").csv(filename).limit(num)
      val samplingRate = 15
      val trajRDD = df.rdd.filter(row => row(8).toString.split(',').length >= 4) // each trajectory should have no less than 2 recorded points
      val resRDD = trajRDD.map(row => {
        val tripID = row(0).toString.toLong
        val taxiID = row(4).toString.toLong
        val startTime = row(5).toString.toLong
        val pointsString = row(8).toString
        var pointsCoord = new Array[Double](0)
        for (i <- pointsString.split(',')) pointsCoord = pointsCoord :+ i.replaceAll("[\\[\\]]", "").toDouble
        var points = new Array[Point](0)
        for (i <- 0 to pointsCoord.length - 2 by 2) {
          points = points :+ Point(pointsCoord(i), pointsCoord(i + 1), startTime + samplingRate * i / 2)
        }
        Trajectory(tripID, taxiID, startTime, points)
      })
      println("==== Read CSV Done")
      println("--- Total number of lines: " + df.count)
      println("--- Total number of valid entries: " + resRDD.count)
      if (timeCount) println("... Time used: " + (nanoTime - t) / 1e9d + "s")
      resRDD.toDS
    }
}

object readTrajTest extends App {
  override def main(args: Array[String]): Unit = {
    /** set up Spark */
    readTrajFile("datasets/porto_traj.csv" ,1000).show(5)
  }
}