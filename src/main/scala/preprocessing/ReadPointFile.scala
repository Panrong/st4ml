package preprocessing

import org.apache.commons.lang.math.NumberUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object ReadPointFile {
  /**
   *
   * @param fileName :  path to csv file
   * @param limitNum : number of lines to read, if not all
   * @return
   */
  def apply(fileName: String, limitNum: Int = Int.MaxValue): RDD[geometry.Point] = {
    val spark = SparkSession.builder().getOrCreate()
    val df = spark.read
      .option("header", "false")
      .option("multiline", "true")
      .json(fileName)
      .limit(limitNum)
    df.filter(!_.anyNull).rdd
      .filter(r=>NumberUtils.isNumber(r(1).toString) && NumberUtils.isNumber(r(2).toString))
      .map(row =>
        geometry.Point(Array(row.getString(2).toDouble ,row.getString(1).toDouble))
          .setID(row.getString(0).hashCode()))
  }
}

