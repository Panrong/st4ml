package preprocessing

import org.apache.spark.sql.SparkSession

object ReadTrajJson {
def apply(fileName: String):Unit = {
  val spark = SparkSession.builder().getOrCreate()
  val df = spark.read.json(fileName)
  df.show
}
}
