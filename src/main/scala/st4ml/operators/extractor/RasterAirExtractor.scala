package st4ml.operators.extractor

import org.apache.spark.rdd.RDD
import st4ml.instances.Utils.rasterRDDFuncs
import st4ml.instances._

class RasterAirExtractor extends Extractor[Raster[Polygon, Array[Event[Point, Array[Double], Int]], None.type]] {
  def add(a: Array[Double], b: Array[Double]): Array[Double] = a.zip(b).map { case (x, y) => x + y }

  def extract(rasterRDD: RDD[Raster[Polygon, Array[Event[Point, Array[Double], Int]], None.type]], numAirIndex: Int = 6):
  Raster[Polygon, Array[Double], None.type] = {
    val valueRDD = rasterRDD.map(_.mapValue(_.map(_.entries.head.value))).map(x => x.mapValue { y =>
      var res = new Array[Double](numAirIndex)
      y.foreach(i => res = add(res, i))
      res
    })
    valueRDD.collectAndMerge(new Array[Double](numAirIndex), add, add)
  }
}