package operators.extraction

import geometry.{Point, TimeSeries}
import org.apache.spark.rdd.RDD

class FlowExtractor extends BaseExtractor[TimeSeries[Point]] {
  def extract(rdd: RDD[TimeSeries[Point]]): RDD[(Array[Double], (Long, Long), Int)] = {
    rdd.flatMap(ts => {
      ts.toMap.mapValues(_.length).toArray.map(
        x => (ts.spatialRange.coordinates, x._1, x._2)
      )
    })
  }
}
