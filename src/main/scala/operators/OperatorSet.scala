package operators

import geometry.Shape
import operators.convertion.Converter
import operators.extraction.BaseExtractor
import operators.repartitioner.{DoNothingRepartitioner, Repartitioner}
import operators.selection.Selector
import org.apache.spark.rdd.RDD

abstract class OperatorSet extends Serializable {
  type I <: Shape
  type O
  val selector: Selector[I]
  val converter: Converter
  val repartitioner: Repartitioner[O] = new DoNothingRepartitioner[O]
  val extractor: BaseExtractor[O]
}