package operators

import geometry.{Rectangle, Shape}
import operators.convertion.Converter
import operators.extraction.BaseExtractor
import operators.repartitioner.{DoNothingRepartitioner, Repartitioner}
import operators.selection.{DefaultSelector, Selector}

abstract class OperatorSet(sQuery: Rectangle  = Rectangle(Array(-180, -90, 180, 90)),
                           tQuery: (Long, Long) = (0L, 999999999L)) extends Serializable {
  type I <: Shape
  type O
  val selector: Selector[I] = new DefaultSelector[I](sQuery, tQuery)
  val converter: Converter {
    type I
    type O
  }
  val repartitioner: Repartitioner[O] = new DoNothingRepartitioner[O]
  val extractor: BaseExtractor[O]
}