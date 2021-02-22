package operators.selection.indexer

trait Index

sealed abstract class IndexType

object IndexType {
  def apply(ty: String): IndexType = ty.toLowerCase match {
    case "rtree" => RTreeType
    case "hashmap" => HashMapType
    case _ => null
  }
}

case object RTreeType extends IndexType
case object HashMapType extends IndexType