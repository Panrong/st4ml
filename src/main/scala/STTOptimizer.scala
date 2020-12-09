//import org.apache.spark.sql.ExperimentalMethods
//import org.apache.spark.sql.catalyst.catalog.SessionCatalog
//import org.apache.spark.sql.catalyst.expressions.{And, Expression, PredicateHelper}
//import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
//import org.apache.spark.sql.catalyst.rules.Rule
//import org.apache.spark.sql.execution.SparkOptimizer
//import org.apache.spark.sql.internal.SQLConf
//
//class STTOptimizer(
//  catalog: SessionCatalog,
//  conf: SQLConf,
//  experimentalMethods: ExperimentalMethods)
//  extends SparkOptimizer(catalog, conf, experimentalMethods) {
//
//  override def batches: Seq[Batch] = super.batches :+
//    Batch("SpatialJoinPushDown", FixedPoint(100), PushPredicateThroughSpatialJoin)
//
//}
//
//object PushPredicateThroughSpatialJoin extends Rule[LogicalPlan] with PredicateHelper {
//  private def split(condition: Seq[Expression], left: LogicalPlan, right: LogicalPlan) = {
//    val (leftEvaluateCondition, rest) =
//      condition.partition(_.references subsetOf left.outputSet)
//    val (rightEvaluateCondition, commonCondition) =
//      rest.partition(_.references subsetOf right.outputSet)
//
//    (leftEvaluateCondition, rightEvaluateCondition, commonCondition)
//  }
//
//  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
//    // push the where condition down into join filter
//    case f @ Filter(filterCondition, SpatialJoin(left, right, joinType, joinCondition)) =>
//      val (leftFilterConditions, rightFilterConditions, commonFilterCondition) =
//        split(splitConjunctivePredicates(filterCondition), left, right)
//
//      val newLeft = leftFilterConditions.reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
//      val newRight = rightFilterConditions.reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
//      val newJoinCond = (commonFilterCondition ++ joinCondition).reduceLeftOption(And)
//      SpatialJoin(newLeft, newRight, joinType, newJoinCond)
//
//    // push down the join filter into sub query scanning if applicable
//    case f @ SpatialJoin(left, right, joinType, joinCondition) =>
//      val (leftJoinConditions, rightJoinConditions, commonJoinCondition) =
//        split(joinCondition.map(splitConjunctivePredicates).getOrElse(Nil), left, right)
//
//      val newLeft = leftJoinConditions.reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
//      val newRight = rightJoinConditions.reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
//      val newJoinCond = commonJoinCondition.reduceLeftOption(And)
//
//      SpatialJoin(newLeft, newRight, joinType, newJoinCond)
//  }
//}