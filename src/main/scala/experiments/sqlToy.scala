package experiments

import experiments.sqlToy.ToyFunctions.{beginning_of_month, longPlus1, longPlusLat}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ImplicitCastInputTypes, Literal, Predicate, UnaryExpression}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{DataType, DateType, DoubleType, LongType, StringType, StructField, StructType}
import st4ml.instances.{Point, Polygon}

import java.time.LocalDate

object sqlToy {
  object ToyFunctions {

    private def withExpr(expr: Expression): Column = new Column(expr)

    /** example */
    def beginning_of_month(col: Column): Column = withExpr {
      BeginningOfMonth(col.expr)
    }

    /** a column function, transfer one column to another */
    def longPlus1(col: Column): Column = withExpr {
      LongPlus1(col.expr)
    }

    /** a column function, transfer two columns to another */
    def longPlusLat(col1: Column, col2: Column): Column = withExpr {
      LongPlusLat(col1.expr, col2.expr)
    }

  }

  case class BeginningOfMonth(startDate: Expression) extends UnaryExpression with ImplicitCastInputTypes {
    override def child: Expression = startDate

    override def inputTypes = Seq(DateType)

    override def dataType: DataType = DateType

    override def nullSafeEval(date: Any): Any = {
      ToyUtils.getFirstDayOfMonth(date.asInstanceOf[Int])
    }

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
      val dtu = ToyUtils.getClass.getName.stripSuffix("$")
      defineCodeGen(ctx, ev, sd => s"$dtu.getFirstDayOfMonth($sd)")
    }
  }

  case class LongPlus1(expression: Expression) extends UnaryExpression with ImplicitCastInputTypes {
    override def child: Expression = expression

    override def inputTypes = Seq(DoubleType)

    override def dataType: DataType = DoubleType

    override def nullSafeEval(long: Any): Any = {
      ToyUtils.longPlus1(long.asInstanceOf[Double])
    }

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
      val dtu = ToyUtils.getClass.getName.stripSuffix("$")
      defineCodeGen(ctx, ev, sd => s"$dtu.longPlus1($sd)")
    }
  }

  case class LongPlusLat(expr1: Expression, expr2: Expression) extends BinaryExpression with ImplicitCastInputTypes {

    override def left: Expression = expr1

    override def right: Expression = expr2

    override def inputTypes = Seq(DoubleType, DoubleType)

    override def dataType: DataType = DoubleType

    override def nullSafeEval(long: Any, lat: Any): Any = {
      ToyUtils.longPlusLat(long.asInstanceOf[Double], lat.asInstanceOf[Double])
    }

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
      val dtu = ToyUtils.getClass.getName.stripSuffix("$")
      defineCodeGen(ctx, ev, (sd, m) => s"$dtu.longPlusLat($sd, $m)")
    }
  }

  //  case class InRange(long: Expression, lat: Expression, range: Expression) extends Predicate with ImplicitCastInputTypes {
  //    override def nullable: Boolean = false
  //
  //    override def eval(input: InternalRow): Any = {
  //      val values = children.map(_.eval(input))
  //      val eval_range = range.asInstanceOf[Literal].value.asInstanceOf[Polygon]
  //      val point = Point(values(0).asInstanceOf[Double], values(1).asInstanceOf[Double])
  //      point.intersects(eval_range)
  //    }
  //
  //    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
  //      val eval = children(0).genCode(ctx)
  //
  //    }
  //
  //    override def children: Seq[Expression] = Seq(long, lat, range)
  //  }


  object ToyUtils {
    type SQLDate = Int

    /**
     * Returns first day of the month for the given date. The date is expressed in days
     * since 1.1.1970.
     */
    def getFirstDayOfMonth(date: SQLDate): SQLDate = {
      val localDate = LocalDate.ofEpochDay(date)
      date - localDate.getDayOfMonth + 1
    }

    def longPlus1(long: Double): Double = {
      long + 1
    }

    def longPlusLat(long: Double, lat: Double): Double = {
      long + lat
    }
  }


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("sqlToy")
      .master("local")
      .getOrCreate()
//    spark.conf.set("spark.sql.cbo.enabled","true")
    import spark.implicits._
    //    val df = Seq(
    //      ("2020-01-15"),
    //      ("2020-01-20"),
    //      (null)
    //    ).toDF("some_date")
    //      .withColumn("beginning_of_month", beginning_of_month(col("some_date")))
    //    df.show()
    val schema = new StructType(Array(
      StructField("longitude", DoubleType, true),
      StructField("latitude", DoubleType, true),
      StructField("t", LongType, true),
      StructField("id", StringType, true),
      StructField("a1", StringType, true),
      StructField("a2", StringType, true),
      StructField("a3", StringType, true),
      StructField("a4", StringType, true),
      StructField("a5", StringType, true),
      StructField("a6", StringType, true),
      StructField("a7", StringType, true),
      StructField("a8", StringType, true),
      StructField("a9", StringType, true),
      StructField("a10", StringType, true),
      StructField("a11", StringType, true),
    ))

    val df2 = spark.read
      .schema(schema)
      .csv("datasets/nyc_geomesa_example")
    df2.printSchema()
    df2.show(2)

    df2.select($"longitude", longPlus1($"longitude")).show(3)
    df2.select($"longitude", $"latitude", longPlusLat($"longitude", $"latitude").as("long+lat")).show(3)
    //    df2.filter(InRange($"longitude",$"latitude",lit()))

    val df3 = df2.select("id").filter(col("longitude") < 100).filter(col("longitude") > 50)
    df3.explain(extended = true)


    val emp = Seq((1, "Smith", -1, "2018", "10", "M", 3000),
      (2, "Rose", 1, "2010", "20", "M", 4000),
      (3, "Williams", 1, "2010", "10", "M", 1000),
      (4, "Jones", 2, "2005", "10", "F", 2000),
      (5, "Brown", 2, "2010", "40", "", -1),
      (6, "Brown", 2, "2010", "50", "", -1)
    )
    val empColumns = Seq("emp_id", "name", "superior_emp_id", "year_joined",
      "emp_dept_id", "gender", "salary")
    import spark.implicits._
    val empDF = emp.toDF(empColumns: _*)
    empDF.show(false)

    val dept = Seq(("Finance", 10),
      ("Marketing", 20),
      ("Sales", 30),
      ("IT", 40)
    )

    val deptColumns = Seq("dept_name", "dept_id")
    val deptDF = dept.toDF(deptColumns: _*)
    deptDF.show(false)

    empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id")).filter($"year_joined" < 2010).explain(mode = "cost")

    val arr = Seq(("1", "A", 0), ("2", "B", 50001))
    val df4 = arr.toDF(Seq("value", "value2", "id"): _*)
//    df4.show
    val arr2 = Seq((50001, 2), (2, 3), (2, 4))
    val df5 = arr2.toDF(Seq("id", "a"): _*)
//    df4.join(df5, "id").filter(df5("id") > 50000)
//      .select($"id", (lit(1) + lit(2) + $"value").alias("v"))
//      .groupBy("v")
//      .sum("v")
//      .show()

    df4.createOrReplaceTempView("t1")
    df5.createOrReplaceTempView("t2")
    spark.sql("select sum(v) from ( select t1.id, 1+2+t1.value as v from t1 join t2 where t1.id=t2.id and t2.id > 50000)").explain(mode = "cost")
    Thread.sleep(1000000)
    spark.sparkContext.stop()
  }
}