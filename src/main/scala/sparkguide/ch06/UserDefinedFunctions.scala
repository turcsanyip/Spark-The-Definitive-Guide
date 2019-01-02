package sparkguide.ch06

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

object UserDefinedFunctions {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val df = session.range(5).toDF("num")

    def power3(number: Double): Double = number * number * number

    println(power3(2.0))

    val power3udf = udf(power3(_: Double): Double)

    df.select(col("num"), power3udf(col("num")).alias("num^3")).show()

    //session.udf.register("power3", power3(_: Double): Double)
    session.udf.register("power3", power3udf)
    df.selectExpr("num", "power3(num) as num_3").show()
  }
}
