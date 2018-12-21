package sparkguide.ch05

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object DataFrameColumns {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("sparkguide").master("local[*]").getOrCreate()

    val df = session.read.json("data/flight-data/json/2015-summary.json")

    for (c <- df.columns)
      println(c)

    // reference to a column
    df.select("DEST_COUNTRY_NAME").show(1)
    df.select(col("DEST_COUNTRY_NAME")).show(1)
    df.select(column("DEST_COUNTRY_NAME")).show(1)

    // column of a specific DataFrame
    df.select(df.col("DEST_COUNTRY_NAME")).show(1)

    // simple column expression
    df.select(expr("DEST_COUNTRY_NAME")).show(1)
    df.selectExpr("DEST_COUNTRY_NAME").show(1)

    // expressions
    df.select(col("count"), col("count") + 15).show(1)
    df.select(col("count"), expr("count + 15")).show(1)
    df.selectExpr("count", "count + 15").show(1)
  }
}
